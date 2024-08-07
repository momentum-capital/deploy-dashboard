import os
from decimal import Decimal
from random import random
from typing import Dict, List, Optional

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.order_book import OrderBook, OrderBookRow
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase


class RuneVolumeConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []

    trading_pair: str = Field(
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trading pair: ",
            prompt_on_new=True))

    debug: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable debug mode?",
            prompt_on_new=True))

    average_24h_volume: Decimal = Field(
        default=Decimal(0),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the average 24h volume (USD): ",
            prompt_on_new=True))

    randomness: Decimal = Field(
        default=Decimal(0.5),
        ge=Decimal(0),
        le=Decimal(1),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the randomness factor: ",
            prompt_on_new=True))

    frequency: Decimal = Field(
        default=Decimal(10),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trade frequency (per hour): ",
            prompt_on_new=True))

    max_slippage: Decimal = Field(
        default=1,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the maximum slippage (%): ",
            prompt_on_new=True))

    volume_skew: Decimal = Field(
        default=Decimal(0),
        le=Decimal(1),
        ge=Decimal(-1),
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the volume skew (1 = only buy, -1 = only sell): ",
            prompt_on_new=True))

    trade_history_file: str = Field(
        default="trade_history.csv",
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the trade history file name: ",
            prompt_on_new=True))


class RuneVolume(StrategyV2Base):
    """
    Volume generator strategy.

    Base Strategy Overview:
    - Generate given volume over 24h period
    - Randomness and frequency control the volume generation
    - Max slippage throttles the volume generation
    - If too much slippage would be reached, then execute only as much volume as possible
    - Leftover volume will be tried to execute in the next tick

    Optional Improvements:
    - Correlate volume with other exchange's volume
    - Skew volume in one or another direction
    """

    # References
    last_tick_volumes: Dict[float, Decimal] = {}  # timestamp: volume (positive = buy, negative = sell)
    leftover_volume: Decimal = Decimal(0)  # leftover volume from previous tick that needs to be executed
    trade_orders: List[str] = []  # list of order ids that are not filled yet

    # Wait condition
    start_wait_timestamp = 0
    wait_time = 0  # initial wait time
    waiting_for_slippage = False

    def __init__(self, connectors: Dict[str, ConnectorBase], config: RuneVolumeConfig):
        super().__init__(connectors, config)
        self.config = config

    def on_tick(self):
        if self.leftover_volume != 0:
            self.execute_volume()
        elif self.start_wait_timestamp <= self.current_timestamp:
            if self.trade_orders:
                self.logger().info("Waiting for previous orders to be filled")
                return
            if self.waiting_for_slippage:
                self.logger().info("Waiting for slippage to resolve")
                return
            self.randomize_volume()
            self.start_wait_timestamp = self.wait_time + self.current_timestamp
        else:
            # Cancel all orders
            for order_id in self.trade_orders:
                self.cancel('cube', self.config.trading_pair, order_id)

    def randomize_volume(self):
        buy_volume_last_24h = Decimal(sum([volume for t, volume in self.last_tick_volumes.items() if volume > 0 and t > self.current_timestamp - 86400]))
        sell_volume_last_24h = Decimal(abs(sum([volume for t, volume in self.last_tick_volumes.items() if volume < 0 and t > self.current_timestamp - 86400])))
        volume_last_24h = buy_volume_last_24h + sell_volume_last_24h  # 0
        # choose buy or sell depending on volume skew and recent volume
        if buy_volume_last_24h > sell_volume_last_24h:
            current_skew = buy_volume_last_24h / (buy_volume_last_24h + sell_volume_last_24h) * Decimal(2) - Decimal(1)
        elif sell_volume_last_24h > buy_volume_last_24h:
            current_skew = -sell_volume_last_24h / (buy_volume_last_24h + sell_volume_last_24h) * Decimal(2) + Decimal(1)
        else:
            current_skew = Decimal(0)

        planned_volume = Decimal((self.config.average_24h_volume + volume_last_24h) / (self.config.frequency * 24))
        if current_skew > self.config.volume_skew:  # sell more
            planned_volume = -planned_volume
        # otherwise buy more

        randomized_volume = planned_volume * (1 + self.config.randomness * Decimal(2 * random() - 1))
        self.last_tick_volumes[self.current_timestamp] = randomized_volume

        randomized_wait_time = 3600 / self.config.frequency * (1 + self.config.randomness * Decimal(2 * random() - 1))
        self.wait_time = float(randomized_wait_time)
        quote_asset = self.config.trading_pair.split('-')[1]
        self.logger().info(f"Planned volume: {'BUY' if randomized_volume >= 0 else 'SELL'} {round(abs(randomized_volume), 2)} {quote_asset}, Wait time for next volume: {round(randomized_wait_time)} second(s)")
        self.leftover_volume += randomized_volume

    def check_balance(self, volume, price) -> bool:
        if self.config.debug:
            self.logger().info("Checking Balance...")
        if volume < 0:
            base_token = self.config.trading_pair.split('-')[0]
            balance = self.connectors['cube'].get_balance(base_token)
            if balance < volume / price:
                self.logger().info(f"Not enough {base_token} (need: {volume / price} / have: {balance}), waiting for next tick")
                return False
        else:
            quote_token = self.config.trading_pair.split('-')[1]
            balance = self.connectors['cube'].get_balance(quote_token)
            if balance < abs(volume):
                self.logger().info(f"Not enough {quote_token} (need: {abs(volume)} / have: {balance}), waiting for next tick")
                return False
        return True

    def execute_volume(self):
        if self.config.debug:
            self.logger().info(f"Executing volume: {self.leftover_volume}$")
        order_book: OrderBook = self.connectors['cube'].get_order_book(self.config.trading_pair)
        volume, price = self.get_depth_with_max_slippage(order_book)
        if price is None:
            if self.config.debug:
                self.logger().info("Max slippage reached, waiting for next tick")
            self.waiting_for_slippage = True
            return
        self.waiting_for_slippage = False
        if not self.check_balance(volume, price):
            self.logger().info("Resetting leftover volume to 0")
            self.leftover_volume = 0
            return
        if volume > 0:
            amount = volume / price
            if self.config.trade_history_file:
                with open(self.config.trade_history_file, 'a') as f:
                    f.write(f"{self.current_timestamp},{self.config.trading_pair},BUY,{amount},{price}\n")
            order_id = self.buy('cube', self.config.trading_pair, amount, OrderType.LIMIT, price)
        else:
            amount = abs(volume) / price
            if self.config.trade_history_file:
                with open(self.config.trade_history_file, 'a') as f:
                    f.write(f"{self.current_timestamp},{self.config.trading_pair},SELL,{amount},{price}\n")
            order_id = self.sell('cube', self.config.trading_pair, amount, OrderType.LIMIT, price)
        self.trade_orders.append(order_id)
        self.leftover_volume -= volume
        self.leftover_volume = round(self.leftover_volume, 0)

    def get_depth_with_max_slippage(self, order_book: OrderBook) -> (Decimal, Optional[Decimal]):
        """Calculate the amount of volume that can be executed with the maximum slippage"""
        max_slippage = self.config.max_slippage / 100
        asks: List[OrderBookRow] = list(sorted(order_book.ask_entries()))
        bids: List[OrderBookRow] = list(sorted(order_book.bid_entries(), reverse=True))

        mid_price = Decimal(asks[0].price + bids[0].price) / 2
        if self.leftover_volume > 0:
            # Buy volume
            volume = Decimal(0)
            last_price: Optional[Decimal] = None
            while asks:
                ask = asks[0]
                if Decimal(ask.price) > mid_price * (1 + max_slippage):
                    break
                volume += Decimal(ask.amount)
                last_price = Decimal(ask.price)
                asks.pop(0)  # Remove the first element
                if volume >= self.leftover_volume / last_price:
                    return self.leftover_volume, last_price
            return volume, last_price
        else:
            # Sell volume
            volume = Decimal(0)
            last_price: Optional[Decimal] = None
            while bids:
                bid = bids[0]
                if Decimal(bid.price) < mid_price * (1 - max_slippage):
                    break
                volume += Decimal(bid.amount)
                last_price = Decimal(bid.price)
                bids.pop(0)
                if volume >= abs(self.leftover_volume) / last_price:
                    return self.leftover_volume, last_price  # negative volume for sell
            return -volume, last_price

    def did_fail_order(self, *args, **kwargs):
        self.logger().info("Order failed, removing from list")
        self.trade_orders.pop()

    def did_fill_order(self, event: OrderFilledEvent):
        if event.order_id in self.trade_orders:
            # Order filled, remove it from the list
            self.trade_orders.remove(event.order_id)
        if not self.trade_orders and self.config.debug:
            self.logger().info("All orders filled")

    def format_status(self) -> str:
        # TODO: Improve format status, print debug settings
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        balance_df = self.get_balance_df()
        lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        try:
            df = self.active_orders_df()
            lines.extend(["", "  Orders:"] + ["    " + line for line in df.to_string(index=False).split("\n")])
        except ValueError:
            lines.extend(["", "  No active maker orders."])

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)
