import asyncio
import os
from decimal import Decimal
from typing import Dict, List

from pydantic import Field

from hummingbot.client.config.config_data_types import ClientFieldData
from hummingbot.connector.connector_base import ConnectorBase
from hummingbot.core.data_type.common import OrderType
from hummingbot.core.data_type.order_book import OrderBookRow
from hummingbot.core.event.events import OrderFilledEvent
from hummingbot.core.utils.async_utils import safe_ensure_future
from hummingbot.data_feed.candles_feed.candles_factory import CandlesConfig
from hummingbot.strategy.strategy_v2_base import StrategyV2Base, StrategyV2ConfigBase
from robotter import constants as CONSTANTS
from robotter.connectors.btc import BitcoinConnector
from robotter.connectors.okx import OKXConnector
from robotter.connectors.unisat import UnisatConnector
from robotter.utils import (
    PlannedArbitrage,
    QueuedArbitrage,
    calculate_price_difference_and_token_amount,
    ticker_to_cube,
)


class RuneArbitrageConfig(StrategyV2ConfigBase):
    script_file_name: str = Field(default_factory=lambda: os.path.basename(__file__))
    candles_config: List[CandlesConfig] = []
    controllers_config: List[str] = []

    max_ask_slippage: Decimal = Field(
        default=0.01, gt=0,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enter the max ask slippage (as a decimal, e.g., 0.01 for 1%): ",
            prompt_on_new=False))

    debug: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable debug mode?",
            prompt_on_new=True))

    dry_run: bool = Field(
        default=True,
        client_data=ClientFieldData(
            prompt=lambda mi: "Enable dry run mode?",
            prompt_on_new=True))

    add_debug_arbitrage: bool = Field(
        default=False,
        client_data=ClientFieldData(
            prompt=lambda mi: "Add debug arbitrage?",
            prompt_on_new=True))


class RuneArbitrage(StrategyV2Base):
    """
    Arbitrage seeker between Cube and other exchanges.
    Should only buy runes from other exchanges and sell them on Cube.
    Difficulty is in reverse-engineering the other exchange's bitcoin transactions & monitoring the mempool.

    Base Strategy Overview:
    - If an arbitrage is spotted, a bitcoin transaction must be dispatched to buy the rune on the other exchange.
    - In the meantime, the second leg of the arbitrage must be prepared on Cube.
    - We use a heuristic to determine the optimal fee for the bitcoin transaction. Only on block completion do we know
    if we commit to the arbitrage or cancel it.

    Optional Improvements:
    - We need to make sure to pay only minimal fees on the bitcoin transaction, therefore we need to constantly monitor
    the mempool and adjust the fees accordingly.
    - We either commit to the arbitrage by selling the rune on Cube, or we cancel the arbitrage if the transaction will
    cease to be profitable.
    - As fees are being adjusted, more profitable arbitrage opportunities may arise, then we need to replace the current
    arbitrage with the new one, or add it to the mempool.
    - Some orders have more runes than others, so we need to calculate the buy price per rune based on the fee.
    """
    """
    TODO: Refactor the async_on_tick flow by create a new arbitrage object and it will propagate the flow
    Solution:
    create "arbitrage" object at start then propagate it through the flow:
        arbitrage   -> self.plan_arbitrage(arbitrage)
                    -> self.balance_checker(arbitrage)
                    -> self.check_valid_arbitrage(arbitrage)
                    -> self.execute_legs(arbitrage)
                    -> self.report_pnl(arbitrage)
    at the end of the flow, "arbitrage" object should contain all the information needed to report PnL
    """
    planned_arbitrages: Dict[str, PlannedArbitrage] = {}  # rune name: arbitrage
    queued_arbitrages: Dict[str, QueuedArbitrage] = {}  # rune name: arbitrage

    # References
    btc_price = 0
    recommended_fee_rate = 0
    rebalanced = False
    trade_orders = {}  # orders on Cube, pair: orders
    is_task_ongoing = False

    # Wait condition
    start_wait_timestamp = 0
    wait_time = 60  # 1 minute

    # Debug prints
    dry_run_lines = []

    # Custom connectors
    bitcoin = BitcoinConnector()
    okx = OKXConnector(bitcoin_connector=bitcoin)
    unisat = UnisatConnector()

    def __init__(self, connectors: Dict[str, ConnectorBase], config: RuneArbitrageConfig):
        super().__init__(connectors, config)
        self.config = config

    async def arbable_runes(self, cube_ticker_symbols: List[str]) -> Dict[str, List[str]]:
        """
        Get the OKX rune symbols that are available on Cube.
        """
        okx_markets = await self.okx.get_all_rune_tickers()
        okx_runes = []
        unisat_markets = await self.unisat.get_all_rune_tickers()
        unisat_runes = []
        for cube_symbol in cube_ticker_symbols:
            for okx_market in okx_markets:
                cube_symbol = cube_symbol.split("-")[0]
                if cube_symbol == ticker_to_cube(okx_market):
                    okx_runes.append(okx_market)
            for unisat_market in unisat_markets:
                cube_symbol = cube_symbol.split("-")[0]
                if cube_symbol == ticker_to_cube(unisat_market):
                    unisat_runes.append(unisat_market)
        return {"okx": okx_runes, "unisat": unisat_runes}

    def on_tick(self):
        if self.start_wait_timestamp <= self.current_timestamp:
            if not self.is_task_ongoing:
                self.is_task_ongoing = True
                safe_ensure_future(self.async_on_tick(), loop=asyncio.get_event_loop())
            # TODO: We solved async task, now we could add cache back

            self.start_wait_timestamp = self.wait_time + self.current_timestamp

    async def async_on_tick(self):
        # Update references
        self.btc_price = self.connectors["cube"].get_mid_price(
            "BTC-USDC"
        )  # Mid-price is a good estimate, as BTC has low spreads
        fees = await self.bitcoin.fetch_recommended_fee_rate()
        self.recommended_fee_rate = int(fees["fastestFee"])

        # Find arbitrage possibilities
        await self.plan_arbitrage()

        # Check if we have enough balance to execute the arbitrage
        self.balance_checker()

        for rune, arbitrage in self.planned_arbitrages.items():
            if self.config.dry_run:
                self.dry_run_lines.append(f"\nDry Run for [{rune}]\n---------")
            await self.execute_first_leg(arbitrage)
            if self.queued_arbitrages.get(rune):
                await self.check_queued_arbitrage(arbitrage)
            else:
                if self.config.debug:
                    self.logger().info(f"First leg for {rune} was not executed successfully!")
                    self.notify_hb_app(f"First leg for {rune} was not executed successfully!")

        # clear planned_arb
        self.planned_arbitrages = {}

        if self.config.dry_run:
            self.logger().info("\n".join(self.dry_run_lines))
            self.notify_hb_app("\n".join(self.dry_run_lines))
            self.dry_run_lines = []

        self.is_task_ongoing = False

    async def plan_arbitrage(self):
        if self.config.debug:
            self.logger().info("Finding Arbitrage...")
            self.notify_hb_app("Finding Arbitrage...")
        cube_ticker_symbols = list(self.markets["cube"])

        all_arbable_runes = await self.arbable_runes(cube_ticker_symbols)
        # TODO: Add OKX later
        del all_arbable_runes["okx"]
        all_failed_warnings = ["\nFailed Arbitrages:\n---------\n"]
        found_arbitrages = ["\nFound Arbitrages:\n---------\n"]

        for exchange, runes in all_arbable_runes.items():
            for rune in runes:
                asks_response = {}

                if exchange == "okx":
                    asks_response = await self.okx.fetch_asks(rune)
                elif exchange == "unisat":
                    asks_response = await self.unisat.fetch_asks(rune)

                asks = self.process_asks_response(asks_response, exchange)
                asks = sorted(asks, key=lambda x: x["unitPrice"]["usdPrice"])  # Lowest price first

                cube_bids = self.connectors["cube"].get_order_book(f"{ticker_to_cube(rune)}-USDC").bid_entries()
                cube_bids = sorted(cube_bids, key=lambda x: x.price, reverse=True)  # Highest price first
                # Find the arbitrage
                total_profit = Decimal(0)
                profitable_asks = []
                profitable_cube_bids = []
                ask_remaining_trade_amount = Decimal(0)
                bid_remaining_trade_amount = Decimal(0)
                first_ask_filled = False

                ask_price_debug = round(Decimal(asks[0]["unitPrice"]["usdPrice"]) if len(asks) > 0 else Decimal(0), 8)
                ask_min_size = Decimal(min([Decimal(ask["amount"]) for ask in asks]))
                bid_price_debug = round(Decimal(cube_bids[0].price) if len(cube_bids) > 0 else Decimal(0), 8)
                # top_bid_size = round(Decimal(cube_bids[0].amount) if len(cube_bids) > 0 else Decimal(0), 2)

                # DEBUG: Artificially add a arbitrage opportunity
                if self.config.dry_run and self.config.debug and self.config.add_debug_arbitrage and (
                        rune == "SATOSHI•NAKAMOTO" or rune == "RSIC•GENESIS•RUNE"):
                    new_top_bid_price = Decimal(Decimal(asks[0]["unitPrice"]["usdPrice"]) * Decimal(1.5))
                    cube_bids = cube_bids + [
                        OrderBookRow(price=float(new_top_bid_price), amount=float(asks[0]["amount"]), update_id=0)]
                    cube_bids = sorted(cube_bids, key=lambda x: x.price, reverse=True)

                price_difference, token_amount = calculate_price_difference_and_token_amount(asks, cube_bids)

                if price_difference <= 0:
                    all_failed_warnings.append(f"[{rune}] No arbitrage possible!\n")
                    all_failed_warnings.append(f"\u21AA[CUBE] Bid Price: {bid_price_debug} USD\n")
                    all_failed_warnings.append(f"\u21AA[{exchange.upper()}] Ask Price: {ask_price_debug} USD\n\n")
                else:
                    # Filter asks to allow only ask that has less amount than token_amount
                    asks = [ask for ask in asks if Decimal(ask["amount"]) <= token_amount]

                    # # If debug turned on, let token amount be the minimum amount of all asks
                    # if self.debug:
                    #     if len(asks) > 0:
                    #         # token_amount = Decimal(min([Decimal(ask["amount"]) for ask in asks]))
                    #         asks = [ask for ask in asks if Decimal(ask["amount"]) <= token_amount]

                    asks = sorted(asks, key=lambda x: x["unitPrice"]["usdPrice"])

                    if len(asks) == 0:
                        all_failed_warnings.append(
                            f"[{rune}] No arbitrage possible! Not enough liquidity for min ask\n")
                        all_failed_warnings.append(
                            f"\u21AA[CUBE] Bid Price: {bid_price_debug} USD | profitable amount: {round(token_amount, 2)}\n")
                        all_failed_warnings.append(
                            f"\u21AA[{exchange.upper()}] Ask Price: {ask_price_debug} USD | ask min amount: {ask_min_size}\n\n")

                    while asks and cube_bids:
                        if first_ask_filled:
                            break  # first ask consumed  # TODO: Implement multiple levels of asks

                        ask_price: Decimal = Decimal(asks[0]["unitPrice"]["usdPrice"])
                        bid_price: Decimal = Decimal(cube_bids[0].price)

                        if ask_remaining_trade_amount == Decimal(0):
                            ask_remaining_trade_amount = Decimal(asks[0]["amount"])

                        if bid_remaining_trade_amount == Decimal(0):
                            bid_remaining_trade_amount = Decimal(cube_bids[0].amount)

                        trade_amount = Decimal(min(ask_remaining_trade_amount, bid_remaining_trade_amount))
                        profit = (bid_price - ask_price) * trade_amount
                        if profit <= 0:
                            break

                        total_profit += profit

                        ask_remaining_trade_amount -= trade_amount
                        bid_remaining_trade_amount -= trade_amount

                        if ask_remaining_trade_amount <= Decimal(0):
                            # Break if ask_remaining_trade_amount is negative and bigger than slippage amount
                            if abs(ask_remaining_trade_amount) > (self.config.max_ask_slippage * token_amount):
                                all_failed_warnings.append(
                                    f"[{rune}][{exchange.upper()}] has {abs(ask_remaining_trade_amount)} token above allowed slippage \n"
                                )
                                break  # Break because slippage is too high

                            profitable_asks.append(asks[0])
                            asks.pop(0)
                            first_ask_filled = True

                        if bid_remaining_trade_amount == Decimal(0):
                            profitable_cube_bids.append(cube_bids[0])
                            cube_bids.pop(0)
                        else:
                            partial_bid = OrderBookRow(
                                price=cube_bids[0].price,
                                amount=float(trade_amount),
                                update_id=cube_bids[0].update_id,
                            )
                            profitable_cube_bids.append(partial_bid)
                            cube_bids.pop(0)

                if profitable_asks and profitable_cube_bids and total_profit > 0:
                    self.planned_arbitrages[rune] = PlannedArbitrage(
                        rune=rune,
                        first_leg_exchange=exchange,
                        first_leg_orders=profitable_asks,
                        second_leg_exchange="cube",
                        second_leg_orders=profitable_cube_bids,
                        size=Decimal(
                            min(
                                sum([float(order["amount"]) for order in profitable_asks]),
                                sum([float(order.amount) for order in profitable_cube_bids]),
                            )
                        ),
                        expected_profit=total_profit,
                    )
                    found_arbitrages.append(f"{self.planned_arbitrages[rune].__repr__()}\n\n")
                elif len(profitable_asks) == 0 and len(profitable_cube_bids) > 0:
                    all_failed_warnings.append(f"[{rune}] No arbitrage possible! Bids liquidity too low\n")
                    all_failed_warnings.append(f"\u21AA[CUBE] Bid Price: {bid_price_debug} USD\n")
                    all_failed_warnings.append(
                        f"\u21AA[CUBE] Missing liquidity: {round(ask_remaining_trade_amount, 2)} token\n")
                    all_failed_warnings.append(f"\u21AA[{exchange.upper()}] Ask Price: {ask_price_debug} USD\n\n")
                elif len(profitable_asks) == 0 and len(profitable_cube_bids) == 0 and price_difference > 0 and len(
                        asks) > 0:
                    ask_price_after_filtered = round(Decimal(asks[0]["unitPrice"]["usdPrice"]), 8)
                    all_failed_warnings.append(f"[{rune}] No arbitrage possible after filtered unfillable asks\n")
                    all_failed_warnings.append(f"\u21AA[CUBE] Bid Price: {bid_price_debug} USD\n")
                    all_failed_warnings.append(f"\u21AA[{exchange.upper()}] Ask Price: {ask_price_debug} USD\n")
                    all_failed_warnings.append(
                        f"\u21AA[{exchange.upper()}] Ask Price after filtered: {ask_price_after_filtered} USD\n\n")

        if len(found_arbitrages) > 1:
            self.logger().info("".join(found_arbitrages))
            self.notify_hb_app("".join(found_arbitrages))

        if self.config.debug and len(all_failed_warnings) > 1:
            # NOTE: Enable this in admin channel to debug only
            self.logger().info("".join(all_failed_warnings))
            self.notify_hb_app("".join(all_failed_warnings))
            # TODO: Make the debug message something like this:
            # [SATOSHI•NAKAMOTO][UNISAT] Price Diff: -0.00423192 -> No arbitrage possible!
            # ↪[CUBE] Bid Price: 2.16900000 USD
            # ↪[OKX] Ask Price: 2.17853672 USD
            # ↪[UNISAT] Ask Price: 2.17323192 USD

    def add_tx_price(self, order: dict) -> dict:
        fee = self.bitcoin.calculate_fee(CONSTANTS.AVG_UNISAT_TX_BYTES, self.recommended_fee_rate)
        order["totalPrice"]["priceOriginal"] = order["totalPrice"]["price"]
        order["totalPrice"]["price"] = Decimal(order["totalPrice"]["price"]) + fee
        order["totalPrice"]["satPrice"] = Decimal(order["totalPrice"]["price"] * Decimal(1e8))
        order["totalPrice"]["usdPrice"] = Decimal(order["totalPrice"]["price"] * self.btc_price)
        order["unitPrice"]["priceOriginal"] = order["unitPrice"]["price"]
        order["unitPrice"]["price"] = order["totalPrice"]["price"] / Decimal(order["amount"])
        order["unitPrice"]["satPrice"] = order["totalPrice"]["satPrice"] / Decimal(order["amount"])
        order["unitPrice"]["usdPrice"] = order["totalPrice"]["usdPrice"] / Decimal(order["amount"])
        return order

    def process_asks_response(self, response: dict, exchange: str) -> List[dict]:
        asks = []
        if exchange == "okx":
            asks = response["data"]["items"]
            asks = [self.add_tx_price(order) for order in asks]

        if exchange == "unisat":
            asks = response["data"]["list"]

            # for each ask, add totalPrice like in OKX
            for ask in asks:
                ask["totalPrice"] = {
                    "price": Decimal(ask["price"] / 1e8),
                    "satPrice": Decimal(ask["price"]),
                    "usdPrice": Decimal(ask["price"] / 1e8) * self.btc_price,
                }
                ask["unitPrice"] = {
                    "price": Decimal(ask["unitPrice"] / 1e8),
                    "satPrice": Decimal(ask["unitPrice"]),
                    "usdPrice": Decimal(ask["unitPrice"] / 1e8) * self.btc_price,
                }

            asks = [self.add_tx_price(order) for order in asks]
        for ask in asks:
            ask["exchange"] = exchange
        return asks

    # SECTION: Balance Checker
    def balance_checker(self):
        if self.config.dry_run:
            self.logger().info("Dry run mode, skipping balance check...")
            return

        self.logger().info("Checking Balance...")
        for rune, arbitrage in self.planned_arbitrages.items():
            cube_balance = self.get_balance_df()
            rune_balance = cube_balance[cube_balance["Asset"] == rune]["Total Balance"].item()
            if rune_balance < arbitrage.size:
                self.logger().info(f"Insufficient balance for {rune}!")
                self.planned_arbitrages.pop(rune)

            required_btc_balance = sum([order["totalPrice"]["price"] for order in arbitrage.first_leg_orders])
            btc_balance = self.bitcoin.get_btc_balance(CONSTANTS.BITCOIN_ADDRESS)
            if btc_balance < required_btc_balance:
                self.logger().info("Insufficient BTC balance!")
                self.planned_arbitrages.pop(rune)

    async def execute_first_leg(self, arbitrage: PlannedArbitrage):
        transactions = {}
        num_of_orders = len(arbitrage.first_leg_orders)
        for order in arbitrage.first_leg_orders:
            exchange = order["exchange"]
            if exchange == CONSTANTS.UNISAT_EXCHANGE:
                connector = self.unisat
            elif exchange == CONSTANTS.OKX_EXCHANGE:
                connector = self.okx
            else:
                raise NotImplementedError(f"Exchange {arbitrage.first_leg_orders[0]['exchange']} not implemented!")

            params = {
                "auction_id": order["auctionId"],
                "price_sats": order["price"],
                "fee_rate": self.recommended_fee_rate,
            }

            if self.config.dry_run:
                transactions["dry_run"] = params

                order_size_in_usd = round(
                    sum([order["totalPrice"]["usdPrice"] for order in arbitrage.first_leg_orders]), 2)

                self.dry_run_lines.append(
                    f"First leg: BUY {arbitrage.size} {arbitrage.rune} at {order['price']} sat for {order_size_in_usd} USD on {order['exchange']}")

            else:
                # Create the buy transactions
                buy_transaction = await connector.create_bid_PSBT(**params)

                if not buy_transaction["msg"] == "ok":
                    self.logger().info(f"Transaction failed for {arbitrage.rune}! Reason: {buy_transaction['msg']}")
                    self.notify_hb_app(f"Transaction failed for {arbitrage.rune}! Reason: {buy_transaction['msg']}")
                    break

                submit_resp = await connector.sign_and_submit_bid_tx(
                    transaction_info=buy_transaction, transaction_params=params
                )

                if not submit_resp["msg"] == "ok":
                    self.logger().info(f"Transaction failed for {arbitrage.rune}! Reason: {submit_resp['msg']}")
                    self.notify_hb_app(f"Transaction failed for {arbitrage.rune}! Reason: {submit_resp['msg']}")
                    break

                tx_hash = submit_resp["data"]["txid"]
                transactions[tx_hash] = params  # TODO: Create dataclass for tx params?

        if (len(transactions) == num_of_orders):
            # TODO: Better hanlding when not all arbitrage are successful
            if self.config.debug and not self.config.dry_run:
                self.logger().info(f"First leg for {arbitrage.rune} was executed successfully!")
                self.notify_hb_app(f"First leg for {arbitrage.rune} was executed successfully!")

            blockchain_height = 0
            try:
                blockchain_height = int(self.bitcoin.get_blockcount())
            except Exception as e:
                self.logger().info(f"Failed to get blockchain height: {e}")
                self.notify_hb_app(f"Failed to get blockchain height: {e}")

            queued_arbitrage = arbitrage.queue(transactions, blockchain_height)
            self.queued_arbitrages[arbitrage.rune] = queued_arbitrage
        else:
            if self.config.debug and not self.config.dry_run:
                self.logger().info(f"First leg for {arbitrage.rune} was not executed successfully!")
                self.notify_hb_app(f"First leg for {arbitrage.rune} was not executed successfully!")

    async def check_queued_arbitrage(self, arbitrage: PlannedArbitrage):
        """
        Check if the first leg transactions have been queued.
        """
        # Since the transaction will be the gatekeeping by unisat, we can safely fire the second leg
        queued_arbitrage = self.queued_arbitrages[arbitrage.rune]

        if queued_arbitrage is not None:
            await self.execute_second_leg(queued_arbitrage)
            # TODO: What if some txn are succesful, others not?

    async def execute_second_leg(self, arbitrage: QueuedArbitrage):
        # If Dry run mode, print the order that bot will make
        if self.config.dry_run:
            order_size = round(arbitrage.size, 2)
            order_size_in_usd = round(
                sum([(Decimal(min(Decimal(second_leg_order.amount), Decimal(order_size))) * Decimal(
                    second_leg_order.price)) for second_leg_order in arbitrage.second_leg_orders]), 2)
            order_price = round(arbitrage.second_leg_orders[-1].price, 8)
            order_ticker = f"{arbitrage.rune}-USDC"

            self.dry_run_lines.append(
                f"Second leg: SELL {order_size} {order_ticker} at {order_price} USD for {order_size_in_usd} USDC on CUBE")

            # btc order
            btc_amount = round(Decimal(sum([order["totalPrice"]["price"] for order in arbitrage.first_leg_orders])), 8)
            btc_amount_in_usd = round(btc_amount * self.btc_price, 8)
            btc_price = self.btc_price
            btc_ticker = "BTC-USDC"

            self.dry_run_lines.append(
                f"Second leg: BUY {btc_amount} {btc_ticker} at {btc_price} USD for {btc_amount_in_usd} USDC on CUBE")
            self.dry_run_lines.append(
                f"\u21AAProfit: {round(Decimal(order_size_in_usd - Decimal(btc_amount_in_usd)), 2)} USD")

            self.queued_arbitrages.pop(arbitrage.rune)
            return

        # Sell the rune on Cube
        cube_ticker = f"{arbitrage.rune}-USDC"
        cube_order = self.sell(
            "cube", cube_ticker, arbitrage.size, OrderType.MARKET, Decimal(arbitrage.second_leg_orders[-1].price)
        )
        self.trade_orders[cube_ticker] = self.trade_orders.get(cube_ticker, []) + [cube_order]

        result = await self.check_if_order_filled(cube_ticker, cube_order)

        if result is True:
            # Buy BTC with the proceeds
            btc_amount = Decimal(sum([order["totalPrice"]["price"] for order in arbitrage.first_leg_orders]))
            btc_order = self.buy("cube", "BTC-USDC", btc_amount, OrderType.MARKET, self.btc_price)
            self.trade_orders["BTC-USDC"] = self.trade_orders.get("BTC-USDC", []) + [btc_order]

            result = await self.check_if_order_filled("BTC-USDC", btc_order)
            if result is True:
                if self.config.debug:
                    self.logger().info(f"Second leg for {arbitrage.rune} was executed successfully!")
                    self.notify_hb_app(f"Second leg for {arbitrage.rune} was executed successfully!")
                else:
                    self.logger().info(f"Second leg for {arbitrage.rune} was not executed successfully!")
                    self.notify_hb_app(f"Second leg for {arbitrage.rune} was not executed successfully!")
        else:
            if self.config.debug:
                self.logger().info(f"Second leg for {arbitrage.rune} was not executed successfully!")
                self.notify_hb_app(f"Second leg for {arbitrage.rune} was not executed successfully!")

        self.queued_arbitrages.pop(arbitrage.rune)
        # TODO: Check if the arbitrage was successful, final statistics & profit

    async def check_if_order_filled(self, ticker: str, order_id: str):
        # Check if the order was filled
        retry_counter = 0
        while True:
            if order_id not in self.trade_orders[ticker]:
                return True

            if retry_counter > 5:
                return False

            await asyncio.sleep(1)
            retry_counter += 1

    def did_fill_order(self, event: OrderFilledEvent):
        for orders in self.trade_orders[event.trading_pair]:
            for order_id in orders:
                if order_id == event.order_id:
                    # Order filled, remove it from the list
                    self.trade_orders[event.trading_pair].remove(order_id)

    def format_status(self) -> str:
        # TODO: Improve format status, print debug settings
        if not self.ready_to_trade:
            return "Market connectors are not ready."
        lines = []
        warning_lines = []
        warning_lines.extend(self.network_warning(self.get_market_trading_pair_tuples()))

        # balance_df = self.get_balance_df()
        # lines.extend(["", "  Balances:"] + ["    " + line for line in balance_df.to_string(index=False).split("\n")])

        # Add some test data to self.arbitrages
        test_arbitrages = {
            "okx": {
                "RSIC•GENESIS•RUNE": PlannedArbitrage(
                    rune="RSIC•GENESIS•RUNE",
                    first_leg_exchange="okx",
                    first_leg_orders=[
                        {
                            "auctionId": "87tdnhidllyh2nc3q7ndwj2mg67q0ubg",
                            "totalPrice": {"price": 0.0001, "satPrice": 1000, "usdPrice": 1000},
                            "unitPrice": {"price": 0.0001, "satPrice": 1000, "usdPrice": 1000},
                            "amount": 100,
                        }
                    ],
                    second_leg_exchange="cube",
                    second_leg_orders=[OrderBookRow(price=0.0002, amount=50, update_id=1)],
                    size=Decimal(50),
                    expected_profit=Decimal(50),
                ),
            },
            "unisat": {
                "RSIC•GENESIS•RUNE": PlannedArbitrage(
                    rune="RSIC•GENESIS•RUNE",
                    first_leg_exchange="unisat",
                    first_leg_orders=[
                        {
                            "auctionId": "87tdnhidllyh2nc3q7ndwj2mg67q0ubg",
                            "totalPrice": {"price": 0.0001, "satPrice": 1000, "usdPrice": 1000},
                            "unitPrice": {"price": 0.0001, "satPrice": 1000, "usdPrice": 1000},
                            "amount": 100,
                        }
                    ],
                    second_leg_exchange="cube",
                    second_leg_orders=[OrderBookRow(price=0.0002, amount=50, update_id=1)],
                    size=Decimal(50),
                    expected_profit=Decimal(50),
                ),
            },
        }

        if len(self.planned_arbitrages) == 0:
            lines.append("No arbitrage opportunities found.\n")
            lines.append("If any arbitrage opportunities are found, they will be displayed here.\n")
            lines.append("Example arbitrage opportunities:\n")
            for exchange, arbitrages in test_arbitrages.items():
                for rune, arbitrage in arbitrages.items():
                    lines.extend(["", f"*** [{rune}] ***", arbitrage.__repr__()])

        if len(self.planned_arbitrages) > 0:
            lines.append("Arbitrage opportunities found so far:\n")
            for rune, arbitrage in self.planned_arbitrages.items():
                lines.extend(["", f"*** {rune} ***", arbitrage.__repr__()])

        warning_lines.extend(self.balance_warning(self.get_market_trading_pair_tuples()))
        if len(warning_lines) > 0:
            lines.extend(["", "*** WARNINGS ***"] + warning_lines)
        return "\n".join(lines)
