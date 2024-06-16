from .candle_base_manager import Candle_base_manager
import logging
from .binance_query import Binance_query
import time
import schedule
from .Sql_query import SQLQuery
from .checks_for_transactions import Check_for_transactions


class Engine:
    TIME_INTERVAL = 5  # xml minutes
    STOCK_FEE = 0.001
    STOCK_FEE_BNB = 0.00075
    USE_BNB_FEES = True

    candle_base_manager = Candle_base_manager()
    logger = logging.getLogger(__name__)
    sqlQuery = SQLQuery()

    def __init__(self):
        self.binance_query = Binance_query()
        self.check = Check_for_transactions()

    def start_engine(self):
        self.logger.info("Bot launched")
        self.sqlQuery.set_progress_status_engine("Bot launched", 1)

        flag_req = True
        while True:

            # Checks whether a scheduled task
            # is pending to run or not
            if not self.sqlQuery.get_status_engine():
                self.logger.info("Bot stopped")
                self.sqlQuery.set_progress_status_engine("", 0)
                return

            local_time = int(time.time())
            if (((local_time + 30) / 60 % 60 % self.TIME_INTERVAL) == 0
                or ((local_time + 29) / 60 % 60 % self.TIME_INTERVAL) == 0):
                try:
                    self.trade_flow()
                    flag_req = True
                except:
                    self.logger.error("Loop ended with an error")
                continue

            if True:
                flag_req = False
                percent_rest = int(((local_time + 30)/60%60%self.TIME_INTERVAL)/self.TIME_INTERVAL*100)
                self.sqlQuery.set_progress_status_engine("Waiting", percent_rest)

            time.sleep(1)

    def trade_flow(self):

        local_time = int(time.time())
        server_time = self.binance_query.server_time()
        shift_seconds = server_time - local_time
        self.binance_query.set_shift_seconds(shift_seconds)

        self.logger.info("Local time: {local_time}"
                         .format(local_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(local_time))))
        self.logger.info("Server time: {server_time}"
                         .format(server_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(server_time))))
        self.logger.info("Diff: {diff:0.8f}".format(diff=shift_seconds))

        print("Local time: {local_time}"
              .format(local_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(local_time))))
        self.sqlQuery.set_progress_status_engine("Start Time: {local_time}".format(
            local_time=time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(local_time))), 1)

        self.candle_base_manager.update_pair_tables()

        # получаем все невыполненые ордера
        self.logger.info("Get all unexecuted orders from the database")
        orders_info = self.sqlQuery.get_unexecuted_orders()

        if orders_info:

            orders_info_logger = [(order, orders_info[order]['order_pair']) for order in orders_info]
            orders_str = "\n".join([f"Order ID: {order}, Pair: {pair}" for order, pair in orders_info_logger])
            self.logger.info("Received unfilled orders from the database:\n{orders}".format(orders=orders_str))

            count_order = 0
            for order in orders_info:

                self.sqlQuery.set_progress_status_engine("Processing Order {order}".format(order=order),
                                                         count_order * 50 // len(orders_info))
                count_order += 1

                self.logger.info("Working with order {order}".format(order=order))

                stock_order_data = self.binance_query.orderInfo(order, orders_info[order]['order_pair'])

                order_status = stock_order_data['status']
                self.logger.info("Order status {order} - {status}".format(order=order, status=order_status))

                if orders_info[order]['order_type'] == 'buy' and order_status == 'FILLED':
                    if self.sqlQuery.get_finished_buy_status(order) is None:
                        self.logger.info("""Order {order} done, received {exec_qty:0.8f}.""".format(
                            order=order, exec_qty=float(stock_order_data['executedQty'])
                        ))
                        self.sqlQuery.set_finished_buy_order(order)

                    signal, max_price, min_price = self.check.check_signal_sell(orders_info[order]['order_pair'])

                    if not signal:
                        continue

                    current_limits = self.binance_query.get_current_limits(orders_info[order]['order_pair'])
                    current_price = self.binance_query.get_current_price(orders_info[order]['order_pair'])
                    has_amount = orders_info[order]['buy_amount'] * (
                        (1 - self.STOCK_FEE) if not self.USE_BNB_FEES else 1)
                    sell_amount = self.adjust_to_step(has_amount, current_limits['filters'][1]['stepSize'])

                    self.logger.info("""Initial {buy_initial:0.8f}, minus commission {has_amount:0.8f},
                                    Can only sell {sell_amount:0.8f}
                                    Selling price: {need_price:0.8f}
                                    Get: {need_to_earn:0.8f}""".format(
                        buy_initial=orders_info[order]['buy_amount'], has_amount=has_amount, sell_amount=sell_amount,
                        need_price=current_price, need_to_earn=current_price * sell_amount
                    ))

                    if self.check.check_trade_am_and_current_limits(current_price * has_amount, current_limits):
                        continue

                    self.logger.info(
                        'Sell Order Calculated: amount {amount:0.8f}, price: {rate:0.8f}'.format(
                            amount=sell_amount, rate=current_price)
                    )

                    new_order = self.binance_query.create_order(orders_info[order]['order_pair'], sell_amount,
                                                                current_limits, 'SELL')

                    if not self.check.check_new_order(new_order, 'SELL'):
                        continue

                    self.sqlQuery.set_new_sell_order(order, new_order, sell_amount, current_price)

                elif orders_info[order]['order_type'] == 'buy' and order_status == 'NEW':
                    cancel_order = self.binance_query.cancel_order(order, orders_info[order]['order_pair'])
                    self.check.check_cancel_order(cancel_order)
                    self.sqlQuery.set_cancel_order(order)

                elif orders_info[order]['order_type'] == 'buy' and order_status == 'PARTIALLY_FILLED':
                    self.logger.info("Order {order} partially filled".format(order=order))

                elif orders_info[order]['order_type'] == 'sell' and order_status == 'FILLED':
                    self.logger.warning("Sell order {order} filled".format(order=order))
                    self.sqlQuery.set_finished_order(order)

                elif orders_info[order]['order_type'] == 'sell' and order_status == 'NEW':
                    cancel_order = self.binance_query.cancel_order(order, orders_info[order]['order_pair'])
                    self.check.check_cancel_order(cancel_order)
                    # self.sqlQuery.set_cancel_order(order)

        else:
            self.logger.info("There are no unexecuted orders in the database")

        free_pairs = self.sqlQuery.get_free_pairs()
        if free_pairs:

            self.logger.info('Free pairs found: {pairs}'.format(pairs=list(free_pairs.keys())))

            count_pair = 0
            for pair_name, pair_obj in free_pairs.items():

                self.sqlQuery.set_progress_status_engine("Processing Pair {pair}".format(pair=pair_name),
                                                         50 + count_pair * 50 // len(free_pairs.items()))
                count_pair += 1

                self.logger.info("Working with {pair}".format(pair=pair_name))

                balances = self.binance_query.get_balances(pair_obj)
                self.logger.info("Balance {balance}".format(
                    balance=["{k}:{bal:0.8f}".format(k=k, bal=balances[k]) for k in balances]))

                if self.check.check_balances_and_spend_sum(balances, pair_obj):
                    continue

                signal, max_price, min_price = self.check.check_signal_buy(pair_name)

                if not signal:
                    continue

                current_limits = self.binance_query.get_current_limits(pair_name)
                current_price = self.binance_query.get_current_price(pair_name)

                buy_amount = self.adjust_to_step(pair_obj['spend_sum'] / current_price,
                                                 current_limits['filters'][1]['stepSize'])

                if self.check.check_buy_amount_and_current_limits(buy_amount, current_limits, current_price, pair_obj):
                    continue

                trade_am = current_price * buy_amount
                self.logger.info("""
                                Average price {av_price:0.8f}, 
                                volume after reduction {buy_amount:0.8f},
                                total deal size {trade_am:0.8f}
                                """.format(
                    av_price=current_price, buy_amount=buy_amount, trade_am=trade_am
                ))

                if self.check.check_trade_am_and_current_limits(trade_am, current_limits):
                    continue

                self.logger.info(
                    'Buy Order Calculated: amount {amount:0.8f}, price: {rate:0.8f}'.format(amount=buy_amount,
                                                                                            rate=current_price)
                )

                new_order = self.binance_query.create_order(pair_name, buy_amount, current_limits, 'BUY')

                if not self.check.check_new_order(new_order, 'BUY'):
                    continue

                self.sqlQuery.set_new_buy_order(pair_name, new_order, buy_amount, current_price, max_price, min_price)

        else:
            self.logger.info("There are no free pairs")

        self.sqlQuery.set_progress_status_engine("End of Cycle", 100)

    @staticmethod
    def adjust_to_step(value, step, increase=False):
        return ((int(value * 100000000) - int(value * 100000000) % int(
            float(step) * 100000000)) / 100000000) + (float(step) if increase else 0)
