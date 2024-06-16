import logging
from .candle_base_manager import Candle_base_manager
from .signal import Signal

class Check_for_transactions:

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Check_for_transactions, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.candle_base_manager = Candle_base_manager()
        self.signal = Signal()

    logger = logging.getLogger(__name__)

    def check_balances_and_spend_sum(self, balances, pair_obj):
        if balances[pair_obj['base']] < pair_obj['spend_sum']:
            self.logger.warning('To create a buy order, you need a minimum {min_qty:0.8f} {curr}, pass'.format(
                min_qty=pair_obj['spend_sum'], curr=pair_obj['base']
            ))
            return True
        else:
            return False

    def check_signal_buy(self, pair_name):

        # update tables for pairs
        #self.candle_base_manager.update_pair_tables()
        #self.candle_base_manager.update_pair_table(pair_name)

        signal, max_price, min_price = self.signal.signal_buy(pair_name)

        if signal:
            self.logger.warning("Buy signal appears for the pair {pair}".format(pair=pair_name))
            return True, max_price, min_price
        else:
            self.logger.info("No buy signal for the pair {pair}".format(pair=pair_name))
            return False, max_price, min_price

    def check_signal_sell(self, pair_name):

        # update tables for pairs
        #self.candle_base_manager.update_pair_tables()

        signal, max_price, min_price = self.signal.signal_sell(pair_name)

        if signal:
            self.logger.warning("Sell signal appears for the pair {pair}".format(pair=pair_name))
            return True, max_price, min_price
        else:
            self.logger.info("No sell signal for the pair {pair}".format(pair=pair_name))
            return False, max_price, min_price

    def check_buy_amount_and_current_limits(self, my_amount, current_limits, current_price, pair_obj):
        if my_amount < float(current_limits['filters'][1]['stepSize']) or my_amount < float(
                current_limits['filters'][1]['minQty']):
            self.logger.error("""
                                            Minimum lot amount: {min_lot:0.8f}
                                            Minimum step amount: {min_lot_step:0.8f}
                                            With our own money we could buy {wanted_amount:0.8f}
                                            After reduction to the minimum step, we can buy {my_amount:0.8f}
                                            Buying is not possible, exit. Increase your bet
                                        """.format(
                wanted_amount=pair_obj['spend_sum'] / current_price,
                my_amount=my_amount,
                min_lot=float(current_limits['filters'][1]['minQty']),
                min_lot_step=float(current_limits['filters'][1]['stepSize'])
            ))
            return True
        else:
            return False

    def check_trade_am_and_current_limits(self, trade_am, current_limits):
        if trade_am < float(current_limits['filters'][2]['minNotional']):
            self.logger.error("""
                                Final deal size {trade_am:0.8f} less than allowed for a pair {min_am:0.8f}. 
                                Increase the amount of trades (in {incr} times)""".format(
                trade_am=trade_am, min_am=float(current_limits['filters'][2]['minNotional']),
                incr=float(current_limits['filters'][2]['minNotional']) / trade_am
            ))
            return True
        else:
            return False

    def check_new_order(self, new_order, type_order):
        if 'orderId' in new_order:
            if type_order == 'BUY':
                self.logger.info("Buy order created {new_order}".format(new_order=new_order))
            else:
                self.logger.info("Sell order created {new_order}".format(new_order=new_order))
            return True
        else:
            self.logger.error("Failed to create order! {new_order}".format(new_order=str(new_order)))
            return False

    def check_cancel_order(self, cancel_order):
        if 'orderId' in cancel_order:
            self.logger.error("Order canceled {cancel_order}".format(new_order=cancel_order))
            return True
        else:
            self.logger.error("Failed to cancel order! {cancel_order}".format(new_order=str(cancel_order)))
            return False
