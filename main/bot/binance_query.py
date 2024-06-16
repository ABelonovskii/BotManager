from .binance_api import Binance
from .Sql_query import SQLQuery
import logging


class Binance_query:
    sqlQuery = SQLQuery()
    # bot = Binance(API_KEY=sqlQuery.get_keys()[0], API_SECRET=sqlQuery.get_keys()[1])
    bot = Binance()
    logger = logging.getLogger(__name__)

    def __init__(self):
        try:
            self.limits = self.bot.exchangeInfo()
        except:
            self.logger.error("Unable to connect to the binance")

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Binance_query, cls).__new__(cls)
        return cls.instance

    def limits_update(self):
        try:
            self.limits = self.bot.exchangeInfo()
        except:
            self.logger.error("Unable to connect to the binance")

    def download_pairs_records(self, pair_name, number_of_candles):
        try:
            return self.bot.klines(
                symbol=pair_name,
                interval='5m',
                limit=number_of_candles,
            )
        except:
            self.logger.error("Unable to connect to the binance")
            return 0

    def server_time(self):
        try:
            self.limits_update()
            return int(self.limits['serverTime']) // 1000
        except:
            self.logger.error("Unable to connect to the binance")
            return 0

    def set_shift_seconds(self, shift_seconds):
        self.bot.set_shift_seconds(shift_seconds)

    def get_current_limits(self, pair_name):
        curr_limits = 0
        try:
            for elem in self.limits['symbols']:
                if elem['symbol'] == pair_name:
                    curr_limits = elem
                    break
            else:
                self.logger.error("Could not find the settings for the selected pair " + pair_name)
        except:
            self.logger.error("Unable to connect to the binance")

        return curr_limits

    def get_balances(self, pair_obj):
        # this is a private method
        self.bot.apply_keys(API_KEY=self.sqlQuery.get_keys()[0], API_SECRET=self.sqlQuery.get_keys()[1])
        balances = -1
        try:
            balances = {
                balance['asset']: float(balance['free']) for balance in self.bot.account()['balances']
                if balance['asset'] in [pair_obj['base'], pair_obj['quote']]
            }
        except:
            self.logger.error("Unable to connect to the binance of incorrect keys")
        return balances

    def get_current_price(self, pair_name):
        try:
            return float(self.bot.tickerPrice(symbol=pair_name)['price'])
        except:
            self.logger.error("Unable to connect to the binance")
            return -1

    def create_order(self, pair_name, amount, current_limits, type_order):
        new_order = -1
        try:
            # this is a private method
            self.bot.apply_keys(API_KEY=self.sqlQuery.get_keys()[0], API_SECRET=self.sqlQuery.get_keys()[1])

            new_order = self.bot.createOrder(
                symbol=pair_name,
                recvWindow=5000,
                side=type_order,
                type='MARKET',
                quantity="{quantity:0.{precision}f}".format(
                    quantity=amount, precision=current_limits['baseAssetPrecision']
                ),
                newOrderRespType='FULL'
            )
            return new_order
        except:
            self.logger.error("Unable to connect to the binance")
            return -1

    def cancel_order(self, order, pair_name):
        try:
            # this is a private method
            self.bot.apply_keys(API_KEY=self.sqlQuery.get_keys()[0], API_SECRET=self.sqlQuery.get_keys()[1])

            return self.bot.cancelOrder(symbol=pair_name, orderId=order)
        except:
            self.logger.error("Unable to connect to the binance")
            return -1

    def orderInfo(self, order, pair_name):
        try:
            # this is a private method
            self.bot.apply_keys(API_KEY=self.sqlQuery.get_keys()[0], API_SECRET=self.sqlQuery.get_keys()[1])

            return self.bot.orderInfo(symbol=pair_name, orderId=order)
        except:
            self.logger.error("Unable to connect to the binance")
            return -1