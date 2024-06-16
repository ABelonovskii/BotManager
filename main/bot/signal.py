from .Bot import Bot
from .NeuralNetwork import NeuralNetwork
import numpy as np
from .Sql_query import SQLQuery
from sklearn.preprocessing import normalize
import subprocess
from datetime import datetime

class Signal:

    sqlQuery = SQLQuery()

    def __new__(cls):
        if not hasattr(cls, 'instance'):
            cls.instance = super(Signal, cls).__new__(cls)
        return cls.instance

    def __init__(self):
        self.bot = Bot()
        self.bot.set_params_for_NN()
        self.bot.read_frames_from_xml()

        self.NN = NeuralNetwork()
        self.NN.create_networks(self.bot)

        gen = self.sqlQuery.get_gen()
        weights = np.loadtxt(gen)
        self.NN.fill_weights(weights)

    def create_data_for_frame(self, data, base_frame, frame):

        def get_first_candle_for_frame(data, frame):

            for i, timestamp in enumerate(data[:, 0]):
                date_time = datetime.fromtimestamp(timestamp / 1000)
                current_hour = date_time.hour
                current_minute = date_time.minute
                current_minute_all = current_minute + current_hour * 60

                if current_minute_all % frame == 0:
                    return i
            return -1

        length_data = len(data)

        data_for_frame = np.zeros(
            (length_data // int(frame / base_frame) + 2, 6));

        ratio = int(frame / base_frame)

        first_candle = get_first_candle_for_frame(data, frame)

        j = 0
        if first_candle > 0:
            data_for_frame[j, 0] = data[0, 0]
            data_for_frame[j, 1] = data[0, 1]
            data_for_frame[j, 2] = np.max(data[0:first_candle, 2])
            data_for_frame[j, 3] = np.min(data[0:first_candle, 3])
            data_for_frame[j, 4] = data[first_candle - 1, 4]
            data_for_frame[j, 5] = np.sum(data[0:first_candle, 5])
            j += 1

        for i in range(first_candle + ratio - 1, len(data), int(frame / base_frame)):
            data_for_frame[j, 0] = data[i - (ratio - 1), 0]
            data_for_frame[j, 1] = data[i - (ratio - 1), 1]
            data_for_frame[j, 2] = np.max(data[(i - (ratio - 1)):(i + 1), 2])
            data_for_frame[j, 3] = np.min(data[(i - (ratio - 1)):(i + 1), 3])
            data_for_frame[j, 4] = data[i, 4]
            data_for_frame[j, 5] = np.sum(data[(i - (ratio - 1)):(i + 1), 5])
            j += 1

        if (i < len(data) - 1):
            data_for_frame[j, 0] = data[i + 1, 0]
            data_for_frame[j, 1] = data[i + 1, 1]
            data_for_frame[j, 2] = np.max(data[(i + 1):(len(data)), 2])
            data_for_frame[j, 3] = np.min(data[(i + 1):(len(data)), 3])
            data_for_frame[j, 4] = data[len(data) - 1, 4]
            data_for_frame[j, 5] = np.sum(data[(i + 1):(len(data)), 5])

        return data_for_frame, first_candle

    def fill_signals_dll(self, pair_name, parameters):

        data = self.sqlQuery.get_records(pair_name)
        data_2, fist_candles_2 = self.create_data_for_frame(data, self.bot.get_frames()[0], self.bot.get_frames()[1])
        data_3, fist_candles_3 = self.create_data_for_frame(data, self.bot.get_frames()[0], self.bot.get_frames()[2])
        data_15, fist_candles_15 = self.create_data_for_frame(data, self.bot.get_frames()[0], self.bot.get_frames()[3])

        np.savetxt('buffer/data.txt', data[:, :6])
        np.savetxt('buffer/data_2.txt', data_2)
        np.savetxt('buffer/data_3.txt', data_3)
        np.savetxt('buffer/data_15.txt', data_15)
        np.savetxt('buffer/parameters.txt', parameters)

        result = subprocess.run(["engine.exe",
                                 str(self.bot.get_frames()[0]),
                                 str(self.bot.get_frames()[1]),
                                 str(self.bot.get_frames()[2]),
                                 str(self.bot.get_frames()[3]),
                                 str(fist_candles_2),
                                 str(fist_candles_3),
                                 str(fist_candles_15),
                                 str(len(data))
                                 ], capture_output=True)

        with open('buffer/signals.bin', 'rb') as f:
            data = f.read()
            signals = np.frombuffer(data, dtype=np.float64).reshape(1, 13)

        return signals

    def calcDecision_buy(self, pair_name):
        init_data = self.sqlQuery.get_records_open(pair_name)
        parameters_buy = self.NN.predict_definition_buy(normalize(init_data.reshape(1, -1)))
        signals_buy = self.fill_signals_dll(pair_name, parameters_buy)
        decisions_buy = self.NN.predict_decisions_buy(signals_buy)
        return decisions_buy

    def calcDecision_sell(self, pair_name):
        init_data = self.sqlQuery.get_records_open(pair_name)
        parameters_sell = self.NN.predict_definition_sell(normalize(init_data.reshape(1, -1)))
        signals_sell = self.fill_signals_dll(pair_name, parameters_sell)
        decisions_sell = self.NN.predict_decisions_sell(signals_sell)
        return decisions_sell

    def signal_buy(self, pair_name):
        signal = False
        decisions_buy = self.calcDecision_buy(pair_name)
        if decisions_buy > 0.9:
            signal = True
        max_price = 1
        min_price = 1
        return signal, max_price, min_price

    def signal_sell(self, pair_name):
        signal = False
        decisions_sell = self.calcDecision_sell(pair_name)
        if decisions_sell > 0.9:
            signal = True
        max_price = 1
        min_price = 1
        return signal, max_price, min_price
