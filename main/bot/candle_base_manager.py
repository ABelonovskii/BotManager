from .Sql_query import SQLQuery
from .binance_query import Binance_query
import logging

class Candle_base_manager:

    def __init__(self):
        self.binance_query = Binance_query()
        self.sqlQuery = SQLQuery()
        self.logger = logging.getLogger(__name__)

    def update_pair_tables(self):

        number_of_candles = 1000  # config.xml

        for pair in self.sqlQuery.get_pairs_name():

            pair_live_name = str(pair[0]) + "_live"
            #pair_archive_name = str(pair[0]) + "_archive"

            # create tables
            self.sqlQuery.crate_table_for_pair(pair_live_name)
            #self.sqlQuery.crate_table_for_pair(pair_archive_name)

            # add records, if tables is empty
            if self.sqlQuery.number_of_records(pair_live_name) == 0:

                # download candles
                data = self.binance_query.download_pairs_records(pair[0], number_of_candles)

                # for live
                self.sqlQuery.add_records_for_pairs_table(data, pair_live_name, number_of_candles)

                # for archive
                #self.sqlQuery.add_records_for_pairs_table(data, pair_archive_name, number_of_candles)

            # check if the number of records in the table is less than 1000
            elif self.sqlQuery.number_of_records(pair_live_name) < 1000:

                # download candles
                data = self.binance_query.download_pairs_records(pair[0], number_of_candles)

                # delete all records in the database
                self.sqlQuery.delete_all_records(pair_live_name)

                # add new records
                self.sqlQuery.add_records_for_pairs_table(data, pair_live_name, number_of_candles)

            # if tables is not empty? then add new records
            else:

                server_time = int(self.binance_query.server_time())
                last_time = self.sqlQuery.last_time_candle(pair_live_name)

                n_candle_for_download = (server_time - last_time) // 300

                if n_candle_for_download < number_of_candles:
                    delete_up = n_candle_for_download
                else:
                    delete_up = number_of_candles

                if n_candle_for_download < 5:
                    delete_down = 5 - n_candle_for_download
                else:
                    delete_down = 0

                if n_candle_for_download > number_of_candles: n_candle_for_download = number_of_candles
                if ((n_candle_for_download > 0) and (n_candle_for_download < 5)): n_candle_for_download = 5

                if (n_candle_for_download > 0):
                    self.logger.info("Download {candle} candles for pare {name}".format(candle=n_candle_for_download, name=pair[0]))

                    # download candles
                    data = self.binance_query.download_pairs_records(pair[0], n_candle_for_download)

                    # delete last records
                    self.sqlQuery.delete_last_records(pair_live_name, delete_down)
                    #self.sqlQuery.delete_last_records(pair_archive_name, delete_down)

                    # delete first records
                    self.sqlQuery.delete_first_records(pair_live_name, delete_up)

                    # for live
                    self.sqlQuery.add_records_for_pairs_table(data, pair_live_name, n_candle_for_download)

                    # for archive
                    #self.sqlQuery.add_records_for_pairs_table(data, pair_archive_name, n_candle_for_download)

    def update_pair_table(self, pair_name):

        number_of_candles = 1000  # config.xml

        pair_live_name = str(pair_name) + "_live"
        self.sqlQuery.crate_table_for_pair(pair_live_name)

        # add records, if tables is empty
        if self.sqlQuery.number_of_records(pair_live_name) == 0:

            # download candles
            data = self.binance_query.download_pairs_records(pair_name, number_of_candles)

            # for live
            self.sqlQuery.add_records_for_pairs_table(data, pair_live_name, number_of_candles)

        # if tables is not empty? then add new records
        else:
            server_time = int(self.binance_query.server_time())
            last_time = self.sqlQuery.last_time_candle(pair_live_name)
            n_candle_for_download = (server_time - last_time) // 300

            if n_candle_for_download < number_of_candles:
                delete_up = n_candle_for_download
            else:
                delete_up = number_of_candles

            if n_candle_for_download < 5:
                delete_down = 5 - n_candle_for_download
            else:
                delete_down = 0

            if n_candle_for_download > number_of_candles: n_candle_for_download = number_of_candles
            if (n_candle_for_download > 0) and (n_candle_for_download < 5): n_candle_for_download = 5

            if n_candle_for_download > 0:
                self.logger.info(
                    "Download {candle} candles for pare {name}".format(candle=n_candle_for_download, name=pair_name))

                # download candles
                data = self.binance_query.download_pairs_records(pair_name, n_candle_for_download)

                # delete last records
                self.sqlQuery.delete_last_records(pair_live_name, delete_down)
                # self.sqlQuery.delete_last_records(pair_archive_name, delete_down)

                # delete first records
                self.sqlQuery.delete_first_records(pair_live_name, delete_up)

                # for live
                self.sqlQuery.add_records_for_pairs_table(data, pair_live_name, n_candle_for_download)

                # for archive
                # self.sqlQuery.add_records_for_pairs_table(data, pair_archive_name, n_candle_for_download)
