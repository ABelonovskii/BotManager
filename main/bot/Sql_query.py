import sqlite3
import time
import numpy as np
from cryptography.fernet import Fernet
import threading
import logging

class SQLQuery:
    _db = 'db.sqlite3'
    _db_binance = 'binance.sqlite3'
    logger = logging.getLogger(__name__)

    def __init__(self):
        self._db_lock = threading.Lock()
        self._binance_lock = threading.Lock()
        self.num_retries = 10

    def create_status_engine_table(self):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("""create table if not exists 
                           On_start 
                                (
                                  id INT PRIMARY KEY,
                                  on_start INT,
                                  status_text TEXT,
                                  progress_status INT
                                );
                                """)
            conn.commit()
            cursor.execute("""
                            INSERT INTO On_start VALUES (1, 0, "", 0)  ON CONFLICT (id) 
                            DO UPDATE SET on_start = 0, status_text="", progress_status=0  WHERE id = 1;
                            """)
            conn.commit()

    def get_status_engine(self):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            on_start = cursor.execute("SELECT on_start FROM On_start WHERE id=1;").fetchone()[0]
            return on_start

    def set_status_engine(self, status):
        with self._binance_lock:
            for i in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute("UPDATE On_start SET on_start = :status WHERE id = 1", {'status': status})
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if i == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def get_progress_status_engine(self):
        with self._binance_lock:
            with sqlite3.connect(self._db_binance) as conn:
                cursor = conn.cursor()
                status_text = cursor.execute("SELECT status_text FROM On_start WHERE id=1;").fetchone()[0]
                progress_status = cursor.execute("SELECT progress_status FROM On_start WHERE id=1;").fetchone()[0]

                return status_text, progress_status

    def set_progress_status_engine(self, status_text, progress_status):
        with self._binance_lock:
            for i in range(self.num_retries):
                try:
                    with sqlite3.connect(self._db_binance) as conn:
                        cursor = conn.cursor()
                        cursor.execute(
                            "UPDATE On_start SET status_text = :status_text, progress_status = :progress_status  WHERE id = 1",
                            {'status_text': status_text, 'progress_status': progress_status})
                        conn.commit()
                        break
                except sqlite3.Error as e:
                    if i == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass

    def get_keys(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            keys = cursor.execute("SELECT API_KEY, API_SECRET FROM main_keys ORDER BY id DESC LIMIT 1;").fetchone()

        # загрузка ключа из файла
        with open('buffer/k.k', 'rb') as key_file:
            key = key_file.read()

        fernet = Fernet(key)
        api_key = fernet.decrypt(keys[0].encode()).decode()
        api_secret = fernet.decrypt(keys[1].encode()).decode()

        return [api_key, api_secret]

    def get_bot(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            Bot_Path = cursor.execute("SELECT Bot_Path_Body FROM main_botpath ORDER BY id DESC LIMIT 1;").fetchone()
            bot_body = str(Bot_Path[0])
            if not bot_body.endswith('.bot'):
                bot_body += '.bot'

            bot_body = "BotStorage/" + bot_body

            return bot_body

    def get_gen(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            Bot_Path = cursor.execute("SELECT Bot_Path_Gen FROM main_botpath ORDER BY id DESC LIMIT 1;").fetchone()

            bot_gen = str(Bot_Path[0])
            if not bot_gen.endswith('.gen'):
                bot_gen += '.gen'

            bot_gen = "BotStorage/" + bot_gen

            return bot_gen

    def get_pairs_name(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            pairs_name = cursor.execute("SELECT Pair_Name FROM main_pairs;").fetchall()
            return pairs_name

    def get_pairs(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            pairs = cursor.execute("SELECT Pair_Name, Pair_quote, Pair_base, Spend_amount FROM main_pairs;").fetchall()
            return pairs

    def get_pairs_status(self):
        with self._db_lock:
            conn = sqlite3.connect(self._db, check_same_thread=False)
            cursor = conn.cursor()
            pairs_status = cursor.execute("SELECT Pair_Name, active FROM main_pairs;").fetchall()
            return pairs_status

    """
    Раздел для таблиц свечей
    """

    def crate_table_for_pair(self, pair_name):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("""create table if not exists """ +
                           pair_name +
                           """
                                (
                                  open_time BIGINT,
                                  open REAL,
                                  high REAL,
                                  low REAL,
                                  close REAL,
                                  volume REAL,
                                  close_time BIGINT
                                );
                                """)
            conn.commit()

    def number_of_records(self, pair_name):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            number_of_records = cursor.execute("SELECT COUNT(*) FROM " + pair_name).fetchone()[0]
            return number_of_records

    def add_records_for_pairs_table(self, data, pair_name, number_of_records):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    query = "INSERT INTO " + pair_name + " VALUES "
                    for i in range(number_of_records):
                        query += "("
                        for j in range(6):
                            query += str(data[i][j]) + ", "
                        if i < (number_of_records - 1):
                            query += str(data[i][6]) + "), "
                        else:
                            query += str(data[i][6]) + ") "

                    cursor.execute(query)
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def last_time_candle(self, pair_name):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            last_time_candle = \
                cursor.execute("SELECT open_time FROM " + pair_name + " ORDER BY open_time DESC LIMIT 1").fetchone()[
                    0] // 1000
            return last_time_candle

    def delete_last_records(self, pair_name, count):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM " + pair_name + " WHERE open_time IN " +
                                   "(SELECT open_time FROM " + pair_name + " ORDER BY open_time DESC LIMIT " + str(
                        count) + ")")
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def delete_first_records(self, pair_name, count):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute("DELETE FROM " + pair_name + " WHERE open_time IN " +
                                   "(SELECT open_time FROM " + pair_name + " ORDER BY open_time ASC LIMIT " + str(
                        count) + ")")
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def get_records_open(self, pair_name):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            data_open_lst = cursor.execute("SELECT open FROM " + pair_name + "_live;").fetchall()
            data_open = np.array([x[0] for x in data_open_lst])[-500:]
            return data_open

    def get_records(self, pair_name):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            data_lst = cursor.execute("SELECT * FROM " + pair_name + "_live;").fetchall()
            data = np.array([x[:] for x in data_lst])
            conn.commit()

            return data

    """
    Раздел для таблиц ордеров
    """

    def crate_table_for_orders(self):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            cursor.execute("""create table if not exists 
                                orders
                                (
                                  order_type TEXT,
                                  order_pair TEXT,
    
                                  buy_order_id NUMERIC,
                                  buy_amount REAL,
                                  buy_price REAL,
                                  buy_created DATETIME,
                                  buy_finished DATETIME NULL,
                                  buy_cancelled DATETIME NULL,       
                                  max_price_point REAL,
                                  min_price_point REAL,
                    
                                  sell_order_id NUMERIC NULL,
                                  sell_amount REAL NULL,
                                  sell_price REAL NULL,
                                  sell_created DATETIME NULL,
                                  sell_finished DATETIME NULL,
                                  force_sell INT DEFAULT 0
                                );
                                """)
            conn.commit()

    def get_all_orders(self):
        self.crate_table_for_orders()
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            orders_q = """
                                SELECT
                                    order_type,
                                    order_pair,
                                    buy_order_id,
                                    buy_amount,
                                    buy_price,
                                    buy_created,
                                    buy_finished,
                                    buy_cancelled,
                                    sell_order_id,
                                    sell_amount,
                                    sell_price,
                                    sell_created,
                                    sell_finished                
                                FROM
                                  orders
                                """

            orders_info = {}
            id = 0
            for row in cursor.execute(orders_q):
                orders_info[id] = {'order_type': row[0], 'order_pair': row[1], 'buy_order_id': row[2],
                                   'buy_amount': row[3], 'buy_price': row[4],
                                   'buy_created': row[5],
                                   'buy_finished': row[6],
                                   'buy_cancelled': row[7],
                                   'sell_order_id': row[8],
                                   'sell_amount': row[9],
                                   'sell_price': row[10],
                                   'sell_created': row[11],
                                   'sell_finished': row[12]}
                id += 1
            """    
            for row in cursor.execute(orders_q):
                orders_info[id] = {'order_type': row[0], 'order_pair': row[1], 'buy_order_id': row[2],
                                   'buy_amount': row[3], 'buy_price': row[4],
                                   'buy_created': None if row[5] is None else
                                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(row[5]) // 1000)),
                                   'buy_finished': None if row[6] is None else
                                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(int(row[6]) // 1000)),
                                   'buy_cancelled': None if row[7] is None else
                                   time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(row[7] // 1000)),
                                   'sell_order_id': row[8],
                                   'sell_amount': row[9],
                                   'sell_price': row[10],
                                   'sell_created': None if row[11] is None else
                                   time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(row[11] // 1000)),
                                   'sell_finished': None if row[12] is None else
                                   time.strftime("%Y-%m-%d %H:%M:%S",time.localtime(row[12] // 1000))
                                   }
                """

            return orders_info

    def get_all_orders_modif(self):
        self.crate_table_for_orders()
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            orders_q = """
                                SELECT
                                    order_type,
                                    order_pair,
                                    buy_order_id,
                                    buy_amount,
                                    buy_price,
                                    buy_created,
                                    buy_finished,
                                    buy_cancelled,
                                    sell_order_id,
                                    sell_amount,
                                    sell_price,
                                    sell_created,
                                    sell_finished                
                                FROM
                                  orders
                                """

            orders_info = {}
            id = 0
            for row in cursor.execute(orders_q):
                order_type = row[0]
                order_pair = row[1]
                buy_created = row[5]
                buy_total = np.round(row[3] * row[4], 10) if row[3] is not None and row[4] is not None else None
                buy_price = np.round(row[4], 10) if row[4] is not None else None
                sell_finished = row[12]
                sell_total = np.round(row[9] * row[10], 10) if row[9] is not None and row[10] is not None else None
                sell_price = np.round(row[10], 10) if row[10] is not None else None
                profit = np.round(sell_total - buy_total, 10) if buy_total is not None and sell_total is not None else None

                orders_info[id] = {'order_type': order_type,
                                   'order_pair': order_pair,
                                   'buy_total': buy_total,
                                   'buy_price': buy_price,
                                   'buy_created': buy_created,
                                   'sell_total': sell_total,
                                   'sell_price': sell_price,
                                   'sell_finished': sell_finished,
                                   'profit': profit}
                id += 1

            return orders_info


    def get_unexecuted_orders(self):
        self.crate_table_for_orders()
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            orders_q = """
                        SELECT
                          CASE WHEN order_type='buy' THEN buy_order_id ELSE sell_order_id END order_id
                          , order_type
                          , order_pair
                          , sell_amount
                          , sell_price
                          ,  strftime('%s',buy_created)
                          , buy_amount
                          , buy_price
                          , max_price_point
                          , min_price_point
                        FROM
                          orders
                        WHERE
                          buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_cancelled IS NULL ELSE sell_finished IS NULL END
                        """

            orders_info = {}

            for row in cursor.execute(orders_q):
                orders_info[str(row[0])] = {'order_type': row[1], 'order_pair': row[2], 'sell_amount': row[3],
                                            'sell_price': row[4],
                                            'buy_created': row[5], 'buy_amount': row[6], 'buy_price': row[7],
                                            'max_price_point': row[8], 'min_price_point': row[9]}

            return orders_info

    def get_free_pairs(self):
        all_pairs = {pair[0]: {'base': pair[2], 'quote': pair[1], 'spend_sum': pair[3]} for pair in self.get_pairs()}
        pairs_status = {pair_status[0]: {'active': pair_status[1]} for pair_status in self.get_pairs_status()}

        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            orders_q = """
                        SELECT
                          distinct(order_pair) pair
                        FROM
                          orders
                        WHERE
                          buy_cancelled IS NULL AND CASE WHEN order_type='buy' THEN buy_cancelled IS NULL ELSE sell_finished IS NULL END
                    """

            for bisy_pair in cursor.execute(orders_q):
                if bisy_pair[0] in all_pairs:
                    del all_pairs[bisy_pair[0]]

            # Удаление пар с active равным False
            for pair, status in pairs_status.items():
                if not status['active'] and pair in all_pairs:
                    del all_pairs[pair]

            return all_pairs

    def set_new_buy_order(self, pair_name, new_order, my_amount, price, max_price, min_price):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                          INSERT INTO orders(
                              order_type,
                              order_pair,
                              buy_order_id,
                              buy_amount,
                              buy_price,
                              buy_created,
                              max_price_point,
                              min_price_point

                          ) Values (
                            'buy',
                            :order_pair,
                            :order_id,
                            :buy_order_amount,
                            :buy_initial_price,
                            datetime(),
                            :max_price_point,
                            :min_price_point
                          )
                        """, {
                            'order_pair': pair_name,
                            'order_id': new_order['orderId'],
                            'buy_order_amount': my_amount,
                            'buy_initial_price': price,
                            'max_price_point': max_price,
                            'min_price_point': min_price
                        }
                    )
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def set_new_sell_order(self, order, new_order, sell_amount, price):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                          UPDATE orders
                          SET
                            order_type = 'sell',
                            sell_order_id = :sell_order_id,
                            sell_created = datetime(),
                            sell_amount = :sell_amount,
                            sell_price = :sell_initial_price
                          WHERE
                            buy_order_id = :buy_order_id

                        """, {
                            'buy_order_id': order,
                            'sell_order_id': new_order['orderId'],
                            'sell_amount': sell_amount,
                            'sell_initial_price': price
                        }
                    )
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def set_cancel_order(self, order):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                          UPDATE orders
                          SET
                            buy_cancelled = datetime()
                          WHERE
                            buy_order_id = :buy_order_id
                        """, {
                            'buy_order_id': order
                        }
                    )
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def set_finished_order(self, order):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                          UPDATE orders
                          SET
                            sell_finished = datetime()
                          WHERE
                            sell_order_id = :sell_order_id

                        """, {
                            'sell_order_id': order
                        }
                    )
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def set_finished_buy_order(self, order):
        with self._binance_lock:
            for con in range(self.num_retries):
                try:
                    conn = sqlite3.connect(self._db_binance, check_same_thread=False)
                    cursor = conn.cursor()
                    cursor.execute(
                        """
                          UPDATE orders
                          SET
                            buy_finished = datetime()
                          WHERE
                            buy_order_id = :buy_order_id

                        """, {
                            'buy_order_id': order
                        }
                    )
                    conn.commit()
                    break  # если операция успешно выполнена, выход из цикла
                except sqlite3.Error as e:
                    if con == self.num_retries - 1:
                        self.logger.error(f"Error writing to database: {e}")
                    else:
                        pass  # продолжаем цикл

    def get_finished_buy_status(self, order):
        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()
            # Выполняем запрос для получения времени завершения покупки
            cursor.execute("SELECT buy_finished FROM orders WHERE buy_order_id = ?", (order,))
            row = cursor.fetchone()
            if row:
                buy_finished = row[0]
            else:
                buy_finished = None

            conn.close()

            return buy_finished


    def get_profit_table(self):

        pairs = self.get_pairs_name()

        # Создаем пустую таблицу для хранения результатов
        profit_table = []

        with self._binance_lock:
            conn = sqlite3.connect(self._db_binance, check_same_thread=False)
            cursor = conn.cursor()

            for pair_name in pairs:

                # Получаем запись с последней покупкой/продажей для данной пары
                query = f"SELECT * FROM orders WHERE order_pair = '{pair_name[0]}' ORDER BY buy_created DESC LIMIT 1"
                cursor.execute(query)
                order = cursor.fetchone()

                try:
                    # Получаем цену закрытия для данной пары из таблицы (Pair_Name + "_live")
                    query = f"SELECT close FROM {pair_name[0]}_live ORDER BY open_time DESC LIMIT 1"
                    cursor.execute(query)
                    close_price = cursor.fetchone()[0]
                except sqlite3.Error as e:
                    close_price = 0  # устанавливаем значение по умолчанию

                if order is None:
                    buy_created = 0
                    buy_total = 0
                    buy_price = 0
                    sell_finished = 0
                    sell_total = 0
                    sell_price = 0
                    profit = 0
                else:
                    buy_created = order[5]
                    buy_total = np.round(order[3] * order[4], 10) if order[3] is not None and order[4] is not None else None
                    buy_price = np.round(order[4], 10) if order[4] is not None else None
                    sell_finished = order[14]
                    sell_total = np.round(order[11] * order[12], 10) if order[11] is not None and order[12] is not None else None
                    sell_price = np.round(order[12], 10) if order[12] is not None else None
                    profit = np.round(sell_total - buy_total, 10) if buy_total is not None and sell_total is not None else None

                profit_table.append({
                    'Pair_Name': pair_name[0],
                    'buy_created': buy_created,
                    'buy_total': buy_total,
                    'buy_price': buy_price,
                    'sell_finished': sell_finished,
                    'sell_total': sell_total,
                    'sell_price': sell_price,
                    'Profit': profit,
                    'close': np.round(close_price, 10)
                })

        return profit_table

