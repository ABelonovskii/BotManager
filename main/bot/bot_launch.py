from threading import Thread
from .engine import Engine
from .Sql_query import SQLQuery


class Bot_launch:
    sqlQuery = SQLQuery()

    def start_bot(self):
        self.sqlQuery.set_status_engine(1)
        name = "Bot #%s" % (1)
        my_thread = MyThread(name)
        my_thread.start()

    def stop_bot(self):
        self.sqlQuery.set_status_engine(0)


class MyThread(Thread):

    def __init__(self, name):
        Thread.__init__(self)
        self.name = name

    def run(self):
        engine = Engine()
        engine.start_engine()
