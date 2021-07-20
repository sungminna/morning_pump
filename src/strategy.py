import pyupbit
import datetime
import pandas as pd

class Strategy():
    def __init__(self):
        print("Strategy() start")

        self.find_target()



    def find_target(self):
        pass



    def get_data(self, ticker, interval, count, to):
        # KRW-BTC, (interval), int, 20201010 (2020-10-10)
        # interval => day, minute1, 3, 5, 10, 15, 30, 60, 240, week, month
        # latest data comes last
        df = pyupbit.get_ohlcv(ticker, interval, count, to)
