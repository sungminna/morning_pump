import pyupbit
import pandas as pd
import datetime
import time
import requests
import math

class Core():
    def __init__(self):
        print("Core() start")

        self.tickers = list()
        self.Y = int()
        self.M = int()
        self.D = int()
        self.h = int()
        self.m = int()
        self.intervals = ["minute1", "minute3", "minute5", "minute10", "minute15", "minute30", 'minute60']
        self.chart_data = pd.DataFrame()

        self.temp_list = []

        self.flow()
        print(self.cnt)

    def flow(self):
        #get available coins on the market
        self.tickers = pyupbit.get_tickers(fiat="KRW")


        #get chart data
        url = "https://api.upbit.com/v1/candles/minutes/60"
        for ticker in self.tickers:
            self.temp_list = []
            self.current_time()
            print(ticker)
            while(True):
                Y = str(self.Y)
                if(self.M < 10):
                    M = "0" + str(self.M)
                else:
                    M = str(self.M)
                if(self.D < 10):
                    D = "0" + str(self.D)
                else:
                    D = str(self.D)
                if (self.h < 10):
                    h = "0" + str(self.h)
                else:
                    h = str(self.h)
                if (self.m < 10):
                    m = "0" + str(self.m)
                else:
                    m = str(self.m)

                to = Y + "-" + M + "-" + D + " " + "01:00:00"
                querystring = {"market": ticker, "to": to, "count": "2"}
                headers = {"Accept": "application/json"}
                response = requests.request("GET", url, headers=headers, params=querystring)
                time.sleep(0.09)
                res_json = response.json()

                if not res_json:
                    break
                else:
                    self.temp_list.append([res_json[0], res_json[1]])

                self.D -= 1
                if (self.D == 0):
                    self.M -= 1
                    if(self.M == 0):
                        self.Y -=1
                        self.M = 12
                        self.D = 31
                    else:
                        if(self.M in[1, 3, 5, 7, 8, 10, 12]):
                            self.D = 31
                        else:
                            self.D = 30
                        if(self.M == 2):
                            self.D = 28

            cntu = 0
            cntd = 0
            total_count = 0
            prev_down_cnt = 0
            sumu = 0
            sumd = 0

            totup = 0
            for item in self.temp_list:
                total_count +=1

                if(item[1]['opening_price'] > item[1]['trade_price']):
                    prev_down_cnt +=1
                    if(item[0]['opening_price'] < item[0]['trade_price']):
                        sumu += ((item[0]['trade_price'] - item[0]['opening_price']) / item[0]['opening_price'])
                        cntu += 1

                        totup +=1

                    else:
                        sumd += ((item[0]['opening_price'] - item[0]['trade_price']) / item[0]['opening_price'])
                        cntd +=1

                else:
                    if (item[0]['opening_price'] < item[0]['trade_price']):

                        totup += 1
                ######


            print("전제 일수:", total_count, "// 8시 음봉:", prev_down_cnt, "// buy:", cntu, "// 9시양봉수:", totup)
            print(100 * sumu / cntu, 100 * sumd / cntd) #percent
            print("100만 투자시 1달 후:", 100 * math.pow((1+sumu/prev_down_cnt) * (1-sumd/prev_down_cnt), 15))
            print("==========================")



    def current_time(self):
        dt_now = str(datetime.datetime.now())
        print(dt_now)
        self.Y = int(dt_now[0:4])
        self.M = int(dt_now[5:7])
        self.D = int(dt_now[8:10])