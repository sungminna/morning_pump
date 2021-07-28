import pandas as pd
import math
import sqlite3
import time


class Backtesting():

    def __init__(self):
        super().__init__()
        print("class Backtesting start")

        self.conn = sqlite3.connect("C:/Users/DEV/Desktop/coin_database/60min.db")
        self.cur = self.conn.cursor()
        self.existing_table = list()
        self.how_many_days = int()

        #####
        self.get_table_from_db()

        self.cur.execute("SELECT count(*) FROM table_BTC;")
        item = self.cur.fetchone()
        self.how_many_days = math.floor((int(item[0])/24))
        print(self.how_many_days)

        for name in self.existing_table:
            name = name[-3:]
            self.parse_data(name)


    def get_table_from_db(self):
        ##### db에 저장된 테이블 이름 가져오기
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table = list(self.cur.fetchall())
        for item in table:
            item = str(item).strip('(,\')')
            self.existing_table.append(item)
        print(self.existing_table)


    def parse_data(self, name):
        query = "SELECT candle_date_time_kst FROM table_" + name + ";"
        self.cur.execute(query)
        item = self.cur.fetchone()
        working_time = item[0][:10] + "T09:00:00"
        working_time = self.next_time(working_time)

        temp_list = list()

        for _ in range(self.how_many_days):
            query = "SELECT candle_date_time_kst, opening_price, high_price, low_price, trade_price FROM table_" + name + " WHERE candle_date_time_kst = '" + working_time + "';"
            self.cur.execute(query)
            item = self.cur.fetchone()
            if item == None:
                pass
            else:
                item = list(item)
                temp_list.append(item)


            working_time = self.next_time(working_time)


        df = pd.DataFrame(temp_list, columns=['candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price'])
        print(df)
        print("====")
        time.sleep(10)
            #latest = item[-1]
            #self.latest_db = str(latest).strip('(,\)')

    def next_time(self, time):
        Y = int(time[:4])
        M = int(time[5:7])
        D = int(time[8:10])
        print(Y, M, D)
        #다음 시간
        if(M in[1, 3, 5, 7, 8, 10]):
            if(D==31):
                M += 1
                D = 1
            else:
                D += 1
        elif(M==2):
            if(D==28):
                M = 3
                D = 1
            else:
                D += 1
        elif(M==12):
            if(D==31):
                Y += 1
                M = 1
                D = 1
            else:
                D += 1
        else:
            if(D==30):
                M += 1
                D = 1
            else:
                D += 1
        ## 0 추가
        if M<10:
            M = "0" + str(M)
        if D<10:
            D = "0" + str(D)

        rtn = str(Y) + "_" + str(M) + "_" + str(D) + "T09:00:00"
        return(rtn)


if __name__ == "__main__":
    Backtesting()