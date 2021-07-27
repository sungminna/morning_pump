import pandas as pd
import sqlite3


class Backtesting():

    def __init__(self):
        super().__init__()
        print("class Backtesting start")

        self.conn = sqlite3.connect("C:/Users/DEV/Desktop/coin_database/60min.db")
        self.cur = self.conn.cursor()
        self.existing_table = list()

        #####
        self.get_table_from_db()

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
        Y = item[0][:4]
        M = item[0][5:7]
        D = item[0][8:10]
        print(item[0])
        print(Y, M, D)

        query = "SELECT candle_date_time_kst, opening_price, high_price, low_price, trade_price FROM table_" + name + "WHERE candle_date_time_kst == '" + time + "';"

        self.cur.execute(query)
        item = self.cur.fetchall()
        #latest = item[-1]
        #self.latest_db = str(latest).strip('(,\)')

    def next_time(self, Y, M, D):
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

        return(Y, M, D)


if __name__ == "__main__":
    Backtesting()