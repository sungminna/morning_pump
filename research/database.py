import requests
import pandas as pd
import time
import sqlite3

# 총 데이터 약 2.7 GB

class Database():

    def __init__(self):
        super().__init__()
        print("class Database start")

        self.url_name = 'https://api.upbit.com/v1/market/all'
        self.url_data = 'https://api.upbit.com/v1/candles/minutes/60'
        self.coin_list = list()
        self.coin_list_krw = list()
        self.exists = 0
        self.time = str()
        self.ext = 0
        self.total_list = list()
        self.existing_table = list()
        self.latest_db = str()

        self.df = pd.DataFrame(columns=['market', 'candle_date_time_utc', 'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'timestamp', 'candle_acc_trade_price', 'candle_acc_trade_volume', 'unit'])

        #####db 연결
        self.conn = sqlite3.connect("C:/Users/DEV/Desktop/coin_database/60min.db")
        self.cur = self.conn.cursor()

        #####업비트에서 종목 리스트 가져오기
        querystring = {"isDetails":"false"}
        response = requests.request("GET", self.url_name, params=querystring)
        res_json = response.json()
        for item in res_json:
            self.coin_list.append(item['market'])

        for item in self.coin_list:
            if(item[0:3] == 'KRW'):
                self.coin_list_krw.append(item)

        for i, item in enumerate(self.coin_list_krw):
            self.coin_list_krw[i]=self.coin_list_krw[i][4:]

        print(self.coin_list_krw)
        ##### db에 저장된 테이블 이름 가져오기
        self.cur.execute("SELECT name FROM sqlite_master WHERE type='table';")
        table = list(self.cur.fetchall())
        for item in table:
            item = str(item).strip('(,\')')
            self.existing_table.append(item)
        print(self.existing_table)

        ##### 테이블 존재 하는지 확인 하고 데이터 받기
        for item in self.coin_list_krw:
            self.ext = 0
            print(item)
            if("table_" + item in self.existing_table):
                self.exists = 1
                self.get_latest_time_db(item)
            else:
                self.exists = 0

            self.get_latest_time_api(item)

            while self.ext != 1:
                a = time.time()     #####api timing 맞추기
                self.get_200_min(item)
                b = time.time()
                if (b-a) < 0.1:
                    time.sleep(0.1-b+a)

            self.to_df()
            self.save_data(item)

        self.conn.commit()
        self.conn.close()


    def get_200_min(self, name):  #####200개 데이터 한번에 받기
        querystring = {"market": "KRW-" + name, "to": self.time, "count": "200"}
        response = requests.request("GET", self.url_data, params=querystring)
        res_json = response.json()

        if not res_json: #res가 바었거나 비었으며 탈출
            self.ext = 1
        else:
            self.time = res_json[-1]['candle_date_time_utc'].replace('T', ' ')
            for item in res_json:
                item['candle_date_time_utc'] = item['candle_date_time_utc'].replace('-', '_')
                item['candle_date_time_kst'] = item['candle_date_time_kst'].replace('-', '_')
                self.total_list.append(item.values())

                if(self.exists == 1):
                    if(item['candle_date_time_utc']==self.latest_db.strip('\'')):
                        self.ext = 1
                        break

        #self.ext = 1 #######시험용

    def get_latest_time_db(self, name):
        query = "SELECT candle_date_time_utc FROM table_" + name + ";"
        self.cur.execute(query)
        item = self.cur.fetchall()
        latest = item[-1]
        self.latest_db = str(latest).strip('(,\)')

    def get_latest_time_api(self, name):
        querystring = {"market": "KRW-" + name, "count": "1"}
        response = requests.request("GET", self.url_data, params=querystring)
        res_json = response.json()
        self.time = res_json[0]['candle_date_time_utc']
        self.time = self.time.replace('T', ' ')



    def to_df(self):
        self.df = pd.DataFrame(self.total_list,  columns=['market', 'candle_date_time_utc', 'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'timestamp', 'candle_acc_trade_price', 'candle_acc_trade_volume', 'unit'])
        self.df = self.df[::-1].reset_index(drop=True) #df 반전
        self.total_list.clear()     #초기화



    def save_data(self, name):    #sql 저장
        if(self.exists == 1):
            delete_query = "DELETE FROM table_" + name + " WHERE candle_date_time_utc = " + self.latest_db + ";"
            self.cur.execute(delete_query)
            self.df.to_sql("table_" + name, self.conn, index=False, if_exists='append')
        else:
            self.df.to_sql("table_" + name, self.conn, index=False, if_exists='replace')

        print(self.df)
        self.df = pd.DataFrame(columns=['market', 'candle_date_time_utc', 'candle_date_time_kst', 'opening_price', 'high_price', 'low_price', 'trade_price', 'timestamp', 'candle_acc_trade_price', 'candle_acc_trade_volume', 'unit']) #초기화

if __name__ == "__main__":
    Database()