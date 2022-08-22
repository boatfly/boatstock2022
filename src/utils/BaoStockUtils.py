# -*- coding: utf-8 -*-
"""
@Author: boatdaddy
@Date: 2022/8/6 14:14
@File: BaoStockUtils.py
@Description: 
"""
from datetime import timedelta

import baostock as bs
import numpy as np
import pandas as pd
from dateutil import parser


def fbi_stock_backtesting():
    pass


def gen_stock_ticks():
    bar_5m_df = pd.read_csv("/Users/song/PycharmProjects/boatStock2022/data/test/600036-5m.csv")
    print(bar_5m_df.head())
    print(bar_5m_df.tail())
    # 利用历史数据来模拟股价波动
    # 读取历史的bar的low high close来模拟价格的波动（生成ticks）  bar --> ticks --> bar_generator()
    # ticks = [(datetime, 36.89), (datetime, 36.90).....,(,)]
    # history_A_stock_k_data.csv --> history_A_stock_k_data_ticks.csv

    ticks = []
    for index, row in bar_5m_df.iterrows():
        if row['open'] < 30:
            step = 0.01
        elif row['open'] < 60:
            step = 0.03
        elif row['open'] < 90:
            step = 0.05
        else:
            step = 0.1
        arr = np.arange(row['open'], row['high'], step)  # 在bar历史数据中生成时间序列数据
        arr = np.append(arr, row['high'])
        arr = np.append(arr, np.arange(row['open'] - 0.01, row['low'], -step))
        arr = np.append(arr, row['low'])
        arr = np.append(arr, row['close'])

        i = 0
        for item in arr:
            # 开始解析数据的时间
            row_time_str = row['datetime']
            row_date_time = parser.parse(row_time_str)
            # string_time = str(row_time)
            # year = row_time // int(1e13)
            # month = row_time // int(1e11) % 100
            # day = row_time // int(1e9) % 100
            # hour = row_time // int(1e7) % 100
            # min = row_time // int(1e5) % 100
            # second = row_time // int(1e3) % 100
            # dt = datetime(year, month, day, hour, min, second) - timedelta(minutes=5)
            dt = row_date_time - timedelta(minutes=5)
            # print(dt)
            ticks.append(((dt + timedelta(seconds=0.1) * i), item))
            i += 1
    ticks = np.array(ticks)
    ticks_df = pd.DataFrame(ticks, columns=['datetime', 'price'])
    ticks_df.to_csv("/Users/song/PycharmProjects/boatStock2022/data/test/600036_ticks.csv",
                    index=0)


def load_stock_industry():
    # 登陆系统
    lg = bs.login()
    # 显示登陆返回信息
    print('login respond error_code:' + lg.error_code)
    print('login respond  error_msg:' + lg.error_msg)

    # 获取行业分类数据
    rs = bs.query_stock_industry()
    # rs = bs.query_stock_basic(code_name="浦发银行")
    print('query_stock_industry error_code:' + rs.error_code)
    print('query_stock_industry respond  error_msg:' + rs.error_msg)

    # 打印结果集
    industry_list = []
    while (rs.error_code == '0') & rs.next():
        # 获取一条记录，将记录合并在一起
        industry_list.append(rs.get_row_data())
    result = pd.DataFrame(industry_list, columns=rs.fields)
    # 结果集输出到csv文件
    result.to_csv("/Users/song/PycharmProjects/boatStock2022/data/industry/stock_industry.csv", encoding="gbk",
                  index=False)
    print(result)

    # 登出系统
    bs.logout()


def query_stock_industry():
    df = pd.read_csv("/Users/song/PycharmProjects/boatStock2022/data/industry/stock_industry.csv", encoding="gbk")
    print(df.head())
    df['industry'] = np.where(pd.isna(df['industry']), '无', df['industry'])
    # df.set_index('code', drop=True, inplace=True)
    print(df.head())

    dfi = df.groupby("industry").agg({"code": "nunique"}).reset_index().rename(
        columns={"code": "nums"})
    print(dfi)

    dfi_sorted = dfi.sort_values(by='nums', ascending=False)
    print(dfi_sorted)

    # stocks = df.loc[:, ['code']]
    # print(stocks.head())
    # print(stocks.tail())


def load_data_from_baostock(stock, start_date, end_date, freq='d', adjustflag='2'):
    if str(stock)[0] == '6':
        stock = "sh." + stock
    else:
        stock = "sz." + stock

    if freq in ['m', 'd', 'w']:
        fields = "date,code,open,high,low,close,volume,amount,adjustflag,turn,pctChg"
    else:
        fields = "date,time,code,open,high,low,close,volume,amount,adjustflag"

    lg = bs.login()
    rs = bs.query_history_k_data_plus(stock,
                                      fields,
                                      start_date=start_date, end_date=end_date,
                                      frequency=str(freq), adjustflag=str(adjustflag))
    print('query_history_k_data_plus respond error_code:' + rs.error_code)
    print('query_history_k_data_plus respond  error_msg:' + rs.error_msg)
    bs.logout()

    df = pd.DataFrame(rs.data)
    df.columns = fields.split(",")

    return df


def save_baostock_data_to_local(stock, start_date, end_date, freq='d'):
    df = load_data_from_baostock(stock, start_date, end_date, freq)
    if freq == 'd':
        df = df.loc[:, ['date', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        df.rename(columns={'time': 'date'}, inplace=True)
        df.set_index('date', drop=True, inplace=True)
        print(df)
        df.to_csv("/Users/song/PycharmProjects/boatStock2022/data/test/%s-%s.csv" % (stock, freq))
    else:
        df['time'] = [t[:-3] for t in df['time']]  # 列表表达式
        df['time'] = pd.to_datetime(df['time'])
        df = df.loc[:, ['time', 'open', 'high', 'low', 'close', 'volume', 'amount']]
        df.rename(columns={'time': 'datetime'}, inplace=True)
        df.set_index('datetime', drop=True, inplace=True)
        print(df)
        df.to_csv("/Users/song/PycharmProjects/boatStock2022/data/test/%s-%sm.csv" % (stock, freq))


def gen_data_to_ma(stock, start_date, end_date, freq='d', ma=5):
    df = load_data_from_baostock(stock, start_date, end_date, freq)
    df = df.loc[:, ['date', 'close']]
    df.rename(columns={'time': 'date'}, inplace=True)
    df.set_index('date', drop=True, inplace=True)
    print(df)
    ma_list = [5, 10, 20]
    for ma in ma_list:
        df["ma_" + str(ma)] = df['close'].rolling(ma).mean()
    print(df)


def gen_interval_data_from_5m():
    pd.interval_range()
    pass


if __name__ == "__main__":
    # save_baostock_data_to_local("600036", '2022-01-01', '2022-08-17', '5')
    # gen_data_to_ma("600036", '2022-07-01', '2022-07-31', 'd', 5)
    # load_stock_industry()
    # query_stock_industry()

    gen_stock_ticks()
