#! /home/hui/anaconda2/bin/python
#coding:utf-8

import data_process
import os
import MySQLdb
import pandas as pd
import numpy as np


class automation(object):

    def __init__(self, dir = '/home/charles/python/intern/data/'):
        self.dir = dir
        self.calculated = []
        self.uncalculated = []
        self.conn = MySQLdb.connect(host = 'localhost', user='root',passwd='hehui477')
        self.cursor = self.conn.cursor()
        self.cursor.execute("""create database if not exists db_corr""")
        self.conn.select_db('db_corr')
        self.cursor.execute("""create table if not exists new_corr
        (start_date DATE not null,
        end_date DATE not null,
        ticker1 varchar(32) not null,
        ticker2 varchar(32) not null,
        type SMALLINT not null DEFAULT 0, 
        period INT not null,
        lag INT not null,
        corr DOUBLE,
        symbol1 varchar(32),
        symbol2 varchar(32),
        primary key(start_date, end_date, ticker1, ticker2, type, lag, period))""")
        self.cursor.execute("""create table if not exists ticker_symbol
                        (start_date DATE not null,
                        end_date DATE not null,
                        ticker varchar(32) not null,
                        symbol varchar(32),
                        primary key(start_date, end_date, ticker))""")

    def get_calculated(self):
        self.cursor.execute('select * from new_corr group by start_date')
        ans = self.cursor.fetchall()
        for row in ans:
            tmp = str(row[0]).split('-')
            self.calculated.append(tmp[0]+tmp[1]+tmp[2]+'.dat.gz')

    def get_uncalculated(self):
        for file in os.listdir(self.dir):
            if self.filter_by_size(file) and file not in self.calculated:
                self.uncalculated.append(file)

    def calculate_today(self):
        today= str(pd.datetime.now()).split(' ')[0]
        tmp = today.split('-')
        date = tmp[0]+tmp[1]+tmp[2]
        file = date+'.dat.gz'
        if self.filter_by_size(file):
            print 'calculating day:', date
            for type in [0,1]:
                corr = data_process.corrAna(filedir = self.dir, type = type)
                raw_data = corr.loaddata(date)
                for level in [0]:   # to get self.level updated
                    major = corr.findMostInType(raw_data, level = level)
                    data = corr.concatdata(dayLst = [date], level = level, filterLst = major.values())
                    corr.recordSymbol(date, major, level = level)
                    for period in [str(i)+'s' for i in [1]]:
                        for lag in [str(i)+'s' for i in [0, 1]]:
                            for target in major.values():
                                target = target[:2]+str(level)
                                if target in data.columns.values:
                                    sampled = data
                                    this_shifted = self.shift(sampled, target, lag, corr)
                                    symbol1 = corr.symbolDict[level][date][target[:2]]
                                    self.ticker_symbol(start_date=date, end_date=date,ticker=target, symbol=symbol1)
                                    corr_mat = this_shifted.corr()
                                    corr_mat.fillna(-2,inplace = True)
                                    for ticker2 in corr_mat.index.values:
                                        corr_value = corr_mat[target][ticker2]
                                        symbol2 = corr.symbolDict[level][date][ticker2[:2]]
                                        self.cursor.execute("""REPLACE INTO new_corr(
                                        start_date,
                                        end_date,ticker1,
                                        symbol1,
                                        ticker2,
                                        symbol2,
                                        type,
                                        period,
                                        lag,
                                        corr)
                                        VALUES ('%s','%s','%s','%s','%s','%s','%d','%d','%d','%.8f')"""
                                                            % (date, date, target, symbol1, ticker2, symbol2, type,
                                                               int(period[:-1]), int(lag[:-1]), corr_value))
                                        self.conn.commit()
        self.calculated.append(file)
        print 'done'

    def calculate_5days(self):
        '''calculate 5 working days from today'''
        cal_range = []
        date_rng = pd.date_range(end=pd.datetime.now(), periods=5, freq='B')
        for day in date_rng:
            tmp = (str(day).split(' ')[0]).split('-')
            day = tmp[0]+tmp[1]+tmp[2]
            if self.filter_by_size(day+'.dat.gz'):
                cal_range.append(day)
        for type in [0, 1]:
            corr = data_process.corrAna(filedir=self.dir, type=type)
            raw_data = corr.loaddata(cal_range[0])  # 由于实际时间跨度为7天，但用来求主力合约或次主力合约的数据只取了一天，所以如果主力合约变化的时候会对不上
            for level in [0, 1]:  # to get self.level updated
                major = corr.findMostInType(raw_data, level=level)
                data = corr.concatdata(dayLst=cal_range, level=level, filterLst=major.values())
                corr.recordSymbol(str(cal_range[0]) + '-' + str(cal_range[-1]), major, level=level)
                for period in [str(i) + 's' for i in [1, 5, 10, 20, 30, 60, 120]]:
                    for lag in [str(i) + 's' for i in [1, 5, 10, 20, 30, 60, 120]]:
                        for target in major.values():
                            target = target[:2] + str(level)
                            if target in data.columns.values:
                                sampled = self.sample(data, '1s', target, corr)
                                this_shifted = self.shift(sampled, target, lag, corr)
                                symbol1 = corr.symbolDict[level][str(cal_range[0]) + '-' + str(cal_range[-1])][
                                    target[:2]]
                                self.ticker_symbol(start_date=cal_range[0], end_date=cal_range[-1], ticker=target,
                                                   symbol=symbol1)
                                corr_mat = this_shifted.corr()
                                corr_mat.fillna(-2, inplace=True)
                                for ticker2 in corr_mat.index.values:
                                    corr_value = corr_mat[target][ticker2]
                                    symbol2 = corr.symbolDict[level][str(cal_range[0]) + '-' + str(cal_range[-1])][
                                        ticker2[:2]]
                                    self.cursor.execute(
                                        """REPLACE INTO new_corr(start_date,end_date,ticker1,symbol1,ticker2,symbol2,type,period,lag,corr)VALUES ('%s','%s','%s','%s','%s','%s','%d','%d','%d','%.8f')""" % (
                                            cal_range[0], cal_range[-1], target, symbol1, ticker2, symbol2, type,
                                        int(period[:-1]), int(lag[:-1]), corr_value))
                                    self.conn.commit()
        print 'done'

    def calculate_history(self):
        self.uncalculated.sort(reverse=True)
        for file in self.uncalculated:
            if self.filter_by_size(file):
                date = file.split('.')[0]
                print 'calculating day:', date
                for type in [0, 1]:
                    corr = data_process.corrAna(filedir=self.dir, type = type)
                    raw_data = corr.loaddata(date)
                    for level in [0]:   # to get self.level updated
                        major = corr.findMostInType(raw_data, level = level)
                        data = corr.concatdata(dayLst = [date], level = level, filterLst = major.values())
                        corr.recordSymbol(date, major, level = level)
                        for period in [str(i) + 's' for i in [0]]:
                            for lag in [str(i) + 's' for i in [1, 5]]:

                                for target in major.values():

                                    target = target[:2]+str(level)
                                    sampled = data
                                    if target in data.columns.values:

                                        this_shifted = self.shift(sampled, target, lag,corr)
                                        symbol1 = corr.symbolDict[level][date][target[:2]]
                                        self.ticker_symbol(start_date=date, end_date=date, ticker=target,
                                                           symbol=symbol1)
                                        corr_mat = this_shifted.corr()
                                        corr_mat.fillna(-2,inplace=True)
                                        for ticker2 in corr_mat.index.values:
                                            corr_value = corr_mat[target][ticker2]
                                            symbol2 = corr.symbolDict[level][date][ticker2[:2]]
                                            self.cursor.execute("""REPLACE INTO new_corr(start_date,end_date,ticker1,symbol1,ticker2,symbol2,type,period,lag,corr)VALUES ('%s','%s','%s','%s','%s','%s','%d','%d','%d','%.8f')""" % (date, date, target, symbol1, ticker2, symbol2, type,int(period[:-1]), int(lag[:-1]), corr_value))
                                            self.conn.commit()
            self.calculated.append(file)
            print 'done'

    def history_7_days(self):
        lst = []
        for file in os.listdir(self.dir):
            if self.filter_by_size(file):
                lst.append(int((file.split('.')[0])))
        earlist = np.min(lst)
        latest = np.max(lst)
        date_rng = pd.date_range(str(earlist), str(latest), freq='B')
        date_rng = [((str(day).split(' ')[0]).split('-'))[0]+((str(day).split(' ')[0]).split('-'))[1]+((str(day).split(' ')[0]).split('-'))[2] for day in date_rng]
        length = len(date_rng)
        for i in range(length-4):
            cal_range = date_rng[i:i+5]
            print 'calculating:', str(cal_range[0])+'-'+str(cal_range[-1])
            for type in [0, 1]:
                corr = data_process.corrAna(filedir=self.dir, type = type)
                raw_data = corr.loaddata(cal_range[0])                          # 由于实际时间跨度为7天，但用来求主力合约或次主力合约的数据只取了一天，所以如果主力合约变化的时候会对不上
                for level in [0, 1]:   # to get self.level updated
                    major = corr.findMostInType(raw_data, level = level)
                    data = corr.concatdata(dayLst = cal_range, level = level, filterLst = major.values())
                    corr.recordSymbol(str(cal_range[0])+'-'+str(cal_range[-1]), major, level = level)
                    for period in [str(i)+'s' for i in [0]]:
                        for lag in [str(i) + 's' for i in [1, 5]]:
                            for target in major.values():
                                target = target[:2]+str(level)
                                sampled = data
                                if target in data.columns.values:

                                    this_shifted = self.shift(sampled, target, lag,corr)
                                    symbol1 = corr.symbolDict[level][str(cal_range[0])+'-'+str(cal_range[-1])][target[:2]]
                                    self.ticker_symbol(start_date=cal_range[0], end_date=cal_range[-1], ticker=target,
                                                       symbol=symbol1)
                                    corr_mat = this_shifted.corr()
                                    corr_mat.fillna(-2,inplace=True)
                                    for ticker2 in corr_mat.index.values:
                                        corr_value = corr_mat[target][ticker2]
                                        symbol2 = corr.symbolDict[level][str(cal_range[0])+'-'+str(cal_range[-1])][ticker2[:2]]
                                        self.cursor.execute("""REPLACE INTO new_corr(start_date,end_date,ticker1,symbol1,ticker2,symbol2,type,period,lag,corr)VALUES ('%s','%s','%s','%s','%s','%s','%d','%d','%d','%.8f')""" % (cal_range[0], cal_range[-1], target, symbol1, ticker2, symbol2, type,int(period[:-1]), int(lag[:-1]), corr_value))
                                        self.conn.commit()
            print 'done'


    def shift(self, data, target, lag, corr ):
        align_base = corr.get_align_base(data)
        res = corr.shift_align(data, target, lag, align_base=align_base)
        return res

    def sample(self,data, period, target, corr):
        res = corr.sampledata(data, period, target)
        res.dropna(how = 'all',axis = 0, inplace=True)
        res.fillna(method='ffill', inplace=True)
        res.fillna(method='bfill', inplace=True)
        return res

    def filter_by_size(self, file):
        '''only calculate those files that have size bigger than 1Mb '''
        return os.path.getsize(self.dir+'/'+file)/float(1024*1024) > 1

    def ticker_symbol(self, start_date, end_date, ticker, symbol):
        self.cursor.execute('''REPLACE INTO ticker_symbol(
        start_date,
        end_date,
        ticker,
        symbol)
        values('%s','%s','%s','%s')''' %(start_date, end_date, ticker, symbol))
        self.conn.commit()


auto =automation(dir = '/home/charles/python/intern/output/')
auto.get_calculated()
auto.get_uncalculated()
auto.calculate_history()
auto.history_7_days()
#
# auto =automation(dir = '/hdd/ctp/day/')
# auto.calculate_today()    # calculate every day data
# auto.calculate_5days()
