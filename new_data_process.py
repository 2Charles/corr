#-*- coding:utf-8 -*-

import pandas as pd
import re
import matplotlib.pylab as plt
import seaborn as sns
import os
import gc
import new_sample_lib
reload(new_sample_lib)

class pre_process(object):
    '''need to input three parameters to initialize, type controls rolling or aggravated
    0 for rolling, 1 for aggravated;
    level : 0 for major option, 1 for secondary, 2 for third '''

    def __init__(self, filedir, type, split = 2):
        self.filedir = filedir
        self.type = type
        self.symbolDict = {}
        self.split = split

    def generateDayLst(self, start, end):
        days = pd.date_range(start=start, end=end, freq='B')
        dayLst = []
        for day in days:
            temp = day.strftime('%Y-%m-%d').split('-')
            day = temp[0]+temp[1]+temp[2]
            dayLst.append(day)
        return dayLst

    def loaddata(self, day):
        '''only load single day
        split controls split one sec into how many parts'''
        if isinstance(day, str):
            if '.gz' not in day:
                dir = self.filedir + day + '.dat.gz'
            else:
                print 'I am here'
                dir = self.filedir + day
        if isinstance(day, int):
            dir = self.filedir + str(day) + '.dat.gz'
        if '.gz' in dir:
            temp = pd.read_csv(dir, header=None, index_col=0, compression='gzip',
                               names=['ticker', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume', 'last_price',
                                      'last_volume', 'open_interest', 'turnover'])
        else:
            temp = pd.read_csv(dir, header=None, index_col=0,
                               names=['ticker', 'bid_price', 'bid_volume', 'ask_price', 'ask_volume', 'last_price',
                                      'last_volume', 'open_interest', 'turnover'])
        self.timeIndex(temp, day)
        temp.sort_index(inplace=True)
        timerange1 = pd.date_range(day+' 09', day+' 11:30', freq=str(1000/self.split)+'ms')
        timerange2 = pd.date_range(day + ' 13:30', day + ' 15', freq=str(1000/self.split)+'ms')
        flag = map(lambda x: (x in timerange1) or (x in timerange2), temp.index.values)  # only keep data that belongs to time [09,11:30] and [13:30,15:00]
        temp = temp[flag]
        return temp

    def timeIndex(self, df, date):
        '''trim time into 500ms or 250ms and change it into timeseries and set as index'''
        lst = list(df.index.values)
        year, month, day = date[:4], date[4:6], date[6:]
        res = []
        for time in lst:
            s = re.split(r'[:.]', time)
            if self.split == 2:
                if int(s[-1]) <= 500:
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '500'
                elif int(s[-1]) < 1000:
                    s[-2] = str(int(s[-2]) + 1)
                    if int(s[-2]) == 60:
                        s[-3] = str(int(s[-3]) + 1)
                        s[-2] = '00'
                        if int(s[-3]) == 60:
                            s[-3] = '00'
                            s[-4] = str(int(s[-4]) + 1)
                    elif len(s[-2]) == 1:
                        s[-2] = '0' + s[-2]
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '000'
            elif self.split == 4:
                if int(s[-1]) <= 250:
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '250'
                elif int(s[-1]) <= 500:
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '500'
                elif int(s[-1]) <= 750:
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '750'
                elif int(s[-1]) < 1000:
                    s[-2] = str(int(s[-2]) + 1)
                    if int(s[-2]) == 60:
                        s[-3] = str(int(s[-3]) + 1)
                        s[-2] = '00'
                        if int(s[-3]) == 60:
                            s[-3] = '00'
                            s[-4] = str(int(s[-4]) + 1)
                    elif len(s[-2]) == 1:
                        s[-2] = '0' + s[-2]
                    s = s[0] + ':' + s[1] + ':' + s[2] + '.' + '000'
            s = year + '-' + month + '-' + day + ' ' + s
            res.append(s)
        df.index = pd.DatetimeIndex(res)

    def filterdata(self, df, lst, period='500ms', level=0, threshold=1000):
        '''lst is a list of option that want to keep from raw dataframe'''
        if self.type == 1:
            keywd = 'aggravated_return'
        else:
            keywd = 'rolling_return'
        align_base = self.get_align_base(df)
        res = pd.DataFrame()
        for name in lst:
            temp = df[df['ticker'] == name]
            if temp.shape[0] < threshold:
                continue
            else:
                temp = self.calcAll(temp, period=period)
                temp = temp.rename(columns={keywd: name[:2]+str(level)})
                temp = pd.DataFrame(temp.loc[:, name[:2]+str(level)])
                temp = self.align_drop(data=temp, base=align_base)
                res = pd.concat([res, temp], axis=1)
        res.fillna(method='ffill', axis=0, inplace=True)
        res.fillna(method='bfill', axis=0, inplace=True)
        return res

    def concatdata(self, dayLst, period='500ms', level=0, filterLst='major'):
        '''load multidays and filter and concat together'''
        if len(dayLst) == 1:
            symbolKey = dayLst[0]
        else:
            symbolKey = dayLst[0]+'-'+dayLst[-1]
        temp = self.loaddata(day=dayLst[0])
        if filterLst == 'major':
            major = self.findMostInType(temp)
            self.recordSymbol(symbolKey, major, level=level)
            filterLst = major.values()
        res = self.filterdata(temp, lst=filterLst, period=period, level=level)
        del temp; gc.collect()
        if len(dayLst) > 1:
            for day in dayLst[1:]:
                temp = self.loaddata(day=day)
                major = self.findMostInType(temp)
                filterLst = major.values()
                self.recordSymbol(symbolKey, major, level = level)
                res0 = self.filterdata(temp, lst=filterLst, level = level)
                res = pd.concat([res, res0])
                del temp, res0; gc.collect()
        return res

    def recordSymbol(self, date, symbolLst, level = 0): # a dictionary record ticker and symbol, first key is level and then date
        '''record symbol and ticker'''
        if level not in self.symbolDict.keys():
            self.symbolDict[level] = {}
            self.symbolDict[level][date] = symbolLst
        else:
            self.symbolDict[level][date] = symbolLst

    def shift_align(self, data, target, lag, align_base):
        '''first shift data of target colume at lag and then align it to origin dataframe'''
        df = data.copy()
        temp = pd.DataFrame(df[target].shift(periods=-int(lag[:-1]), freq = lag[-1]))
        temp = self.align_drop(data=temp, base = align_base)
        df[target] = temp
        df.fillna(method = 'ffill', inplace=True)
        df.fillna(method = 'bfill', inplace=True)
        return df

    def get_align_base(self, df):
        '''get index as the align base for later align'''
        align_base = pd.DataFrame([1 for i in range(df.shape[0])],index=df.index)
        align_base['helper'] = align_base.index
        align_base.drop_duplicates(subset='helper', inplace=True)
        align_base.drop('helper', axis=1, inplace=True)
        return align_base

    def align_drop(self, data, base):
        '''align target data to base index'''
        df = data.copy()
        _, df = base.align(df, join='left', axis = 0)
        df = pd.DataFrame(df)
        df['helper'] = df.index
        df.drop_duplicates(subset = 'helper', inplace=True)
        df.drop('helper', axis=1, inplace=True)
        return df

    def getsymbol(self, df, ticker):    #依据symbol前两个得到对应的ticker
        '''column name according to ticker as column name maybe ru0 or ru1 or ru2 and use this function to find symbol'''
        if len(ticker) == 3:
            ticker = ticker[:2]
        if len(ticker) == 1:
            ticker = ticker + '1'
        for name in df.columns.values:
            if ticker == name[:2]:
                return name

    def midPrice(self, df):  # 计算mid_pricr,存在部分记录中bid_price或者ask_price出错的情形
        flag = (df.ask_price * df.bid_price) != 0
        if flag.all():
            df.loc[:, 'mid_price'] = (df.ask_price + df.bid_price) / 2
        else:
            bid_index, ask_index = 1, 3
            mid_price = []
            for i in range(df.shape[0]):
                if (df.iloc[i, bid_index] != 0) and (df.iloc[i, ask_index] != 0):
                    mid_price.append((df.iloc[i, bid_index] + df.iloc[i, ask_index])/2)
                elif df.iloc[i, bid_index] == 0:
                    mid_price.append(df.iloc[i, ask_index])
                elif df.iloc[i, bid_index] == 0:
                    mid_price.append(df.iloc[i, bid_index])
                else:
                    mid_price.append(0)
            df.loc[:, 'mid_price'] = mid_price
            df.mid_price.replace(0, method='ffill', inplace=True)

    def rollingRet(self, df, period):
        sample = new_sample_lib.sample(period=period, split=self.split)
        res = sample.sample_multidays(df)
        return res

    def aggravatedRet(self, df):
        data = df.copy()
        data['aggravated_return'] = ((data['mid_price'] - data['mid_price'].values[0]) / data['mid_price'].values[0]).values
        return data

    def calcAll(self, df, period):
        self.midPrice(df)
        df = self.rollingRet(df, period)
        df = self.aggravatedRet(df)
        return df

    def filterName(self, lst):  # 判断是否为期权
        '''judge whether is option or not'''
        ans = []
        for name in lst:
            if not ('-P-' in name or '-C-' in name or 'SR' in name):
                ans.append(name)
        return ans

    def findMostInType(self, df, level = 0):  #寻找主力合约 选取第二、第三通过每选出一次就把那一些从列表里去掉
        dic = df.groupby('ticker')['turnover'].max()
        lst = dic.index.values
        lst = self.filterName(lst)
        for time in range(level+1):
            existed = []
            length = {}
            most = {}
            for name in lst:
                l = dic[name]
                if name[:2] in existed:
                    if l > length[name[:2]]:
                        most[name[:2]] = name
                        length[name[:2]] = l
                else:
                    existed.append(name[:2])
                    length[name[:2]] = l
                    most[name[:2]] = name
            for times in range(len(lst)):
                for elem in lst:
                    if elem in most.values():
                        lst.remove(elem)
        return most

# pre =pre_process(filedir='/home/charles/python/intern/data/',type = 0, split=2)
# res = pre.concatdata(['20180201'])