#! /home/hui/anaconda2/bin/python
#coding:utf-8

import pandas as pd
import seaborn as sns
import matplotlib.pylab as plt
import MySQLdb
import  math

def connect(db_name = 'db_corr', passwd = 'hhui123456'):
    conn = MySQLdb.connect(host='localhost', user='root', passwd=passwd)
    cursor = conn.cursor()
    conn.select_db(db_name)
    return cursor


cursor = connect()


def query(ticker1, k=10, period=1, lag=1, type=0, date_rng='default', index_by=0, cursor=cursor,
          table_name='new_corr', figsize=(15,10)):
    '''default date range means past 5 working days
    index_by is used in sort_values and by = df.columns.values[index_by]'''
    if date_rng == 'default':
        date_rng = pd.date_range(end = pd.datetime.now(), periods=5, freq = 'B')
    res = {}
    for day in date_rng:
        if len(str(day).split(' ')) != 1:    # when date_rng like [Timestamp('2017-12-29 10:06:50.796996', freq='B')] rather than ['20171204']
            tmp = (str(day).split(' ')[0]).split('-')
            day = tmp[0]+tmp[1]+tmp[2]
        res[day] = {}
        sql = 'select * from ' + table_name + ' where ticker1 = ' + '\"' + ticker1 + '\"' + ' and period = ' + str(
            period) + ' and lag = ' + str(lag) + ' and type = ' + str(type) + ' and start_date = ' + day + ' order by corr DESC '
        cursor.execute(sql)
        rec = cursor.fetchall()
        for i in rec[:k]:
            res[day][i[3]] = i[7]
    corr = pd.DataFrame(res)
    corr = corr.sort_values(by=corr.columns.values[index_by],ascending=False)
    fontsize = 15
    if corr.shape[1] > 5:
        fontsize = 12
    if corr.shape[1] > 10:
        fontsize = 10
    plt.figure(figsize=figsize)
    sns.heatmap(corr, cmap = 'coolwarm', cbar=True, fmt='.4f', annot=True,annot_kws={'size': fontsize})
    plt.yticks(rotation = 45, fontsize = fontsize)
    plt.xticks(rotation = 45, fontsize =fontsize)
    plt.show()
    return res

def query_kind(kind, ticker1,level = 0,period = 1, lag = 1, type = 0, date_rng = 'default', cursor = cursor, table_name = 'new_corr', figsize = (15,10)):
    '''output correlation heatmap of same kind such as farm-products
    your input should be one of [] or a specified list
    level controls first major or second major like ru0 or ru1'''
    if kind == 'noble':
        kind = ['ag', 'au']
        kind = [elem+str(level) for elem in kind]
    if kind == 'non-ferrous':
        kind = ['cu', 'al', 'ni', 'pb', 'zn', 'sn']
        kind = [elem + str(level) for elem in kind]
    if kind == 'black':
        kind = ['hc', 'SF', 'SM', 'i1', 'j1', 'jm', 'rb', 'ZC']
        kind = [elem + str(level) for elem in kind]
    if kind == 'farm':
        kind = ['CF', 'CY', 'LR', 'OI', 'PM', 'RI', 'RM', 'RS', 'SR', 'WH', 'c1', 'cs', 'jd', 'm1', 'p1', 'y1', 'a1', 'b1']
        kind = [elem + str(level) for elem in kind]
    if kind == 'chemical':
        kind = ['bu', 'fu', 'ru', 'FG', 'MA', 'TA', 'bb', 'fb', 'I1', 'pp', 'v1']
        kind = [elem + str(level) for elem in kind]
    if date_rng == 'default':
        date_rng = pd.date_range(end = pd.datetime.now(), periods=5, freq = 'B')
    res = {}
    for day in date_rng:
        if len(str(day).split(' ')) != 1:    # when date_rng like [Timestamp('2017-12-29 10:06:50.796996', freq='B')] rather than ['20171204']
            tmp = (str(day).split(' ')[0]).split('-')
            day = tmp[0]+tmp[1]+tmp[2]
        res[day] = {}
        sql = 'select * from ' + table_name + ' where period = ' + str(
            period) + ' and lag = ' + str(lag) + ' and type = ' + str(type) + ' and start_date = ' + day + ' and ticker1 = \"'+ticker1+'\"' ' order by corr DESC '
        cursor.execute(sql)
        rec = cursor.fetchall()
        for i in rec:
            res[day][i[3]] = i[7]
    tmp = pd.DataFrame(res)
    kind_ = []
    for elem in kind:
        if elem in tmp.index.values:
            kind_.append(elem)
    corr = tmp.loc[kind_]
    fontsize = 15
    if corr.shape[1] > 5:
        fontsize = 12
    if corr.shape[1] > 10:
        fontsize = 10
    plt.figure(figsize=figsize)
    sns.heatmap(corr, cmap='coolwarm', cbar=True, fmt='.4f', annot=True, annot_kws={'size': fontsize})
    plt.yticks(rotation=45, fontsize=fontsize)
    plt.xticks(rotation=45, fontsize=fontsize)
    plt.show()
    return tmp.loc[kind_]

def trend(ticker1, ticker2, date_rng = 'default', type = 0,lagLst = None, periodLst = None, table_name = 'new_corr', cursor = cursor, show = True, title_tail = ''):
    # 给出两个ticker之间在不同lag、period下corr随时间的变化
    '''plot how relation of two tickers change by time at different period and lag'''
    if lagLst == None:
        lagLst = [1, 5, 10, 20, 30, 60, 120]
    elif 's' in lagLst[0]:
        lagLst = [int(elem[:-1]) for elem in lagLst]
    if periodLst == None:
        periodLst = [1, 5, 10, 20, 30, 60, 120]
    elif 's' in periodLst[0]:
        periodLst = [int(elem[:-1]) for elem in periodLst]
    if date_rng == 'default':
        date_rng = pd.date_range(end = pd.datetime.now(), periods=5, freq = 'B')
    res = {}
    for lag in lagLst:
        for period in periodLst:
            res[str(lag)+'s-'+ str(period)+'s'] = {}
            for day in date_rng:
                if len(str(day).split(' ')) != 1:    # when date_rng like [Timestamp('2017-12-29 10:06:50.796996', freq='B')] rather than ['20171204']
                    tmp = (str(day).split(' ')[0]).split('-')
                    day = tmp[0]+tmp[1]+tmp[2]
                sql = 'select corr from ' + table_name + ' where period = ' + str(
                    period) + ' and lag = ' + str(lag) + ' and type = ' + str(
                    type) + ' and start_date = ' + day + ' and end_date = '+day + ' and ticker1 = \"' + ticker1 + '\"'\
                      + ' and ticker2 = \"'+ticker2 + '\"' + ' order by corr DESC '
                cursor.execute(sql)
                rec = cursor.fetchall()
                for i in rec:
                    res[str(lag)+'s-'+ str(period)+'s'][day] = i[0]
    res = pd.DataFrame(res)
    res.plot()
    plt.title('a-b means lag-period match'+title_tail)
    if show:
        plt.show()
    return res

def select_symbol(ticker1, dateLst, threshold = 0.05, cursor = cursor, type = 0, table_name = 'new_corr'):
    symbolLst = []
    for day in dateLst:
        sql = 'select ticker2 from ' + table_name + ' where ticker1 = \"' + ticker1 + '\" and start_date = ' + day + \
              ' and type = ' + str(type) + ' and corr > ' + str(threshold) + ' and corr < 1'
        cursor.execute(sql)
        rec = cursor.fetchall()
        for i in rec:
            if i[0] not in symbolLst:
                symbolLst.append(i[0])
    for symbol in symbolLst:
        trend(ticker1, symbol, dateLst,type = type, table_name=table_name, cursor=cursor,show = False, periodLst=['1s','5s'],lagLst=['1s','5s'], title_tail='-'+symbol)
    plt.show()


