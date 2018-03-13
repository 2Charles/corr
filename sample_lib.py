import re
import pandas as pd
import numpy as np

class sample(object):
    def __init__(self, period, target, split = 2):
        self.period = period
        self.target = target
        self.split = split

    def sample_multidays(self, data, target):
        '''sample data manually, keep as many none-zero value as possible'''
        day_first = re.split(r'[-T]', str(data.index.values[0]))[0]+re.split(r'[-T]', str(data.index.values[0]))[1]+re.split(r'[-T]', str(data.index.values[0]))[2]
        day_last = re.split(r'[-T]', str(data.index.values[-1]))[0]+re.split(r'[-T]', str(data.index.values[0]))[1]+re.split(r'[-T]', str(data.index.values[0]))[2]
        if day_first == day_last:
            day = day_first
            res = self.sample_single_day(data = data, day = day, target = target)
        else:
            res = pd.DataFrame()
            days = pd.date_range(start=day_first, end=day_last, freq='B')
            daylst = []
            for day in days:
                temp = day.strftime('%Y-%m-%d').split('-')
                day = temp[0]+temp[1]+temp[2]
                daylst.append(day)
            for day in daylst:
                singleday = self.extract_single_day(data, day)
                temp = self.sample_single_day(data = singleday, day = day, target=target)
                res = pd.concat([res, temp])
        return res

    def extract_single_day(self, data, day):
        '''get single data from a concated multiday data'''
        timerange1 = pd.date_range(day+' 09', day+' 11:30', freq = str(1000/self.split)+'ms')
        timerange2 = pd.date_range(day + ' 13:30', day + ' 15', freq=str(1000/self.split)+'ms')
        flag = map(lambda x: (x in timerange1) or (x in timerange2), data.index.values)      
        if np.sum(flag) == 0:
            print 'your data contains no records of day:', day
        else:
            return data[flag]

    def sample_single_day(self, data, day, target):
        step = int(self.period[:-1]) * self.split
        rng1 = pd.period_range(day + ' 09', day + ' 11:30', freq= self.period)
        rng2 = pd.period_range(day + ' 13:30', day + ' 15', freq= self.period)
        morning_rng = pd.date_range(day + ' 09', day + ' 11:30', freq=str(1000/self.split)+'ms')
        morning_flag = [True if time in morning_rng else False for time in data.index.values]
        afternoon_flag = [False if morning else True for morning in morning_flag]
        morning_data = data[morning_flag]
        afternoon_data = data[afternoon_flag]
        res = []
        accumulated_row = 0
        for i in range(len(rng1)-1):
            time_range = pd.date_range(str(rng1[i]), str(rng1[i+1]),freq=str(1000/self.split)+'ms')
            flag = [True if time in time_range else False for time in morning_data.index.values[accumulated_row:accumulated_row+step]]
            slice = morning_data[accumulated_row:accumulated_row+step][flag]
            to_append = accumulated_row+0
            for row_num in range(slice.shape[0]):   # get row numbers of target column that has none-zero value
                if slice[target][row_num] != 0:
                    to_append = row_num+accumulated_row
                    continue
            accumulated_row += slice.shape[0]
            res.append(to_append)
        for i in range(len(rng2)-1):
            time_range = pd.date_range(str(rng2[i]), str(rng2[i+1]),freq=str(1000/self.split)+'ms')
            flag = [True if time in time_range else False for time in data.index.values[accumulated_row:accumulated_row+step]]
            slice = data[accumulated_row:accumulated_row+step][flag]
            to_append = accumulated_row+0
            for row_num in range(slice.shape[0]):   # get row numbers of target column that has none-zero value
                if slice[target][row_num] != 0:
                    to_append = row_num+accumulated_row
                    continue
            accumulated_row += slice.shape[0]
            res.append(to_append)
        flag = [True if i in res else False for i in range(data.shape[0])]
        return data[flag]