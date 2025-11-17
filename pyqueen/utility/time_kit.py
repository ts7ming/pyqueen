import datetime
import time
from dateutil.relativedelta import relativedelta


class TimeKit:
    """
    时间工具箱
    """

    def __init__(self, theday=None, thetime=None, the_datetime=None):
        """
        theday: int(8)
        thetime: int(6)
        """
        if theday is None:
            self.theday = int(time.strftime("%Y%m%d"))
        else:
            self.theday = theday
        if thetime is None:
            self.thetime = int(time.strftime("%H%M%S"))
        else:
            self.thetime = thetime
        if the_datetime is None:
            pass
        else:
            self.theday = int(str(the_datetime)[0:8])
            self.thetime = int(str(the_datetime)[9:14])
        # 常用时间点
        self.__base_time(self.theday)

    def date2int(self, date_obj):
        """
        datetime对象转int
        """
        return int(date_obj.strftime('%Y%m%d'))

    @classmethod
    def time_delta(self, thetime, flag, value):
        """
        时间加减
        thetime: 基准时间
            int(6) 24小时制时间
            int(8) 8位数年月日
            int(14) 年月日时分秒
        flag: 加减单位
            years,months,days,hours,minutes,seconds
        value: 加减值
            thetime之前 value 写负值
            thetime之后 value 写正值
        """
        ts = str(thetime)
        if len(ts) == 8:
            thetime_obj = datetime.date(int(ts[0:4]), int(ts[4:6]), int(ts[6:8]))
        elif len(ts) <= 6:
            ts = ts.zfill(6)
            thetime_obj = datetime.datetime(2000, 1, 1, int(ts[0:2]), int(ts[2:4]), int(ts[4:6]))
        elif len(ts) == 14:
            thetime_obj = datetime.datetime(int(ts[0:4]), int(ts[4:6]), int(ts[6:8]), int(ts[8:10]), int(ts[10:12]),
                                            int(ts[12:14]))
        else:
            raise Exception('wrong date time')
        if flag == 'days' or flag == '日':
            new_date = thetime_obj + relativedelta(days=value)
        elif flag == 'weeks' or flag == '周':
            new_date = thetime_obj + relativedelta(weeks=value)
        elif flag == 'months' or flag == '月':
            new_date = thetime_obj + relativedelta(months=value)
        elif flag == 'years' or flag == '年':
            new_date = thetime_obj + relativedelta(years=value)
        elif flag == 'hours' or flag == '时':
            new_date = thetime_obj + relativedelta(hours=value)
        elif flag == 'minutes' or flag == '分':
            new_date = thetime_obj + relativedelta(minutes=value)
        elif flag == 'seconds' or flag == '秒':
            new_date = thetime_obj + relativedelta(seconds=value)
        else:
            print('error date flag  :' + str(flag))
            return None

        if len(ts) == 8:
            return int(new_date.strftime('%Y%m%d'))
        elif len(ts) <= 6:
            return str(new_date.strftime('%H%M%S'))
        elif len(ts) == 14:
            return int(new_date.strftime('%Y%m%d%H%M%S'))

    def delta(self, flag, value, format=8):
        new_time = self.time_delta(self.now, flag, value)
        if format==8:
            return str(new_time)[0:8]
        elif format == 10:
            return self.str(str(new_time)[0:8])
        else:
            return new_time

    @classmethod
    def get_day_list(self, start, end):
        """ start, end 之间所有日期 """
        day_list = []
        while start <= end:
            day_list.append(start)
            start = self.time_delta(start, 'days', 1)
        return day_list

    @classmethod
    def get_day_num(self, start, end):
        """ 计算start, end 之间的天数 """
        day_list = self.get_day_list(start, end)
        return len(day_list)

    @classmethod
    def get_nday_of_week(self, the_day):
        """ 查询the_day是星期几 """
        the_day_date = datetime.date(int(str(the_day)[0:4]), int(str(the_day)[4:6]), int(str(the_day)[6:8]))
        nday_of_week = int(the_day_date.weekday()) + 1
        return nday_of_week

    @classmethod
    def get_month_on_day(self, the_day):
        """ 获取指定日期月份 """
        month_start = int(str(the_day)[0:6] + '01')
        month_end = self.time_delta(self.time_delta(month_start, 'months', 1), 'days', -1)
        return month_start, month_end

    @classmethod
    def get_week_on_day(self, the_day):
        """ 获取指定日期周 """
        n = self.get_nday_of_week(the_day) - 1
        week_start = self.time_delta(the_day, 'days', -n)
        week_end = self.time_delta(week_start, 'days', 6)
        return week_start, week_end

    @classmethod
    def get_nday_of_month(self, the_day):
        """ 查询the_day日期 """
        num = int(str(the_day)[6:8])
        return num

    @classmethod
    def get_week_list(self, start, end):
        """ 获取 start, end 之间所有自然周 """
        week_list = []
        status = 0
        week = []
        for theday in self.get_day_list(start, end):
            if self.get_nday_of_week(theday) == 1:
                status = 1
                week = [theday]
            if self.get_nday_of_week(theday) == 7 and status == 1:
                status = 0
                week.append(theday)
            if status == 0 and len(week) == 2:
                week_list.append(week)
        if self.get_nday_of_week(end) != 7:
            wk_start = self.time_delta(end, 'days', 1 - self.get_nday_of_week(end))
            week_list.append([wk_start, end])
        return week_list

    @classmethod
    def get_month_list(self, start, end):
        """ 获取 start,end 之间所有自然月 """
        month_list = []
        status = 0
        month = []
        dlist = self.get_day_list(start, end)
        for theday in dlist:
            if self.get_nday_of_month(theday) == 1:
                m_end = self.time_delta(theday, 'months', 1)
                m_end = self.time_delta(m_end, 'days', -1)
                if m_end in dlist:
                    month_list.append([theday, m_end])
        return month_list

    @classmethod
    def int2str(self, time_value, sep='-'):
        """ int型时间转字符串 """
        time_value = str(time_value)
        if len(time_value) == 8:
            return time_value[0:4] + sep + time_value[4:6] + sep + time_value[6:8]
        elif len(time_value) == 14:
            return time_value[0:4] + sep + time_value[4:6] + sep + time_value[6:8] + ' ' + time_value[8:10] + ':' + time_value[10:12] + ':' + time_value[12:14]
        else:
            return None

    @classmethod
    def str(self, time_value, sep='-'):
        """ int型时间转字符串 """
        time_value = str(time_value)
        if len(time_value) == 8:
            return time_value[0:4] + sep + time_value[4:6] + sep + time_value[6:8]
        elif len(time_value) == 14:
            return time_value[0:4] + sep + time_value[4:6] + sep + time_value[6:8] + ' ' + time_value[8:10] + ':' + time_value[10:12] + ':' + time_value[12:14]
        else:
            return None
        
    @classmethod
    def date_div(self, start, end, num, by='groups'):
        """ 指定分段数 拆分时间段 """
        if by == 'ndays':
            step = num
        elif by == 'groups':
            d_len = self.get_day_num(start, end)
            step = int(d_len / num)
        else:
            raise Exception('错误参数 by')
        flag = 1
        d_list = []
        while flag:
            tmp_day = self.time_delta(start, 'days', step)
            if tmp_day >= end:
                tmp_day = end
                flag = 0
            d_list.append([start, tmp_day])
            start = self.time_delta(tmp_day, 'days', 1)
        return d_list

    @staticmethod
    def get_nweek_of_year(week_start):
        week_start = str(week_start)
        thetime_obj = datetime.date(int(week_start[0:4]), int(week_start[4:6]), int(week_start[6:8]))
        return thetime_obj.isocalendar()[1]

    def __base_time(self, today):
        today_date = datetime.date(int(str(today)[0:4]), int(str(today)[4:6]), int(str(today)[6:8]))
        self.today = today
        self.now = int(str(self.today) + str(self.thetime).zfill(6))
        self.hour = int(str(self.thetime).zfill(6)[0:2])
        self.minute = int(str(self.thetime).zfill(6)[2:4])
        self.second = int(str(self.thetime).zfill(6)[4:6])

        self.yesterday = self.time_delta(self.today, 'days', -1)
        self.yesterday1 = self.time_delta(self.today, 'days', -2)
        self.tomorrow = self.time_delta(self.today, 'days', 1)

        self.today8 = str(today)
        self.today10 = self.str(today)
        self.yesterday8 = str(self.yesterday)
        self.yesterday10 = self.str(self.today10)
        self.tomorrow8 = str(self.tomorrow)
        self.tomorrow10 = self.str(self.tomorrow)

        # 本周第几天  1-7 对应周一到周末
        nday_of_week = int(today_date.weekday()) + 1
        self.nday_of_week = nday_of_week

        # 本月第几天
        ndays_of_month = int(str(today)[6:8])
        self.nday_of_month = ndays_of_month

        # 本周开始日期
        week_start_date = today_date - datetime.timedelta(days=nday_of_week - 1)
        self.week_start = self.date2int(week_start_date)

        # 上周起止日期
        lw_start_date = week_start_date - datetime.timedelta(weeks=1)
        lw_end_date = lw_start_date + datetime.timedelta(days=6)
        self.lw_start = self.date2int(lw_start_date)
        self.lw_end = self.date2int(lw_end_date)

        # 本月开始日期
        month_start_date = datetime.date(int(str(today)[0:4]), int(str(today)[4:6]), 1)
        self.month_start = self.date2int(month_start_date)

        # 上月起止日期
        lm_start_date = month_start_date - relativedelta(months=1)
        lm_end_date = month_start_date - datetime.timedelta(days=1)
        self.lm_start = self.date2int(lm_start_date)
        self.lm_end = self.date2int(lm_end_date)

        # 上上周
        lw2_start_date = lw_start_date - datetime.timedelta(weeks=1)
        lw2_end_date = lw2_start_date + datetime.timedelta(days=6)
        self.lw2_start = self.date2int(lw2_start_date)
        self.lw2_end = self.date2int(lw2_end_date)

        # 上上月起止日期
        lm2_start_date = lm_start_date - relativedelta(months=1)
        lm2_end_date = lm_start_date - datetime.timedelta(days=1)
        self.lm2_start = self.date2int(lm2_start_date)
        self.lm2_end = self.date2int(lm2_end_date)