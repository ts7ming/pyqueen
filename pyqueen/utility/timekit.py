import datetime
import time
from dateutil.relativedelta import relativedelta


class TK:
    """
    时间工具箱
    """
    @staticmethod
    def delta(flag, value, fmt='%Y-%m-%d'):
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
            new_time = thetime_obj + relativedelta(days=value)
        elif flag == 'weeks' or flag == '周':
            new_time = thetime_obj + relativedelta(weeks=value)
        elif flag == 'months' or flag == '月':
            new_time = thetime_obj + relativedelta(months=value)
        elif flag == 'years' or flag == '年':
            new_time = thetime_obj + relativedelta(years=value)
        elif flag == 'hours' or flag == '时':
            new_time = thetime_obj + relativedelta(hours=value)
        elif flag == 'minutes' or flag == '分':
            new_time = thetime_obj + relativedelta(minutes=value)
        elif flag == 'seconds' or flag == '秒':
            new_time = thetime_obj + relativedelta(seconds=value)
        else:
            print('error date flag  :' + str(flag))
            return None

        return new_time

    @staticmethod
    def today(fmt='%Y-%m-%d'):
        if str(fmt) == '0':
            return str(time.strftime('%Y%m%d'))
        else:
            return str(time.strftime(fmt))

    @staticmethod
    def now(fmt='%Y-%m-%d %H:%M:%S'):
        if str(fmt) == '0':
            return str(time.strftime('%Y%m%d%H%M%S'))
        else:
            return str(time.strftime(fmt))

    @staticmethod
    def yesterday(obj='int',fmt='%Y-%m-%d'):
        pass


print(TK.now_int())
print(TK.now_str())
