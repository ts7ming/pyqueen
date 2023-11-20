import re
from pyqueen.utility.utils import Utils
import os
import re

script = '''import win32api, win32con,time;time.sleep(%s);win32api.MessageBox(0, '%s', 'PyQueen', win32con.MB_OK);'''


def kw(cmd):
    get_time = ['时间', '时候', '几点']
    wk = ['一', '二', '三', '四', '五', '六', '日']
    for gt in get_time:
        if gt in cmd:
            from pyqueen.utility.time_kit import TimeKit
            tk = TimeKit()
            wk_str = wk[tk.nday_of_week - 1]
            print('PyQueen: 现在是: %s 星期%s' % (str(tk.int2str(tk.now)), wk_str))
    if re.match(r'^过[0-9]+[(分钟)(小时)(秒)(秒钟)]+提醒我.+', cmd):
        n = re.search(r'^过([0-9]+)[(分钟)(小时)(秒)(秒钟)]+提醒我.+', cmd).group(1)
        ntype = re.search(r'^过[0-9]+([(分钟)(小时)(秒)(秒钟)]+)提醒我.+', cmd).group(1)
        nmsg = re.search(r'^过[0-9]+[(分钟)(小时)(秒)(秒钟)]+提醒我(.+)', cmd).group(1)
        if ntype == '分钟':
            nsec = str(60 * int(n))
        elif ntype == '小时':
            nsec = str(3600 * int(n))
        else:
            nsec = str(1 * int(n))
        script2 = script % (nsec, nmsg)
        import subprocess
        subprocess.Popen('python -c "' + script2 + '"', shell=True)
        print('PyQueen: 将在 '+str(nsec)+'秒 后弹窗提醒')
    else:
        pass


def pq_chat():
    service = True
    while service:
        msg = input('\nPyQueen: May I help you, Sir\n$:')
        if msg.lower() == 'bye':
            service = False
            print('PyQueen: Bye')
        elif str(msg)[0:7] == 'pyqueen':
            cmd = str(msg).split(' ')[1]
            func = getattr(Utils, cmd)
            param = str(msg).split(' ')[2:]
            print('PyQueen: '+func(*param))
        else:
            kw(msg)
