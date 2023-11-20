from pyqueen.utility.utils import Utils
import os

os.system('chcp 65001')

START = '\033[33m'
END = '\033[0m'


def pq_chat():
    service = True
    while service:
        msg = input(START + '\nPyQueen: May I help you, Sir\n$:' + END)
        if msg == 'bye':
            service = False
            print(START + 'PyQueen: Bye' + END)
        elif str(msg)[0:7] == 'pyqueen':
            cmd = str(msg).split(' ')[1]
            func = getattr(Utils, cmd)
            param = str(msg).split(' ')[2:]
            print(param)
            print(func(*param))