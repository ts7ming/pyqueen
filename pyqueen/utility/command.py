import sys
from .utils import Utils

doc = '''
  ____         ___                        
 |  _ \ _   _ / _ \ _   _  ___  ___ _ __  
 | |_) | | | | | | | | | |/ _ \/ _ \ '_ \ 
 |  __/| |_| | |_| | |_| |  __/  __/ | | |
 |_|    \__, |\__\_\\\__,_|\___|\___|_| |_|
        |___/                                                                               
=================================================================================                                                                                              
用法: pyqueen command args1,args2,...
---
command: 
    #1  sql2table [file_path] 从sql解析用到的表(通过正则解析, 有误差) (不带参数时读取剪切板)
    
    #2  detcode file_path 检测文件编码
    
    #3  md5 基于剪切板文本生成md5
'''


def cmd():
    parms = sys.argv[1:]
    if len(parms) == 0:
        print(doc)
    elif parms[0] == 'sql2table':
        if len(parms) == 1:
            print('从剪切板读取SQL文本')
            import pyperclip
            sql_text = str(pyperclip.paste()).replace('\n', '').strip(' ')
        else:
            with open(parms[1], 'r', encoding='utf-8') as f:
                sql_text = f.read()
        tb_list = Utils.sql2table(sql_text)
        print('================= begin =========================\n')
        for tb in tb_list:
            print(tb)
        print('\n================= end =========================\n')
    elif parms[0] == 'detcode':
        if len(parms) >= 2:
            print('================= begin =========================\n')
            encoding, confidence = Utils.detect_encoding(parms[1])
            confidence = str(int(confidence * 100)) + '%'
            print('文件编码: %s, 可信度: %s' % (str(encoding), str(confidence)))
            print('\n================= end =========================\n')
        else:
            print('指定文件路径')
    elif parms[0] == 'md5':
        import pyperclip
        text = str(pyperclip.paste()).replace('\n', '').strip(' ')
        print('================= begin =========================\n')
        print('输入文本: "%s"\n' % text)
        print(Utils.md5(text))
        print('\n================= end =========================\n')
