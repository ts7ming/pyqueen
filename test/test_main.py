from pyqueen import DataSource, TimeKit
import pandas as pd
import os

tk = TimeKit()

SERVERS = {
    'mssql_1': {
        'conn_type': 'mssql',
        'host': 'localhost',
        'port': '1433',
        'username': 'sa',
        'password': 'mssql123',
        'db_name': 'master'
    },
    'pgsql_1': {
        'conn_type': 'pgsql',
        # 'host': 'localhost',
        'host':'olympus',
        'port': '5432',
        'username': 'postgres',
        'password': ' ',
        'db_name': 'postgres'
    },
    'mysql_1': {
        'conn_type': 'mysql',
        'host': '172.23.213.221',
        'port': '3306',
        'username': 'root',
        'password': '',
        'db_name': 'mysql'
    },
}


def clean():
    for x in os.listdir('.'):
        if '.db' in x or '.xlsx' in x:
            os.remove(x)


def test_data_source(conn_info):
    ds = DataSource(**conn_info)
    test_db_name = 'pq_td_' + str(tk.today)
    test_table_name = 'pq_tb_' + str(tk.today)
    print('创建数据库')
    auto_commit = True
    if conn_info['conn_type'] == 'mssql': ds.set_db('master')
    if conn_info['conn_type'] == 'pgsql': ds.set_db('postgres')
    ds.exe_sql(sql='DROP DATABASE IF EXISTS %s' % test_db_name, auto_commit=auto_commit)
    ds.exe_sql(sql='CREATE DATABASE %s' % test_db_name, auto_commit=auto_commit)
    ds.set_db(test_db_name)

    auto_commit = False
    print('创建表')
    ds.exe_sql('DROP TABLE IF EXISTS ' + test_table_name)
    ds.exe_sql('CREATE TABLE %s (id INT NOT NULL,name varchar(100) NULL)' % test_table_name)
    ds.exe_sql("INSERT INTO %s VALUES (9,'hhh')" % test_table_name)

    # 模拟数据
    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    print('写入表')
    ds.to_db(df, tb_name=test_table_name, fast_load=False)

    print('查询')
    v = ds.read_sql('select count(1) as v from ' + test_table_name)
    if int(v.values[0][0]) != 5:
        print('read_sql not match')
    print('导出excel')
    ds.to_excel(file_path='df2excel.xlsx', sheet_list=[[df, 'sht']])

    auto_commit = True
    print('删除')
    if conn_info['conn_type'] == 'mssql': ds.set_db('master')
    if conn_info['conn_type'] == 'pgsql': ds.set_db('postgres')
    ds.exe_sql(sql='DROP DATABASE IF EXISTS %s' % test_db_name, auto_commit=auto_commit)


def test_data_source_e():
    print('单独使用 read_sql')
    ds = DataSource()
    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    df2 = pd.DataFrame({'id': [11, 22, 33, 44], 'name': ['trtr', 'sd', 'gf', 'hg']})
    df_r = ds.read_sql(sql='select * from tb1 union all select * from tb2', data={'tb1': df, 'tb2': df2})
    if df_r.shape[0] != 8:
        print('not match')

    print('sql on excel')
    ds2 = DataSource(conn_type='excel', file_path='test_data_source_e')
    ds2.to_excel(sheet_list=[[df, 'tb1'], [df2, 'tb2']])
    df_r = ds2.read_sql(sql='select * from tb1 union all select * from tb2')
    if df_r.shape[0] != 8:
        print('not match')


def main():
    conn_list = [
        'mssql_1',
        'pgsql_1',
        'mysql_1'
    ]
    for conn in conn_list:
        clean()
        test_data_source(SERVERS[conn])
        clean()

    test_data_source_e()


if __name__ == '__main__':
    main()
