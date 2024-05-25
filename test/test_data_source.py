import pandas as pd

from pyqueen import DataSource


def test_mysql():
    ds = DataSource(conn_type='mysql', host='172.23.213.221', username='root', password='qiming', port=3306, db_name='demo')

    sql = 'DROP TABLE IF EXISTS t_table'
    ds.exe_sql(sql)

    sql = '''
        CREATE TABLE t_table (
            id INT NOT NULL,
            name varchar(100) NULL
        )
    '''
    ds.exe_sql(sql)
    print('mysql exe_sql is ok')

    import pandas as pd

    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    ds.to_db(df, tb_name='t_table', fast_load=False)
    print('mysql to_db is ok')

    v = ds.get_sql('select count(1) as v from t_table')
    if int(v.values[0][0]) == 4:
        print('mysql read_sql is ok')
    else:
        print('mysql read_sql not match')

def test_mysql2():
    ds = DataSource(conn_type='mysql', host='olympus', username='root', password='54maqiming', port=3306, db_name='cdc_source')

    sql = 'DROP TABLE IF EXISTS t_table'
    ds.exe_sql(sql)

    sql = '''
        CREATE TABLE t_table (
            id INT NOT NULL,
            name varchar(100) NULL
        )
    '''
    ds.exe_sql(sql)
    print('mysql exe_sql is ok')

    import pandas as pd

    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    ds.to_db(df, tb_name='t_table', fast_load=False)
    print('mysql to_db is ok')

    v = ds.get_sql('select count(1) as v from t_table')
    if int(v.values[0][0]) == 4:
        print('mysql read_sql is ok')
    else:
        print('mysql read_sql not match')


def test_postgresql():
    ds = DataSource(conn_type='pgsql', host='localhost', username='postgres', password='1qaz2wsx!', port=5432, db_name='postgres')

    sql = 'DROP TABLE IF EXISTS t_table_pg'
    ds.exe_sql(sql)

    sql = '''
        CREATE TABLE t_table_pg (
            id INT NOT NULL,
            name varchar(100) NULL
        )
    '''
    ds.exe_sql(sql)
    print('pgsql exe_sql is ok')

    import pandas as pd

    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    ds.to_db(df, tb_name='t_table_pg')
    print('pgsql to_db is ok')

    v = ds.get_sql('select count(1) as v from t_table_pg')
    v2 = ds.row_count(table_name='t_table_pg')
    v3 = ds.ge
    if int(v.values[0][0]) == 4 and int(v2) == 4:
        print('pgsql read_sql is ok')
    else:
        print('pgsql read_sql not match')


def test_redis():
    ds = DataSource(conn_type='redis', host='172.23.213.221', port='6379', db_name=0)
    ds.set_logger(logger=print)
    ds.set_v('ff', 11)
    ds.set_v('中文k', '中文v')
    if int(ds.get_v('ff')) == 11:
        print('redis get is ok')
    else:
        print('redis get  not match')
    if ds.get_v('中文k') == '中文v':
        print('redis get中文 is ok')
    else:
        print('redis get  not match')


def test_sqlite():
    ds = DataSource(conn_type='sqlite', file_path='tst.db')
    import pandas as pd

    df = pd.DataFrame({'dd': [1, 2, 3], 'gg': ['daf', 'gfytr', 'eee']})
    ds.to_db(df, tb_name='hhhhh')
    print('sqlite: to_db is ok')
    sql = 'select max(dd) as f from hhhhh'
    v = int(ds.get_value(sql))
    if v == 3:
        print('sqlite: read_sql is ok')

def test_excel():
    ds = DataSource(conn_type='excel', file_path='./tst.xlsx')
    import pandas as pd

    df = pd.DataFrame({'dd': [1, 2, 3], 'gg': ['daf', 'gfytr', 'eee']})
    ds.to_excel(sheet_list=[[df,'ma']])
    print('excel: to_excel is ok')




if __name__ == '__main__':
    # test_mysql()
    test_mysql2()
    # test_postgresql()
    # test_redis()
    test_sqlite()
    test_excel()
