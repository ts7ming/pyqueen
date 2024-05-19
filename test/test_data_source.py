from pyqueen.io.data_source import DataSource


def mysql():
    sql1 = 'DROP TABLE IF EXISTS cdc_source.t_table'
    sql = '''
        CREATE TABLE cdc_source.t_table (
            id INT NOT NULL,
            name varchar(100) NULL
        )
    '''
    ds = DataSource(conn_type='mysql', host='olympus', username='root', password='54maqiming', port=3306, db_name='cdc_source')
    ds.exe_sql([sql1,sql])
    print('exe_sql is ok')

    import pandas as pd

    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    ds.to_db(df, tb_name='t_table',fast_load=True)
    print('to_db is ok')

    v = ds.get_sql('select count(1) as v from t_table')
    if int(v.values[0][0]) == 4:
        print('read_sql is ok')
    else:
        print('read_sql not match')

def postgersql():
    sql1 = 'DROP TABLE IF EXISTS cdc_source.t_table'
    sql = '''
        CREATE TABLE cdc_source.t_table (
            id INT NOT NULL,
            name varchar(100) NULL
        )
    '''
    ds = DataSource(conn_type='mysql', host='olympus', username='root', password='54maqiming', port=3306, db_name='cdc_source')
    ds.exe_sql([sql1,sql])
    print('exe_sql is ok')

    import pandas as pd

    df = pd.DataFrame({'id': [1, 2, 3, 4], 'name': ['libnl', 'agds', 'gfrt', 'hhg']})
    ds.to_db(df, tb_name='t_table',fast_load=True)
    print('to_db is ok')

    v = ds.get_sql('select count(1) as v from t_table')
    if int(v.values[0][0]) == 4:
        print('read_sql is ok')
    else:
        print('read_sql not match')


if __name__ == '__main__':
    mysql()