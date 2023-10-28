import os
import sys
import numpy as np
import pandas as pd
import hashlib
import itertools

class Table(object):
    @staticmethod
    def cartesian_product(input_sets):
        if type(input_sets) is pd.DataFrame:
            t_input_sets = {}
            for col in input_sets.columns:
                t_input_sets[col] = list(set(input_sets[col].to_list()))
            input_sets = t_input_sets
        df_out = None
        field_list = []
        for col_name, col_value in input_sets.items():
            col_value = list(set(col_value))
            field_list.append(col_name)
            if df_out is None:
                df_out = pd.DataFrame({col_name: col_value, 'tmp': 1})
            else:
                df_tmp = pd.DataFrame({col_name: col_value, 'tmp': 1})
                df_out = pd.merge(df_out, df_tmp, on='tmp', how='outer')
        df_out = df_out[field_list]
        return df_out

    @staticmethod
    def pivot(df, index, columns, values, sep='-'):
        fd_list_all = []
        for item in [index, columns, values]:
            if type(item) is list:
                fd_list_all.extend(item)
            else:
                fd_list_all.append(item)
        df = df[fd_list_all]
        df_pivot = pd.pivot_table(data=df, index=index, columns=columns, values=values).reset_index()

        if type(columns) is list and len(columns) > 1:
            fd_list_fix = []
            for fd in df_pivot.columns.ravel():
                if fd[-1] == '':
                    fd_list_fix.append(fd[0])
                else:
                    fd_list_fix.append(sep.join(fd))
            df_pivot.columns = fd_list_fix
        return df_pivot

    @staticmethod
    def table_join(df_master, df_list):
        for df_tmp in df_list:
            k_left = set(df_master.columns)
            k_right = set(df_tmp.columns)
            join_key = list(k_left & k_right)
            df_master = pd.merge(df_master, df_tmp, on=join_key, how='left')
        return df_master

    @staticmethod
    def col_sum(df, include_col=None, exclude_col=None, col_name='columns_sum', insert_at=None, insert_after=None,
                insert_before=None):
        fd_list = list(df.columns).copy()
        df[col_name] = 0
        if include_col is None:
            for col in fd_list:
                if col not in exclude_col:
                    df[col_name] = df[col_name] + df[col].astype(float)
        else:
            for col in fd_list:
                if col in include_col:
                    df[col_name] = df[col_name] + df[col].astype(float)
        if insert_at is not None:
            fd_list.insert(insert_at, col_name)
        elif insert_after is not None:
            insert_aft = fd_list.index(insert_after)
            insert_after = insert_aft - 1
            fd_list.insert(insert_aft, col_name)
        elif insert_before is not None:
            insert_bef = fd_list.index(insert_before)
            fd_list.insert(insert_bef, col_name)
        else:
            fd_list.append(col_name)
        df = df[fd_list]
        return df

    @staticmethod
    def print(df, N=5):
        pd.set_option('display.max_columns', 100)
        pd.set_option('display.unicode.east_asian_width', True)
        print(df.head(N))

    @staticmethod
    def combinations(columns_list):
        column_group = []
        for i in range(1, len(columns_list) + 1):
            for x in itertools.combinations(columns_list, i):
                column_group.append(list(x))
        return column_group

    def __md5_str(self,tmp_str):
        m = hashlib.md5()
        str1s = str(tmp_str).encode(encoding='utf-8')
        m.update(str1s)
        md5_str = m.hexdigest()
        return md5_str

    def add_md5(self,df, col_name='md5', col_list='all'):
        col_name = str(col_name)
        if str(col_list) == 'all':
            col_list = list(df.index)
        tmp_df=df[col_list].copy()
        data=tmp_df.values.tolist()
        md5=[]
        for row in data:
            tmp=''
            for fd in row:
                tmp=tmp+str(fd)
            tmp=self.__md5_str(tmp)
            md5.append(tmp)
        df[col_name]=md5
        return df