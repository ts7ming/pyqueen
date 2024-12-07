import os
import shutil
import zipfile
from pyqueen import Utils
import xml.etree.cElementTree as ET


class FineReport:
    @staticmethod
    def __is_fr_file(path):
        if '.cpt' in path or '.frm' in path:
            return True
        else:
            return False

    def __search_file(self, sub_dir_name, sub_dir_path):
        """
        递归搜索帆软模板文件, 复制到目标路径下
        """
        if '.cpt' in sub_dir_path:
            target_path = str(sub_dir_path).replace(sub_dir_name + '\\', '')
            target_dir_name = '/'.join(target_path.split('\\')[0:-1])
            print(sub_dir_path, target_dir_name)
            if not os.path.exists(target_dir_name):
                os.makedirs(target_dir_name)
            shutil.copy(sub_dir_path, target_path)
        else:
            for sub_dir_name2 in os.listdir(sub_dir_path):
                sub_dir_path2 = os.path.join(sub_dir_path, sub_dir_name2)
                self.__search_file(sub_dir_name2, sub_dir_path2)

    def extract_file(self, fr_zip_path, target_path):
        """
        提取文件结构
        """
        with zipfile.ZipFile(fr_zip_path, 'r') as zip_ref:
            zip_ref.extractall(target_path)

        for sub_dir_name in os.listdir(target_path):
            sub_dir_path = os.path.join(target_path, sub_dir_name)
            self.__search_file(sub_dir_name, sub_dir_path)

    def __get_fr_list(self, file_list, par_dir, cur_item):
        full_path = os.path.join(par_dir, cur_item)
        if self.__is_fr_file(cur_item):
            file_list.append([cur_item, full_path])
        else:
            for item in os.listdir(full_path):
                self.__get_fr_list(file_list, full_path, item)
        return file_list

    @staticmethod
    def __read_xml(file_path):
        tree = ET.parse(file_path)
        info = []
        for elem in tree.iter():
            if elem.tag == 'TableData':
                try:
                    server = str(elem.find('Connection').find('DatabaseName').text).replace('\n', '')
                except:
                    continue
                try:
                    fr_dataset = str(elem.attrib['name']).replace('\n', '')
                except:
                    fr_dataset = ''
                for sql_elem in elem:
                    if sql_elem.tag == 'Query':
                        sql_text = sql_elem.text
                        info.append([server, fr_dataset, sql_text])
        return info

    def extract_sql(self, fr_dir_path):
        file_list = []
        for item in os.listdir(fr_dir_path):
            self.__get_fr_list(file_list, fr_dir_path, item)

        fr_sql_tb = []
        for fr_name, fr_path in file_list:
            sql_list = self.__read_xml(fr_path)
            for server, fr_dataset, sql in sql_list:
                temp_list = []
                for tb in Utils.sql2table(sql):
                    if '.' in tb and str(tb).count('.') == 1:
                        db_name = str(tb).split('.')[0]
                        table_name = str(tb).split('.')[1]
                    elif '.' in tb and str(tb).count('.') == 2:
                        db_name = str(tb).split('.')[0]
                        table_name = str(tb).split('.')[2]
                    else:
                        db_name = ''
                        table_name = tb
                    temp_list.append({
                        'fr_path': fr_path,
                        'server': server,
                        'db_name': db_name,
                        'table_name': table_name,
                        'fr_dataset': fr_dataset,
                        'sql': sql
                    })
                fr_sql_tb.extend(temp_list)
        return fr_sql_tb
