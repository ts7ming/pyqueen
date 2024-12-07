import os
import xml.etree.cElementTree as ET


class FineReport:
    @staticmethod
    def __is_fr_file(path):
        if '.cpt' in path or '.frm' in path:
            return True
        else:
            return False

    def __get_fr_list(self, file_list, par_dir, cur_item):
        full_path = os.path.join(par_dir, cur_item)
        if self.__is_fr_file(cur_item) and os.path.isfile(full_path):
            file_list.append([cur_item, full_path])
        elif os.path.isdir(full_path):
            for item in os.listdir(full_path):
                self.__get_fr_list(file_list, full_path, item)
        else:
            pass
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
                fr_sql_tb.append({
                    'fr_path': fr_path,
                    'server': server,
                    'fr_dataset': fr_dataset,
                    'sql': sql
                })
        return fr_sql_tb
