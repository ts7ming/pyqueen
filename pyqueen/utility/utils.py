import os
import hashlib
import re


class Utils:
    @staticmethod
    def delete_file(path):
        try:
            ls = os.listdir(path)
            for i in ls:
                c_path = os.path.join(path, i)
                try:
                    os.remove(c_path)
                except:
                    print(c_path)
        except:
            pass

    @staticmethod
    def div_list(listTemp, n):
        for i in range(0, len(listTemp), n):
            yield listTemp[i:i + n]

    @staticmethod
    def list2str(list_in, with_single_quote=1):
        if with_single_quote == 1:
            tmp = ["'" + str(x) + "'" for x in list_in]
        else:
            tmp = [str(x) for x in list_in]
        list_out = ','.join(tmp)
        return list_out

    @staticmethod
    def md5(text):
        m = hashlib.md5()
        str1s = str(text).encode(encoding='utf-8')
        m.update(str1s)
        md5_str = m.hexdigest()
        return md5_str

    @staticmethod
    def unzip(from_path, to_path):
        import zipfile
        try:
            zip_file = zipfile.ZipFile(from_path)
            if zipfile.is_zipfile(from_path):
                zip_file.extractall(to_path)
                zip_file.close()
        except Exception as ee:
            raise Exception('解压文件失败：%s' % ee)

    @staticmethod
    def zip(from_path, to_path):
        import zipfile
        try:
            filepath, tmpfilename = os.path.split(from_path)
            file_type = tmpfilename.split('.')
            name = file_type[0]
            if len(file_type) == 1:
                zip_file = zipfile.ZipFile(to_path, 'a', zipfile.ZIP_DEFLATED)
                for root, dirs, files in os.walk(from_path):
                    path = root.replace(from_path, '')
                    for file in files:
                        zip_file.write(os.path.join(root, file),
                                       os.path.join(name, path, file))
                zip_file.close()
            else:
                zip_file = zipfile.ZipFile(to_path, 'w', zipfile.ZIP_DEFLATED)
                zip_file.write(from_path, tmpfilename)
                zip_file.close()
        except Exception as ee:
            raise Exception('压缩失败：%s' % ee)

    @staticmethod
    def sql2table(sql_text, kw=None, strip=None):
        if kw is None:
            kw = ['from', 'join', 'truncate table', 'into', 'delete from', 'update']
        if strip is None:
            strip = [' ', '[', ']', '.', '(', ')', '\n', '`']
        table_list = []
        for mf in kw:
            p = mf + '\s+[0-9a-zA-Z_\[\]\.]+[\s\(]+'
            if re.findall(p, sql_text, re.IGNORECASE) is None:
                continue
            for tmp in re.findall(p, sql_text, re.IGNORECASE):
                tmp = tmp.lower().replace(mf, '')
                for r in strip:
                    tmp = tmp.replace(r, '')
                if len(tmp) == 0:
                    continue
                if tmp[0] == '#' or tmp[0:1] == '--':
                    continue
                if tmp not in table_list:
                    table_list.append(tmp)
        return table_list

    @staticmethod
    def detect_encoding(file_path):
        import chardet
        with open(file_path, 'rb') as file:
            data = file.read()
            result = chardet.detect(data)
            encoding = result['encoding']
            confidence = result['confidence']
        return encoding, confidence

    @staticmethod
    def exec(q, func, args):
        result = func(args)
        q.put(result)

    @staticmethod
    def mult_run(func, args_list):
        import multiprocessing
        job_list = []
        q = multiprocessing.Queue()
        for args in args_list:
            p = multiprocessing.Process(target=Utils.exec, args=(q, func, args))
            p.start()
            job_list.append(p)
        for job in job_list:
            job.join()
        result = []
        for _ in range(q.qsize()):
            result.append(q.get())
        return result
