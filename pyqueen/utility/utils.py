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
    def sql2table(sql_str):
        strip = ['[', ']', '(', ')', '\n', '`']
        for s in strip:
            sql_str = sql_str.replace(s, ' ')
        for tmp in re.findall(r'[a-zA-Z]`[a-zA-Z]', sql_str):
            sql_str = sql_str.replace(str(tmp), str(tmp).replace('`', ' '))
        q = re.sub(r"/\*[^*]*\*+(?:[^*/][^*]*\*+)*/", "", sql_str)
        lines = [line for line in q.splitlines() if not re.match("^\s*(--|#)", line)]
        q = " ".join([re.split("--|#", line)[0] for line in lines])
        tokens = re.split(r"[\s)(;]+", q)
        result = []
        get_next = False
        for token in tokens:
            if get_next:
                if token.lower() not in ["", "select"]:
                    result.append(token.replace('`', ''))
                get_next = False
            get_next = token.lower() in ['from', 'join', 'truncate table', 'into', 'delete from', 'update']
        return result

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
    def exec(q, func, index, args):
        result = func(args)
        q.put([index, result])

    @staticmethod
    def mult_run(func, args_list, max_process=1):
        import multiprocessing
        q = multiprocessing.Queue()
        args_list_with_index = []
        i = 0
        for t in args_list:
            args_list_with_index.append([i, t])
            i += 1
        task_list = Utils.div_list(args_list_with_index, max_process)
        for sub_args_list in task_list:
            job_list = []
            for index, args in sub_args_list:
                p = multiprocessing.Process(target=Utils.exec, args=(q, func, index, args))
                p.start()
                job_list.append(p)
            for job in job_list:
                job.join()
        result = []
        for _ in range(q.qsize()):
            result.append(q.get())
        sorted(result, key=(lambda x: x[0]))
        result = [x[1] for x in result]
        return result

    @staticmethod
    def html2text(html):
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, 'html.parser')
        text = soup.get_text()
        return text
