import zipfile
import os


class Zip:
    """
    压缩&解压
    """
    def __init__(self, from_path, to_path):
        """
        初始化
        :param from_path: 输入路径
        :param to_path: 输出路径
        """
        self.from_path = from_path
        self.to_path = to_path

    def unzip(self):
        """
        解压
        """
        try:
            zip_file = zipfile.ZipFile(self.from_path)
            if zipfile.is_zipfile(self.from_path):
                zip_file.extractall(self.to_path)
                zip_file.close()
        except Exception as ee:
            raise Exception('解压文件失败：%s' % ee)

    def pack(self):
        """
        压缩
        """
        try:
            filepath, tmpfilename = os.path.split(self.from_path)
            file_type = tmpfilename.split('.')
            name = file_type[0]
            if len(file_type) == 1:
                zip_file = zipfile.ZipFile(self.to_path, 'a',
                                           zipfile.ZIP_DEFLATED)
                for root, dirs, files in os.walk(self.from_path):
                    path = root.replace(self.from_path, '')
                    for file in files:
                        zip_file.write(os.path.join(root, file),
                                       os.path.join(name, path, file))
                zip_file.close()
            else:
                zip_file = zipfile.ZipFile(self.to_path, 'w',
                                           zipfile.ZIP_DEFLATED)
                zip_file.write(self.from_path, tmpfilename)
                zip_file.close()
        except Exception as ee:
            raise Exception('压缩失败：%s' % ee)
