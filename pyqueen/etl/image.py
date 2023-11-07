import pandas as pd


class Image:
    @staticmethod
    def df2image(df, file_path=None, col_width=None, font_size=None):
        """
        基于 pandas.DataFrame 生成png图片

        :param font_size: 字体大小
        :param col_width: 列宽: auto: 根据传入 df 自动设置 也可以传入列表指定每列宽度 由plt自动设置,
        :param df: pd.DataFrame对象
        :param file_path: 目标图片路径, 如果为None则自动生成临时路径
        :return:
        """
        import matplotlib.pylab as plt
        import tempfile

        if file_path is None:
            file_path = tempfile.gettempdir() + '/tmp.png'
        pd.set_option('display.unicode.ambiguous_as_wide', True)
        pd.set_option('display.unicode.east_asian_width', True)

        plt.figure()
        plt.rcParams['font.sans-serif'] = ['SimHei']  # 用来正常显示中文标签
        plt.subplots_adjust(top=0.7, bottom=0, left=0, right=1, hspace=3, wspace=3)
        plt.margins(1, 1)
        ax = plt.subplot(111, 1, 1)
        ax.xaxis.set_visible(False)
        ax.yaxis.set_visible(False)

        if col_width == 'auto':
            tmp_list = [max(len(str(a)), len(str(b))) for a, b in
                        zip(list(df.head(1).to_records()[0])[1:], list(df.columns))]
            new_col_width = [x / sum(tmp_list) for x in tmp_list]
            dtable = ax.table(cellText=df.values, colLabels=df.columns, colWidths=new_col_width)
        elif col_width != 'auto' and col_width is not None:
            dtable = ax.table(cellText=df.values, colLabels=df.columns, colWidths=col_width)
        else:
            dtable = ax.table(cellText=df.values, colLabels=df.columns)
        if font_size is not None:
            dtable.auto_set_font_size(False)
            dtable.set_fontsize(font_size)
        plt.savefig(file_path, dpi=600, bbox_inches='tight')
        return file_path
