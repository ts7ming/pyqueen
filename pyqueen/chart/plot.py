try:
    import matplotlib.pyplot as plt

    plt.rcParams['font.sans-serif'] = ['SimHei']
    plt.rcParams['axes.unicode_minus'] = False
except:
    pass


class Chart:

    @staticmethod
    def line(x, y, title='Line Plot', x_label='x', y_label='y', img_path=None, show=True):
        plt.plot(x, y)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if img_path is not None:
            plt.savefig(img_path)
        if show:
            plt.show()

    @staticmethod
    def bar(x, y, title='Bar Plot', x_label='x', y_label='y', img_path=None, show=True):
        plt.bar(x, y)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if img_path is not None:
            plt.savefig(img_path)
        if show:
            plt.show()

    @staticmethod
    def scatter(x, y, title='Scatter Plot', x_label='x', y_label='y', img_path=None, show=True):
        plt.scatter(x, y)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if img_path is not None:
            plt.savefig(img_path)
        if show:
            plt.show()

    @staticmethod
    def bubble(x, y, v, c=None, title='Bubble Plot', x_label='x', y_label='y', img_path=None, show=True):
        plt.scatter(x, y, s=v, c=c)
        plt.title(title)
        plt.xlabel(x_label)
        plt.ylabel(y_label)
        if img_path is not None:
            plt.savefig(img_path)
        if show:
            plt.show()
