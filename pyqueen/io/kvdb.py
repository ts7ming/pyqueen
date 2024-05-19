

class A:
    def __init__(self):
        pass

    def aa(self,ac):
        print('wahredtrv')

    def bb(self):
        self.aa(4)
        print('bbb is done')


class C(A):
    def __init__(self):
        super().__init__()

    def aa(self,ac):
        print('sub  aaaaa')



cc = C()

cc.bb()


