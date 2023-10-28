
import os

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


def div_list(listTemp, n):
    for i in range(0, len(listTemp), n):
        yield listTemp[i:i + n]

def list2str(list_in,with_single_quote=1):
    if with_single_quote ==1:
        tmp=["'"+str(x)+"'" for x in list_in]
    else:
        tmp=[str(x) for x in list_in]
    list_out=','.join(tmp)
    return list_out