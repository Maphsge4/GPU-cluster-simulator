# -*- coding: UTF-8 -*-

# from asyncio import subprocess
import subprocess
import utils.flags01 as flags01
import sys

FLAGS01 = flags01.FLAGS01


def print_fn(log):
    if FLAGS01.print:
        print(log)
        if FLAGS01.flush_stdout:
            sys.stdout.flush()


def mkdir(folder_path):
    cmd = 'mkdir -p ' + folder_path
    ret = subprocess.check_call(cmd, shell=True)
    # subprocess.check_call(args, *, stdin = None, stdout = None,
    #                       stderr = None, shell = False)
    # 与call方法类似，不同在于如果命令行执行成功，check_call返回返回码0，
    # 否则抛出subprocess.CalledProcessError异常.

    print_fn(ret)


def search_dict_list(dict_list, key, value):
    '''
    Search the targeted <key, value> in the dict_list
    Return:
        list entry, or just None
    '''
    for e in dict_list:
        # if e.has_key(key) == True:
        if key in e:
            if e[key] is value:
                return e
    return None
