'''
Allocate job resource
'''

from placement import *

def try_get_job_res(job,FLAGS01,CLUSTER01):
    print('---------- 进入try_get_job_res(job)')  # debug
    print('---------- FLAGS01.scheme=', FLAGS01.scheme)  # debug 发现这儿用到了scheme
    '''
    select placement scheme
    '''
    # yarn方式会把placement的具体方式写入job变量，从而在释放资源时需要具体释放
    # greedy,count不会记录placement方式
    if FLAGS01.scheme == 'yarn':
        ret = yarn.ms_yarn_placement(job,CLUSTER01)
    elif FLAGS01.scheme == 'random':
        ret = random.random_placement(job,CLUSTER01)
    elif FLAGS01.scheme == 'count':
        ret = count.none_placement(job,CLUSTER01)
    # elif FLAGS01.scheme == 'crandom':
        # ret = crandom.consolidate_random_placement(job,CLUSTER01)
    # elif FLAGS01.scheme == 'greedy':
        # ret = greedy.greedy_placement(job)
    elif FLAGS01.scheme == 'gandiva':
        ret = gandiva.gandiva_placement(job,CLUSTER01)
    else:
        ret = yarn.ms_yarn_placement(job,CLUSTER01)
    if ret is True:
        # job['status'] = 'RUNNING'
        pass
    print('---------- 离开try_get_job_res(job)')  # debug
    return ret