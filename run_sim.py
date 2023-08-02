# -*- coding: UTF-8 -*-

import csv
import re
import sys
import types
import time
import math
import argparse
import copy
import os

import utils.util01 as util01
import utils.flags01 as flags01
import client.jobs as jobs
import server.cluster as cluster
import utils.log01 as log01
from scheduler import *

FLAGS = flags01.FLAGS01

# prepare JOBS list
JOBS01 = jobs.JOBS01
CLUSTER01 = cluster.CLUSTER01
LOG01 = log01.LOG01


# parse input arguments
# 定义了一些命令行参数，以及默认值
flags01.DEFINE_string('trace_file', 'tf_job.csv',
                      '''Provide TF job trace file (*.csv, *.txt).
                      *.csv file, use \',\' as delimiter;
                      *.txt file, user \' \' as deliminter.
                      Default file is tf_job.csv ''')
flags01.DEFINE_string('log_path',
                      'result-' + time.strftime("%Y%m%d-%H-%M-%S",
                                                time.localtime()),
                      '''Simulation output folder,
                      including cluster/node/gpu usage trace,
                      pending job_queue info.
                      Default folder is result-[time]''')
flags01.DEFINE_string('scheme', 'yarn',
                      '''
                      Job placement scheme:
                      0.count, just resource counting,
                      without assignment (which gpu, which cpu)
                      1.yarn, ms yarn
                      2.random
                      Default is yarn''')
flags01.DEFINE_string('schedule', 'fifo',
                      '''
                      Job schedule scheme:
                      1.fifo
                      2.fjf, fit job first( in fifo order)
                      3.sjf, smallest job first
                      4.lpjf, longest pending job first
                      5.shortest, shortest-remaining-time job first
                      6.shortest-gpu, shortest-remaining-gputime job first
                      7.dlas, discretized las
                      8.dlas-gpu, dlas using gpu time
                      Default is fifo''')

flags01.DEFINE_integer('num_switch', 1,
                       '''Part of cluster spec:
                       the number of switches in this cluster,
                       default is 1''')

flags01.DEFINE_integer('num_node_p_switch', 32,
                       '''Part of cluster spec:
                       the number of nodes under a single switch,
                       default is 32''')

flags01.DEFINE_integer('num_gpu_p_node', 8,
                       '''Part of cluster spec:
                       the number of gpus on each node,
                       default is 8''')

flags01.DEFINE_integer('num_cpu_p_node', 64,
                       '''Part of cluster spec:
                       the number of cpus on each node,
                       default is 64''')

flags01.DEFINE_integer('mem_p_node', 256,
                       '''Part of cluster spec:
                       memory capacity on each node,
                       default is 128''')

flags01.DEFINE_string('cluster_spec', None,
                      '''Part of cluster spec: cluster infra spec file,
                      this file will overwrite the specs from num_switch,
                      num_node_p_switch, and num_gpu_p_node
                      Spec format:
                      num_switch,num_node_p_switch,num_gpu_p_node
                      int,int,int''')

flags01.DEFINE_boolean('print', False,
                       '''Enable print out information,
                       default is False''')

flags01.DEFINE_float('starve', 0,
                       '''for dlas algorithm''')

flags01.DEFINE_boolean('flush_stdout', True,
                       '''Flush stdout,
                       default is True''')

flags01.DEFINE_version('0.1')


def parse_job_file01(trace_file):
    util01.print_fn('---------- 进入parse_job_file01(trace_file)函数-----------------')  # debug
    # --trace_file=input/60_job.csv
    # check trace_file is *.csv 
    fd = open(trace_file, 'r') 
    deli = ','
    if ((trace_file.find('.csv') == (len(trace_file) - 4))):
        deli = ','
    elif ((trace_file.find('.txt') == (len(trace_file) - 4))):
        deli = ' '

    # csv.DictReader （，）（，）根据csv第一行 每个单词作为key， 然后每一行 同一列 作为value， 每一行都有一组dict
    reader = csv.DictReader(fd, delimiter=deli)
    ''' Add job from job trace file'''
    keys = reader.fieldnames
    util01.print_fn('---------- Read TF jobs from 从以下输入文档阅读: %s-------------------------'
                    % trace_file)

    util01.print_fn('---------- we get the following fields 读取60_job.csv的抬头/keys:\n----------%s----------' % keys)
    job_idx = 0
    for row in reader:
        # add job into JOBS
        # 把--trace_file=input/60_job.csv 中的job文件写入到jobs中，
        JOBS01.add_job01(row)
        # JOBS.read_job_info(job_idx, 'num_gpu')
        job_idx += 1
    
    # print("network:")  # debug
    # for j in JOBS01.job_list :
    #     print(j['job_id'], j['ps_network'], j['w_network'])  # debug

    # python中的assert命令通常在代码调试中会被使用，它用来判断紧跟着的代码的正确性，
    # 如果满足条件(正确)，万事大吉，程序自动向后执行，
    # 如果不满足条件(错误)，会中断当前程序并产生一个AssertionError错误。
    assert job_idx == len(JOBS01.job_list) # 前面的for循环结束以后当前idx肯定就等于length。不对的话就要触发assert
    assert JOBS01.num_job == len(JOBS01.job_list)
    # JOBS01.print_all_job_size_info()  # 可注释 debug

    JOBS01.sort_all_jobs()

    # print(lp.prepare_job_info(JOBS.job_list[0]))
    util01.print_fn('---------- Get %d TF jobs in total； 把所有jobs都从60_job.csv里面读出来了 ----------------------------------'
                    % job_idx)
    # JOBS01.read_all_jobs()  # 可注释 debug
    fd.close()
    util01.print_fn('---------- 离开parse_job_file01(trace_file)函数-----------------')  # debug

def cal_r_gittins_index(job_data, a):
    '''
    a means attained-service to that job
    gittins_index = P/E
    r_gi = E/P
    详见SOAP论文p7
    '''
    ut_delta = JOBS01.gittins_delta

    data = job_data['data']
    # print("gittins_data:   ", data)  # debug
    if a > (job_data['data'][-1] - 1):
        return 0.0
    else:
        idx = next(x[0] for x in enumerate(data) if x[1] > a)

    # 原始：
    next_a = a + ut_delta
    if next_a > (job_data['data'][-1] - 1):
        idx_delta = job_data['num'] - 1
    else:
        idx_delta = next(x[0] for x in enumerate(data) if x[1] > next_a)
    # print(idx, idx_delta)

    p = round(((idx_delta - idx) * 1.0) / (job_data['num'] - idx), 5)

    e_sum = sum(data[idx: idx_delta]) + (ut_delta * (job_data['num'] - idx_delta))
    e = round(e_sum / (job_data['num'] - idx), 5)

    # rank of gittins index = 1/gi
    # r_gi = round(e / p, 4)
    r_gi = round(p * 1000000 / e, 4)

    # idx_delta = idx + 1
    # r_gi_max = 0
    # while idx_delta <= job_data['num'] - 1:
    #     p = round(((idx_delta - idx) * 1.0) / (job_data['num'] - idx), 5)

    #     e_sum = sum(data[idx: idx_delta]) + (ut_delta * (job_data['num'] - idx_delta))
    #     e = round(e_sum / (job_data['num'] - idx), 5)

    #     # rank of gittins index = 1/gi
    #     # r_gi = round(e / p, 4)
    #     r_gi = round(p * 1000000 / e, 4)   

    #     if r_gi > r_gi_max:
    #         r_gi_max = r_gi     

    #     idx_delta += 1

    # print(idx, idx_delta, p, e_sum, e, r_gi)
    return r_gi


def parse_job_dist():
    job_dist_file = os.path.join(os.getcwd(), './input/input01/yarn-gput1000.csv')
    fd = open(job_dist_file, 'r')
    reader = csv.DictReader(fd, delimiter=',')
    durations = list()
    for row in reader:
        durations.append(int(row['duration']))
    fd.close()
    total_len = len(durations)
    durations.sort()
    print("  %s samples are learned" % total_len)

    job_dict = dict()
    job_dict['num'] = total_len
    job_dict['data'] = durations
    # print("job_dict:  ", job_dict)  # debug

    gi = list()
    for v in job_dict['data']:  # v是 121,122那些数
        gi.append(cal_r_gittins_index(job_dict, int(v - 1)))

    # print(gi)
    job_dict['data'].append(sys.maxsize)
    gi.append(0.0)
    job_dict['gittins'] = gi
    print("job_dict:  ", job_dict)  # debug

    return job_dict


def parse_cluster_spec01():
    print('---------- 进入parse_cluster_spec01()函数')  # debug
    # 分析n32g4.csv文件 --cluster_spec=n32g4.csv
    if FLAGS.cluster_spec:
        print('---------- FLAGS01.cluster_spec为',FLAGS.cluster_spec)  # debug
        spec_file = FLAGS.cluster_spec
        fd = open(spec_file, 'r')
        deli = ','
        if ((spec_file.find('.csv') == (len(spec_file) - 4))):
            deli = ','
        elif ((spec_file.find('.txt') == (len(spec_file) - 4))):
            deli = ' '
        reader = csv.DictReader(fd, delimiter=deli)
        keys = reader.fieldnames
        util01.print_fn(keys)  # debug
        if 'num_switch' not in keys:
            return
        if 'num_node_p_switch' not in keys:
            return
        if 'num_gpu_p_node' not in keys:
            return
        if 'num_cpu_p_node' not in keys:
            return
        if 'mem_p_node' not in keys:
            return

        ''' there should be only one line remaining'''
        assert reader.line_num == 1

        ''' get cluster spec '''
        for row in reader:
            # util.print_fn('num_switch %s' % row['num_switch'])
            FLAGS.num_switch = int(row['num_switch'])
            FLAGS.num_node_p_switch = int(row['num_node_p_switch'])
            FLAGS.num_gpu_p_node = int(row['num_gpu_p_node'])
            FLAGS.num_cpu_p_node = int(row['num_cpu_p_node'])
            FLAGS.mem_p_node = int(row['mem_p_node'])
        fd.close()

    util01.print_fn("num_switch: %d" % FLAGS.num_switch)  # debug
    util01.print_fn("num_node_p_switch: %d" % FLAGS.num_node_p_switch)  # debug
    util01.print_fn("num_gpu_p_node: %d" % FLAGS.num_gpu_p_node)  # debug
    util01.print_fn("num_cpu_p_node: %d" % FLAGS.num_cpu_p_node)  # debug
    util01.print_fn("mem_p_node: %d" % FLAGS.mem_p_node)  # debug

    '''init infra'''
    CLUSTER01.init_infra()
    # util.print_fn(lp.prepare_cluster_info())
    util01.print_fn('---------- End of cluster spec ---------------------------------')
    print('---------- 离开parse_cluster_spec01()函数')  # debug
    return

def summary_all_jobs():
    # assert 断言：
    # 当表达式为真时，程序继续往下执行，只是判断，不做任何处理；
    # 当表达式为假时，抛出AssertionError错误，并将 [参数] 输出
    assert all([job['status'] == 'END' for job in JOBS01.job_list])
    num_job = 1.0 * len(JOBS01.job_list)
    jct = 0
    
    last_end = 0
    first_submit = sys.maxsize
    for job in JOBS01.job_list:
        jct += (job['end_time'] - job['submit_time']) / num_job
        if job['end_time']> last_end:
            last_end = job['end_time']
        if job['submit_time'] < first_submit:
            first_submit = job['submit_time']
    makespan = last_end - first_submit
    print('average jct of scheduler %s is %d'%(FLAGS.schedule,  jct))
    print('makespan of scheduler %s is %d'%(FLAGS.schedule,  makespan))


def main():
    # 暂时用不到，不测试dlas
    # if FLAGS.schedule == 'multi-dlas-gpu':
    #     if FLAGS.scheme != 'count':
    #         util01.print_fn("In Main, multi-dlas-gpu without count")
    #         exit()

    ''' Parse input'''
    # 分析输入的60_job.csv，  --trace_file=input/60_job.csv
    parse_job_file01(FLAGS.trace_file)
    
    # 分析输入的n32g4.csv文件 --cluster_spec=n32g4.csv
    parse_cluster_spec01()

    ''' prepare logging '''
    # 准备日志文件，也就是./output/test_1这个文件夹里面的6个csv文件
    LOG01.init_log()

    ''' Prepare jobs'''
    JOBS01.prepare_job_start_events01()

    # sim_job_events()
    if FLAGS.schedule == 'fifo':
        fifo.one_queue_fifo_simulator(JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'fjf':
        fjf.fit_first_simulator(JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'sjf':
        sjf.smallest_first_sim_jobs01(False,JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'lpjf':
        lpjf.longest_pending_first_sim_job1(JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'shortest':
        shortest.shortest_first_sim_jobs01(False,JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'shortest-gpu':
        shortest.shortest_first_sim_jobs01(True,JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'shortest-expected':
        JOBS01.job_dist_data = parse_job_dist()
        shortest.shortest_first_sim_jobs01(False,JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'dlas-gpu':
        dlas.dlas_sim_jobs(True,FLAGS.starve,JOBS01,CLUSTER01,LOG01,FLAGS)
    elif FLAGS.schedule == 'dlas-gpu-gittins':
        JOBS01.job_dist_data = parse_job_dist()
        dlas.dlas_sim_jobs(True,FLAGS.starve,JOBS01,CLUSTER01,LOG01,FLAGS)
    # elif FLAGS.schedule == 'gandiva':
    #     CLUSTER01.init_gandiva_nodes()
    #     gandiva.gandiva_sim_jobs(True, 1000,JOBS01,CLUSTER01,LOG01,FLAGS)
    else:
        fifo.one_queue_fifo_sim_jobs01(JOBS01,CLUSTER01,LOG01,FLAGS)

    summary_all_jobs()

if __name__ == '__main__':
    main()
