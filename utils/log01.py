
# -*- coding: UTF-8 -*-

# from simulator.cluster import CLUSTER, FLAGS

import subprocess
import csv
import math

# from simulator.cluster import FLAGS
import utils.util01 as util01
import utils.flags01 as flags01
import server.cluster as cluster
import client.jobs as jobs

FLAGS01 = flags01.FLAGS01
CLUSTER01 = cluster.CLUSTER01
JOBS01 = jobs.JOBS01


class _Log01(object):

    def __init__(self):
        self.log_path = ''
        self.log_file = ''
        self.log_cpu = ''
        self.log_gpu = ''
        self.log_network = ''
        self.log_mem = ''
        self.log_job = ''
        self.log_list = list()
        self.cpu_list = list()
        self.gpu_list = list()
        self.network_list = list()
        self.job_list = list()
        self.mem_list = list()

    def init_log(self):
        print('---------- 进入LOG01.init_log()  \
        \n 定义output cluster.csv; job.csv; gpu.csv; network.csv; memory.csv')  # debug
        # --log_path=output/test_1
        self.log_path = FLAGS01.log_path
        if self.log_path[-1] == '/':
            self.log_path = self.log_path[:-1]
        util01.print_fn(self.log_path)
        util01.print_fn(' ')

        # prepare folder
        cmd = 'mkdir -p ' + self.log_path
        ''' python 2.7
        status, output = commands.getstatusoutput(cmd)
        '''
        # python 2.7 & 3
        ret = subprocess.check_output(cmd, shell=True)

        self.log_file = self.log_path + '/cluster.csv'
        self.log_job = self.log_path + '/job.csv'
        if FLAGS01.scheme != 'count':
            self.log_cpu = self.log_path + '/cpu.csv'
            self.log_gpu = self.log_path + '/gpu.csv'
            self.log_network = self.log_path + '/network.csv'
            self.log_mem = self.log_path + '/memory.csv'

        fd = open(self.log_file, 'w+')
        log_writer = csv.writer(fd)
        if FLAGS01.scheme == 'gandiva':
            # 0923修改 把fra_gpu 改成 free_gpu 然后改成idle_gpu
            log_writer.writerow(['time', 'idle_node', 'busy_node', 'full_node',
                                 'idle_gpu', 'busy_gpu', 'pending_job',
                                 'running_job', 'completed_job',
                                 'len_g1', 'len_g2', 'len_g4',
                                 'len_g8', 'len_g16', 'len_g32', 'len_g64'])
        else:
            log_writer.writerow(['time', 'idle_node', 'busy_node', 'full_node',
                                 'idle_gpu', 'busy_gpu', 'pending_job',
                                 'running_job', 'completed_job'])
        fd.close()

        # * ``count``: just resource counting   --scheme=yarn*
        # ``yarn``: get GPUs from the same server nodes under the same switch
        # ``random``: randomly select GPUs from the entire cluster
        if FLAGS01.scheme != 'count':
            # 打开log_cpu，写入output cpu.csv
            # 有多少个node，就有多少个cpu列
            fd = open(self.log_cpu, 'w+')
            log_writer = csv.writer(fd)
            log_writer.writerow(['time'] + ['cpu'+str(i) for i in range(CLUSTER01.num_node)])
            ''''if combine all the info together
            log_writer.writerow(['cpu'+str(i) for i in range(CLUSTER.num_node)]
                                + ['gpu'+str(i) for i in range(CLUSTER.num_node)]
                                + ['net'+str(i) for i in range(CLUSTER.num_node)])
            '''
            fd.close()
            
            # 打开log_gpu，写入output gpu.csv
            # 有多少个node，就有多少个gpu列
            fd = open(self.log_gpu, 'w+')
            log_writer = csv.writer(fd)
            log_writer.writerow(['time'] + ['gpu'+str(i) for i in range(CLUSTER01.num_node)])
            fd.close()
            
            # 打开log_network，写入output network.csv
            fd = open(self.log_network, 'w+')
            log_writer = csv.writer(fd)
            title_list = list()
            title_list.append('time')
            # 有多少个node，就有多少个in[] out[]
            for i in range(CLUSTER01.num_node):
                title_list.append('in'+str(i))
                title_list.append('out'+str(i))
            log_writer.writerow(title_list)
            # log_writer.writerow(['net'+str(i) for i in range(CLUSTER.num_node)])
            fd.close()
            
            # 打开log_mem，写入output mem.csv
            fd = open(self.log_mem, 'w+')
            log_writer = csv.writer(fd)
            # log_writer.writerow(['time'] + ['mem'+str(i) for i in range(CLUSTER.num_node)])
            log_writer.writerow(['time', 'max', '99th', '95th', 'med'])
            fd.close()

        # 打开log_job，写入output job.csv
        # 使用’w’写入模式，或者’w+'读写模式。文件不存在,则会创建文件;如果文件存在，则会将其覆盖。
        fd = open(self.log_job, 'w+')  # job.csv
        log_writer = csv.writer(fd)
        if FLAGS01.schedule == 'gpu-demands':
            log_writer.writerow(['time', '1-GPU', '2-GPU', '4-GPU',
                                 '8-GPU', '12-GPU', '16-GPU',
                                 '24-GPU', '32-GPU'])
        else:
            if FLAGS01.scheme == 'count':
                log_writer.writerow(['time', 'job_id', 'num_gpu',
                                     'submit_time', 'start_time', 'end_time',
                                     'executed_time', 'JCT', 'duration', 'pending_time',
                                     'preempt', 'resume', 'promote'])
            else:
                log_writer.writerow(['time', 'job_id', 'num_gpu',
                                     'submit_time', 'start_time','end_time', 
                                     'executed_time','JCT', 'duration', 'pending_time',
                                     'preempt', 'promote'])
        fd.close()
        print('---------- 离开LOG01.init_log()')  # debug

    def dump_all_logs(self):
        # ’a’追加写模式，和’a+'追加读写模式。文件存在，则打开该文件；文件不存在，则新建一个空白文件。
        fd = open(self.log_file, 'a+')  # cluster.csv
        log_writer = csv.writer(fd)
        for log in self.log_list:
            log_writer.writerow(log)
        fd.close()
        del self.log_list[:]

        if FLAGS01.scheme != 'count':
            fd = open(self.log_cpu, 'a+')  # cpu.csv
            log_writer = csv.writer(fd)
            for log in self.cpu_list:
                log_writer.writerow(log)
            fd.close()
            del self.cpu_list[:]

            fd = open(self.log_gpu, 'a+')  # gpu.csv
            log_writer = csv.writer(fd)
            for log in self.gpu_list:
                log_writer.writerow(log)
            fd.close()
            del self.gpu_list[:]

            fd = open(self.log_network, 'a+')  # network.csv
            log_writer = csv.writer(fd)
            for log in self.network_list:
                log_writer.writerow(log)
            fd.close()
            del self.network_list[:]

            fd = open(self.log_mem, 'a+')  # memory.csv
            log_writer = csv.writer(fd)
            for log in self.mem_list:
                log_writer.writerow(log)
            fd.close()
            del self.mem_list[:]

    def gandiva_checkpoint(self, event_time, idle_node, busy_gpu,
                           frag_gpu, pending_job, running_job, len_g1,
                           len_g2, len_g4, len_g8, len_g16, len_g32, len_g64):
        busy_node = CLUSTER01.num_node - idle_node
        full_node = 0
        idle_gpu = frag_gpu
        completed_job = len(JOBS01.completed_jobs)
        self.log_list.append([event_time, idle_node, busy_node,
                              full_node, idle_gpu, busy_gpu,
                              pending_job, running_job, completed_job,
                              len_g1, len_g2, len_g4, len_g8,
                              len_g16, len_g32, len_g64])
        if len(self.log_list) >= 1:
            self.dump_all_logs()

    def checkpoint(self, event_time):
        # print("进入log01的checkpoint函数") # debug
        '''
        记录集群、job的信息，放在每个调度算法的末尾
        Record cluster, and job information, including: 
        time
        idle_node
        busy_node: gpu running
        full_node: all gpus are running
        idle_gpu
        busy_gpu
        pending_job
        running_job
        completed_job
        '''
        idle_node = 0
        busy_node = 0
        full_node = 0
        idle_gpu = 0
        busy_gpu = 0
        pending_job = 0
        running_job = 0
        completed_job = 0

        if FLAGS01.scheme != 'count':
            # get info
            cpu = list()
            gpu = list()
            net = list()
            cpu.append(event_time)
            gpu.append(event_time)
            net.append(event_time)
            mem = list()
            mem_result = list()
            mem.append(event_time)
            for switch in CLUSTER01.switch_list:
                for node in switch.node_list:
                    free_gpu = node.check_free_gpus()
                    #updage gpu
                    idle_gpu += free_gpu
                    busy_gpu += node.num_gpu - free_gpu
                    #update node
                    if free_gpu == node.num_gpu:
                        idle_node += 1
                    elif free_gpu > 0:
                        busy_node += 1
                    elif free_gpu == 0:
                        full_node += 1

                    #cpu
                    free_cpu = node.check_free_cpus()
                    busy_cpu = node.num_cpu - free_cpu
                    b_gpu = node.num_gpu - free_gpu
                    # 记录的都是有多少busy的CPU、GPU
                    cpu.append(busy_cpu)
                    gpu.append(b_gpu)

                    #network in or out
                    net.append(node.network_in)
                    net.append(node.network_out)

                    used_mem = FLAGS01.mem_p_node - node.free_mem + 2
                    if used_mem > 2:
                        mem.append(used_mem)

            len_m = len(mem)
            if len_m == 1:
                idx_95 = 0
                idx_99 = 0
                idx_med = 0
            else:
                # Python math.ceil(x) 方法将 x 向上舍入到最接近的整数。
                idx_99 = int(math.ceil(len_m * 0.01))
                if idx_99 > (len_m - 1):
                    idx_99 = int(len_m - 1)
                idx_95 = int(math.ceil(len_m * 0.05))
                if idx_95 > (len_m - 1):
                    idx_95 = int(len_m - 1)

                idx_med = (len_m - 1) // 2
            # idx_99 = 3
            # idx_95 = 13
            # idx_med = 128
            if len_m > 0:
                rs_mem = sorted(mem, reverse=True)
                mem_result.append(event_time)
                mem_result.append(round(rs_mem[0], 1))  # max
                mem_result.append(round(rs_mem[idx_99], 1))  # max99
                mem_result.append(round(rs_mem[idx_95], 1))  # max95
                mem_result.append(round(rs_mem[idx_med], 1))  # median

        else:  # 如果scheme == count
            if FLAGS01.schedule == 'dlas-gpu-pack':
                for gpu in CLUSTER01.gpu_list:
                    if gpu == 1:
                        idle_gpu = idle_gpu + 1
                    else:
                        busy_gpu = busy_gpu + 1
            else:
                idle_gpu = CLUSTER01.free_gpu
                busy_gpu = CLUSTER01.num_gpu - CLUSTER01.free_gpu

            busy_node = int(math.ceil(busy_gpu / CLUSTER01.num_gpu_p_node))
            full_node = busy_node
            idle_node = int(CLUSTER01.num_node - busy_node)

        for job in JOBS01.job_list:
            if job['status'] == 'RUNNING':
                running_job += 1
            elif job['status'] == 'PENDING':
                pending_job += 1
            elif job['status'] == 'END':
                completed_job += 1

        # add log
        self.log_list.append([event_time, idle_node, busy_node,
                              full_node, idle_gpu, busy_gpu,
                              pending_job, running_job, completed_job])
        if FLAGS01.scheme != 'count':
            self.cpu_list.append(cpu)
            self.gpu_list.append(gpu)
            self.network_list.append(net)
            if len(mem_result) > 0:
                self.mem_list.append(mem_result)

        if len(self.log_list) >= 1:
            self.dump_all_logs()

    def checkpoint_multi_dlas_gpu(self, event_time):
        '''
        Record cluster, and job information, including:
        time
        idle_node
        busy_node: gpu running
        full_node: all gpus are running
        idle_gpu
        busy_gpu
        pending_job
        running_job
        completed_job
        '''
        idle_node = 0
        busy_node = 0
        full_node = 0
        idle_gpu = 0
        busy_gpu = 0
        pending_job = 0
        running_job = 0
        completed_job = 0

        if FLAGS01.schedule != 'multi-dlas-gpu':
            util01.print_fn("Error, not multi-dlas-gpu in checkpoint")
            exit()

        for num_gpu, gjob in JOBS01.gpu_job.items():
            idle_gpu += gjob.free_gpu

        busy_gpu = CLUSTER01.num_gpu - idle_gpu

        busy_node = int(math.ceil(busy_gpu / CLUSTER01.num_gpu_p_node))
        full_node = busy_node
        idle_node = int(CLUSTER01.num_node - busy_node)

        for job in JOBS01.job_list:
            if job['status'] == 'RUNNING':
                running_job += 1
            elif job['status'] == 'PENDING':
                pending_job += 1
            elif job['status'] == 'END':
                completed_job += 1

        # add log
        self.log_list.append([event_time, int(idle_node),
                              int(busy_node), int(full_node),
                              int(idle_gpu), int(busy_gpu),
                              int(pending_job), int(running_job),
                              int(completed_job)])
        if len(self.log_list) >= 1:
            self.dump_all_logs()

    def dump_job_logs(self):
        fd = open(self.log_job, 'a+')  # job.csv
        log_writer = csv.writer(fd)
        for log in self.job_list:
            log_writer.writerow(log)
        fd.close()
        del self.job_list[:]

    def job_complete(self, job, event_time):
        '''
        ['even_time', 'job_id', 'num_gpu', 'submit_time',
         'start_time', 'end_time', 'executed time',
         duration', 'pending_time']
        '''
        job['end_time'] = event_time
        if FLAGS01.schedule == 'gpu-demands':
            job['start_time'] = job['submit_time']
            job['status'] = 'END'
        executed_time = job['end_time'] - job['start_time']
        print("exe_time: ",executed_time)  # debug
        jct = int(job['end_time'] - job['submit_time'])
        print("jct: ", jct)  # debug
        if FLAGS01.scheme == 'count':
            self.job_list.append([event_time, job['job_id'],
                                  job['num_gpu'], job['submit_time'],
                                  job['start_time'], job['end_time'],
                                  executed_time, jct, job['duration'],
                                  job['pending_time'], job['preempt'],
                                  job['resume'], job['promote']])
        else:
            self.job_list.append([event_time, job['job_id'],
                                  job['num_gpu'], job['submit_time'],
                                  job['start_time'], job['end_time'],
                                  executed_time, jct, job['duration'],
                                  job['pending_time'], job['preempt'],
                                  job['promote']])

        if len(self.job_list) >= 1:
            self.dump_job_logs()

    def checkpoint_gpu_demands(self, event_time):
        '''
        1-GPU, 2-GPU, 4-GPU, 8-GPU, 12-GPU, 16-GPU, 24-GPU, 32-GPU
        '''
        log_list = [event_time]
        gpu_list = [1, 2, 4, 8, 12, 16, 24, 32]
        for num_gpu in gpu_list:
            total_gpu_job = 0
            if num_gpu in JOBS01.gpu_job:
                total_gpu_job = num_gpu * JOBS01.gpu_job[num_gpu]

            log_list.append(total_gpu_job)

        self.job_list.append(log_list)
        if len(self.job_list) >= 1:
            self.dump_job_logs()


LOG01 = _Log01()


_allowed_symbols = [
    'LOG01'
]
