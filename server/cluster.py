# -*- coding: UTF-8 -*-

import math
import random

from server.switch import _Switch
from server.node import _Node

from numpy import fromregex
# from simulator.cluster import FLAGS
import utils.util01 as util01
import utils.flags01 as flags01

FLAGS01 = flags01.FLAGS01


class _Cluster(object):

    def __init__(self, num_switch=0, num_node_p_switch=0,
                 num_gpu_p_node=0, num_cpu_p_node=0, mem_p_node=0):
        ''' Init GPU cluster with basic switch, node, gpu information'''
        self.num_switch =  num_switch
        self.num_node_p_switch = num_node_p_switch
        self.num_gpu_p_node = num_gpu_p_node
        self.num_cpu_p_node = num_cpu_p_node
        self.mem_p_node = mem_p_node
        self.num_node = num_switch * num_node_p_switch
        self.num_gpu = self.num_node * num_gpu_p_node
        self.num_cpu = self.num_node * num_cpu_p_node
        self.mem = self.num_node * mem_p_node

        self.switch_list = list()

        # for non-placement
        self.free_gpu = self.num_gpu
        self.gpu_list = list()

        # for gandiva
        self.free_nodes = list()
        self.node_g1 = list()
        self.node_g2 = list()
        self.node_g4 = list()
        self.node_g8 = list()
        self.node_g12 = list()
        self.node_g16 = list()
        self.node_g24 = list()
        self.node_g32 = list()
        self.node_g64 = list()
        # self.node_g128 = list()
        # self.node_g256 = list()
        self.node_g = {1: self.node_g1, 2: self.node_g2, 4: self.node_g4,
                       8: self.node_g8, 12: self.node_g12, 16: self.node_g16,
                       24: self.node_g24, 32: self.node_g32, 64: self.node_g64}
                       # 64: self.node_g64, 128: self.node_g128, 256: self.node_g256}

    def set_spec(self, num_switch=0, num_node_p_switch=0,
                 num_gpu_p_node=0, num_cpu_p_node=0, mem_p_node=0):
        self.num_switch = num_switch
        self.num_node_p_switch = num_node_p_switch
        self.num_gpu_p_node = num_gpu_p_node
        self.num_cpu_p_node = num_cpu_p_node
        self.mem_p_node = mem_p_node
        self.num_node = num_switch * num_node_p_switch
        self.num_gpu = self.num_node * num_gpu_p_node
        self.num_cpu = self.num_node * num_cpu_p_node
        self.free_gpu = self.num_gpu
        self.mem = self.num_node * mem_p_node

    def print_cluster_spec(self):
        print('Cluster Spec')
        print('#ofswitch: %d, #ofnode: %d, #ofgpu: %d, #ofcpu: %d, #ofmem: %d'
              %(self.num_switch, self.num_node,
                self.num_gpu, self.num_cpu, self.mem))
        print('#ofnode/switch: %d, #ofgpu/node: %d, #ofcpu/node: %d, #ofmem/node: %d'
              % (self.num_node_p_switch, self.num_gpu_p_node,
                 self.num_cpu_p_node, self.mem_p_node))

    def init_infra(self, num_switch=0, num_node_p_switch=0,
                   num_gpu_p_node=0, num_cpu_p_node=0, mem_p_node=0):
        print('---------- 进入 CLUSTER01.init_infra() \n---------- Init and create cluster infration entities (switches, nodes) \n---------- by using class _Switch, _Node')
        '''
        Init and create cluster infration entities (switches, nodes)
        by using class _Switch, _Node
        '''
        # 第一次进入的时候，没传参数，所以默认都是0
        if (num_switch == 0 and num_node_p_switch == 0
            and num_gpu_p_node == 0 and num_cpu_p_node == 0
            and mem_p_node == 0):
            # no new spec, apply FLAGS spec info
            self.set_spec(FLAGS01.num_switch, FLAGS01.num_node_p_switch,
                          FLAGS01.num_gpu_p_node, FLAGS01.num_cpu_p_node,
                          FLAGS01.mem_p_node)
        else:
            self.set_spec(num_switch, num_node_p_switch, num_gpu_p_node,
                          num_cpu_p_node, mem_p_node)

        '''create/init switch and node objects'''
        for s in range(0, self.num_switch):
            # print(s)  # debug 结果是咱们这个只会输出一个0，循环只进行一次，很对
            tmp_s = _Switch(s, self.num_node_p_switch, self.num_gpu_p_node,
                            self.num_cpu_p_node, self.mem_p_node)
            # 上一步是init一个switch类，下面这个是给这个switch里面添加node
            tmp_s.add_nodes(self.num_node_p_switch, self.num_gpu_p_node,
                            self.num_cpu_p_node, self.mem_p_node)
            self.switch_list.append(tmp_s)

        util01.print_fn('Cluster is ready to use')
        self.print_cluster_spec()
        print('----------离开 CLUSTER01.init_infra()')

    def empty_infra(self):
        self.free_gpu = self.num_gpu
        # print("self.free_gpu",self.free_gpu)  # debug
        for switch in self.switch_list:
            for node in switch.node_list:
                node.init_node(self.num_gpu_p_node, self.num_cpu_p_node)

        if FLAGS01.schedule == 'dlas-gpu-pack':
            self.init_dlas_pack_gpu()

    def init_dlas_pack_gpu(self):
        self.gpu_list[:] = []
        for switch in self.switch_list:
            for node in switch.node_list:
                for i in range(self.num_gpu_p_node):
                    self.gpu_list.append(1)
        util01.print_fn("There are %d gpus" % len(self.gpu_list))

    def free_gpu_util(self, job):
        job_util = job['model']['mem_util']
        num_gpu = job['num_gpu']
        i = 0
        for gpu in self.gpu_list:
            if gpu >= job_util:
                # gpu = gpu - job_util
                i = i + 1
            if num_gpu <= i:
                return True
        return False

    def dlas_pack_get_gpu_util(self, job):
        job_util = job['model']['mem_util']
        num_gpu = job['num_gpu']
        i = 0
        for j in range(len(self.gpu_list)):
            if self.gpu_list[j] >= job_util:
                self.gpu_list[j] = self.gpu_list[j] - job_util
                job['gpus'].append(j)
                i = i + 1
            if num_gpu <= i:
                return True
        return False

    def init_gandiva_nodes(self):
        '''
        init node class:
        '''
        print("-------进入init_gandiva_nodes()----------")  # debug
        # free nodes
        for switch in self.switch_list:
            for node in switch.node_list:
                self.free_nodes.append(node)

        if len(self.free_nodes) != self.num_node:
            util01.print_fn("free_node: %s, is incorrect"
                            % len(self.free_nodes))
            exit()

        # print(self.node_g.items())  # debug
        for num_gpu, node_list in self.node_g.items():
            print(num_gpu)

    def gandiva_node_set_shrink(self, ns_num_gpu, node_list,
                                num_ns, cur_time, jobs, logs):
        '''
        ns_num_gpu: num_gpu of job in this node_set
        node_list: node_set list to expend to
        num_ns: # of node_set to expend

        when there are no enough free nodes,
        we need to shrink some node_sets under some node_gx.
        How many node_sets should be taken from which node_gx?
        '''
        # can't shrink too many node_set.
        if len(node_list) <= num_ns:
            num_ns = len(node_list) - 1
        job_list = list()
        i = 0
        for i in range(1, num_ns+1):
            # free node_set, keep jobs
            node_set = node_list.pop(0)
            if len(node_set['jobs']) > 0:
                job_list.extend(node_set['jobs'])
            for node in node_set['nodes']:
                self.free_nodes.append(node)

        if i > 0:
            # assign jobs
            for job in job_list:
                node_set = node_list[0]
                job_util = round(job['model']['mem_util'], 2)
                node_set['util'] = round(node_set['util'] + job_util, 2)
                node_set['jobs'].append(job)
                node_set['num_jobs'] = node_set['num_jobs'] + 1
                node_list.sort(key=lambda e: e.__getitem__('util'))

        util01.print_fn("node_g%d shrink %d node_sets" % (ns_num_gpu, i))
        return i

    def gandiva_node_set_expand(self, ns_num_gpu, node_list,
                                num_ns, cur_time, jobs, logs):
        '''
        ns_num_gpu: num_gpu of job in this node_set
        node_list: node_set list to expend to
        num_ns: # of node_set to expend
        '''
        # each node_set need
        num_node = int(math.ceil(ns_num_gpu / self.num_gpu_p_node))
        i = 0
        # expand node_set
        for i in range(1, num_ns+1):
            if num_node <= len(self.free_nodes):
                # init node_set
                node_set = dict()
                node_set['nodes'] = list()
                node_set['gpu_u'] = list()
                node_set['jobs'] = list()
                node_set['concurrency'] = 0
                node_set['capacity'] = int(num_node * self.num_gpu_p_node / ns_num_gpu)
                node_set['num_gpus'] = ns_num_gpu
                node_set['num_jobs'] = 0
                node_set['util'] = 0
                for i in range(num_node):
                    node_set['nodes'].append(self.free_nodes.pop(0))

                node_list.append(node_set)

        # re-arrange
        if i > 0:
            job_list = list()
            for node_set in node_list:
                if len(node_set['jobs']) > 0:
                    job_list.extend(node_set['jobs'])
                    node_set['jobs'][:] = []
                    node_set['util'] = 0
                    node_set['num_jobs'] = 0

            for job in job_list:
                node_set = node_list[0]
                job_util = round(job['model']['mem_util'], 2)
                node_set['util'] = round(node_set['util'] + job_util, 2)
                node_set['jobs'].append(job)
                node_set['num_jobs'] = node_set['num_jobs'] + 1
                node_list.sort(key=lambda e: e.__getitem__('util'))

        util01.print_fn("node_g %d expand %d node_sets" % (ns_num_gpu, i))
        return i

    def gandiva_node_set_adjust(self, cur_time, jobs, logs):
        '''
        when there are free nodes in the cluster,
        while some node_sets are heavy-loaded.
        Busy node sets should shift jobs to those free nodes.
        challenge:
        1. How to allocate the free resources to different node_sets.
        '''
        print("-----------进入gandiva_node_set_adjust()-------------")  # debug
        # total_gpu
        total_gpu_demands = 0
        nl_gpu_demands = dict()
        nl_gpu_occupied = dict()
        for num_gpu, node_list in self.node_g.items():  # 每一轮循环，计算每个pack内有多少job，占了多少GPU
            total_jobs = 0
            tmp_gpus = 0
            occupied_gpus = 0
            for node_set in node_list:
                total_jobs = total_jobs + len(node_set['jobs'])
                occupied_gpus = occupied_gpus + (len(node_set['nodes'])
                                                 * self.num_gpu_p_node)
            tmp_gpus = total_jobs * num_gpu
            total_gpu_demands = total_gpu_demands + tmp_gpus
            nl_gpu_demands[num_gpu] = tmp_gpus
            nl_gpu_occupied[num_gpu] = occupied_gpus

        if total_gpu_demands == 0:
            return

        for num_gpu, node_list in self.node_g.items():
            if nl_gpu_demands[num_gpu] == 0:
                continue
            # Math.floor() 返回小于或等于一个给定数字的最大整数。
            nl_gpu_plan = int(math.floor((nl_gpu_demands[num_gpu] / total_gpu_demands)
                                           * self.num_gpu))
            nl_gpu_target = nl_gpu_plan if nl_gpu_plan < nl_gpu_demands[num_gpu] else nl_gpu_demands[num_gpu]
            nl_gpu_diff = nl_gpu_target - nl_gpu_occupied[num_gpu]

            if nl_gpu_diff > 0:
                # growth: how many node_set are needed
                num_ns = int(math.ceil(nl_gpu_diff / num_gpu))
                expand_ns = self.gandiva_node_set_expand(num_gpu, node_list,
                                                         num_ns, cur_time,
                                                         jobs, logs)

            elif nl_gpu_diff < 0:
                # shrink
                # Python math.ceil(x) 方法将 x 向上舍入到最接近的整数。
                num_ns = int(math.ceil((0 - nl_gpu_diff) / num_gpu))
                # num_node = int(math.ceil( (0 - nl_gpu_diff) / self.num_gpu_p_node))
                shrink_ns = self.gandiva_node_set_shrink(num_gpu, node_list,
                                                         num_ns, cur_time, jobs, logs)


    def ms_yarn_placement(self, job):
        '''
        MS_YARN, all gpus should come from the same switch
        '''
        for switch in self.switch_list:
            ret = switch.ms_yarn_alloc_res(job)
            if ret is True:
                return True
            else:
                continue
        return False

    def random_placement(self, job):
        '''
        randomly pick up enough resource for both PS and worker in a job
        allocate one by one
        '''
        num_ps = len(job['ps_network'])
        num_w = job['num_gpu']
        if num_w != 1:
            assert num_ps == num_w

        # go through workers
        w_node_list = list()
        w_switch_list = list()

        for w in range(0, num_w):
            start_ngid = random.randint(0, self.num_node - 1)
            allocated = False
            for i in range(self.num_node):
                n_gid = int(int(start_ngid + i) % self.num_node)
                tmp_infra = self.get_node_with_gid(n_gid)
                node = tmp_infra['node']
                switch = tmp_infra['switch']
                switch_idx = switch.id

                if node.check_free_gpus() >= 1 and node.check_free_cpus() >= 2:
                    w_node_list.append(node)
                    w_switch_list.append(switch_idx)
                    node.alloc_job_res(1, 2)
                    allocated = True
                    break
            if allocated is False:
                for n in w_node_list:
                    n.release_job_gpu_cpu(1, 2)
                return False

        # go through PS
        ps_node_list = list()
        ps_switch_list = list()

        for ps in range(0, num_ps):
            start_ngid = random.randint(0, self.num_node - 1)
            allocated = False
            for i in range(self.num_node):
                n_gid = int(int(start_ngid + i) % self.num_node)
                tmp_infra = self.get_node_with_gid(n_gid)
                node = tmp_infra['node']
                switch = tmp_infra['switch']
                switch_idx = switch.id

                if node.check_free_cpus() >= 4:
                    ps_node_list.append(node)
                    ps_switch_list.append(switch_idx)
                    node.alloc_cpus(4)
                    allocated = True
                    break
            if allocated is False:
                for n in w_node_list:
                    n.release_job_gpu_cpu(1, 2)
                for n in ps_node_list:
                    n.release_cpus(4)

                return False

        node_list = list()
        for i in range(len(w_node_list)):
            w_n = w_node_list[i]
            tmp_dict = util01.search_dict_list(node_list, 'node', w_n)
            if tmp_dict is None:
                tmp_dict = dict()
                tmp_dict['node'] = w_n
                tmp_dict['worker'] = 1
                tmp_dict['ps'] = list()
                node_list.append(tmp_dict)
            else:
                tmp_dict['worker'] += 1

        for i in range(len(ps_node_list)):
            ps_n = ps_node_list[i]
            tmp_dict = util01.search_dict_list(node_list, 'node', ps_n)
            if tmp_dict is None:
                tmp_dict = dict()
                tmp_dict['node'] = ps_n
                tmp_dict['worker'] = 0
                tmp_dict['ps'] = list([i])
                node_list.append(tmp_dict)
            else:
                tmp_dict['ps'].append(i)

        # job placements, and network load
        for i in range(0, num_w):
            s_id = w_switch_list[i]
            node = w_node_list[i]

            # check colocate info, and deduct co-locate worker-to-PS traffic
            # ps为parallel server？ 参数服务器？placement scheme? 
            # ps为参数服务器 从worker发送信息给PS
            colocate_info = util01.search_dict_list(node_list, 'node', node)
            de_traffic = 0
            if colocate_info is not None:
                for j in colocate_info['ps']:
                    de_traffic += job['ps_network'][j]
                    # round 四舍五入到小数点后1位(num_digits位) round(number, num_digits)
                    de_traffic = round(de_traffic, 1)

            tmp_dict = dict()
            node_dict = dict()
            node_dict['id'] = node.id
            node_dict['num_gpu'] = 1
            node_dict['num_cpu'] = 2
            node_dict['network'] = round(job['w_network'][i] - de_traffic, 1)
            node.add_network_load(node_dict['network'], node_dict['network'])

            node_dict['tasks'] = list()
            tmp_dict['switch'] = s_id
            tmp_dict['nodes'] = list()
            tmp_dict['nodes'].append(node_dict)
            job['placements'].append(tmp_dict)

        for i in range(0, num_ps):
            s_id = ps_switch_list[i]
            node = ps_node_list[i]

            # check colocate info, and deduct co-locate PS-to-worker traffic
            # ps为参数服务器 从ps发送信息给worker
            # https://cloud.tencent.com/developer/news/393079
            colocate_info = util01.search_dict_list(node_list, 'node', node)
            de_worker = 0
            if colocate_info is not None:
                de_worker = colocate_info['worker']

            tmp_dict = dict()
            node_dict = dict()
            node_dict['id'] = node.id
            node_dict['num_gpu'] = 0
            node_dict['num_cpu'] = 4
            node_dict['network'] = round(job['ps_network'][i] 
                                         * (num_w - de_worker), 1)
            node.add_network_load(node_dict['network'], node_dict['network'])

            node_dict['tasks'] = list()
            tmp_dict['switch'] = s_id
            tmp_dict['nodes'] = list()
            tmp_dict['nodes'].append(node_dict)
            job['placements'].append(tmp_dict)

        # print(job['placements'])
        return True


    def consolidate_random_placement(self, job):
        '''
        consolidate first, but randomly pick machines;
        if cross machines, still try to consolidate.
        if can't consolidate, consider spreed the jobs;
        also PS is randomly placed on the selected machines
        '''

        num_ps = len(job['ps_network'])
        num_w = job['num_gpu']
        if num_w != 1:
            assert num_ps == num_w

        # handle single w case

        '''
        ret = self.try_consolidate_job(job)
        randomly pick up machines
        '''
        num_full_nodes = math.floor(num_w / self.num_gpu_p_node)
        last_node_gpu = num_w % self.num_gpu_p_node
        num_node = num_full_nodes
        num_gpu_list = list([self.num_gpu_p_node] * int(num_full_nodes))
        if last_node_gpu != 0:
            num_node += 1
            num_gpu_list.append(last_node_gpu)
        num_node = int(num_node)

        # print(num_node)
        # print(num_gpu_list)

        # go through workers
        w_node_list = list()
        w_switch_list = list()
        p_done = True
        for i in range(0, num_node):
            switch_idx = random.randint(0, self.num_switch - 1)
            switch_s_idx = switch_idx
            allocated = False
            need_gpu = num_gpu_list[i]
            need_cpu = 0
            if num_w == 1:
                need_cpu = need_gpu * 2
            else:
                need_cpu = need_gpu * 6

            while True:  # scan all the switch
                switch = self.switch_list[switch_idx]
                node_idx = random.randint(0, self.num_node_p_switch - 1)
                node_s_idx = node_idx
                while True:  # scan all the nodes
                    # print('xxx', node_idx, need_gpu)
                    node = switch.node_list[node_idx]
                    if (node.check_free_gpus() >= need_gpu
                        and node.check_free_cpus() >= need_cpu):
                        for i in range(need_gpu):
                            w_node_list.append(node)
                            w_switch_list.append(switch_idx)
                        node.alloc_job_res(need_gpu, need_cpu)
                        # print(node.id, need_gpu)
                        allocated = True
                        break
                    else:
                        node_idx += 1
                        node_idx %= self.num_node_p_switch
                        node_idx = int(node_idx)
                        if node_idx == node_s_idx:
                            break
                if allocated is True:
                    break
                else:
                    switch_idx += 1
                    switch_idx %= self.num_switch
                    switch_idx = int(switch_idx)
                    if switch_idx == switch_s_idx:
                        break

            # go through all the machines, can't consolidate the jobs
            if allocated is False:
                p_done = False
                break
                # can't conslidate, need to handle differently
                # for n in node_list:
                #     n.release_job_gpu_cpu(need_gpu, need_cpu)

        # can't conslidate
        if p_done is False:
            remain_gpu = 0
            for j in range(i, num_node):
                remain_gpu += num_gpu_list[j]
            if remain_gpu == 1:
                # release allocated resource
                for n in w_node_list:
                    n.release_job_gpu_cpu(1, 6)
                return False

            # print('remain_gpu', remain_gpu)

            # while remain_gpu > 0:
            switch_idx = random.randint(0, self.num_switch - 1)
            switch_s_idx = switch_idx
            while remain_gpu > 0:
                switch = self.switch_list[switch_idx]
                node_idx = random.randint(0, self.num_node_p_switch - 1)
                node_s_idx = node_idx
                while remain_gpu > 0:
                    node = switch.node_list[node_idx]
                    free_gpu = node.check_free_gpus()
                    free_cpu = node.check_free_cpus()
                    # print('node: %d, free_gpu %d, remain_gpu %d'
                    #       % (node.id, free_gpu, remain_gpu))
                    if free_gpu >= 1 and free_cpu >= 6:
                        p_w = free_gpu
                        if free_gpu >= (free_cpu / 6):
                            p_w = int(free_cpu/6)
                        if p_w >= remain_gpu:
                            p_w = remain_gpu
                        remain_gpu -= p_w
                        remain_gpu = int(remain_gpu)

                        for i in range(p_w):
                            w_node_list.append(node)
                            w_switch_list.append(switch_idx)
                        node.alloc_job_res(int(p_w), int(p_w * 6))
                    # else:
                    node_idx += 1
                    node_idx %= self.num_node_p_switch
                    node_idx = int(node_idx)
                    if node_idx == node_s_idx:
                        break

                switch_idx += 1
                switch_idx %= self.num_switch
                switch_idx = int(switch_idx)
                if switch_idx == switch_s_idx:
                    break

            if remain_gpu is not 0:                     
                '''can't allocate '''
                for n in w_node_list:
                    n.release_job_gpu_cpu(1, 6)
                return False

        '''
        randomly place PS to node_list
        '''
        ps_node_list = list()
        ps_switch_list = list()
        node_idx_list = list([i for i in range(num_ps)])
        random.shuffle(node_idx_list)
        for ind in node_idx_list:
            ps_node_list.append(w_node_list[ind])
            ps_switch_list.append(w_switch_list[ind])

        # go through all the related nodes
        node_list = list()
        for i in range(len(w_node_list)):
            w_n = w_node_list[i]
            tmp_dict = util01.search_dict_list(node_list, 'node', w_n)
            if tmp_dict is None:
                tmp_dict = dict()
                tmp_dict['node'] = w_n
                tmp_dict['worker'] = 1
                tmp_dict['ps'] = list()
                node_list.append(tmp_dict)
            else:
                tmp_dict['worker'] += 1

        for i in range(len(ps_node_list)):
            ps_n = ps_node_list[i]
            tmp_dict = util01.search_dict_list(node_list, 'node', ps_n)
            if tmp_dict is None:
                tmp_dict = dict()
                tmp_dict['node'] = ps_n
                tmp_dict['worker'] = 0
                tmp_dict['ps'] = list([i])
                node_list.append(tmp_dict)
            else:
                tmp_dict['ps'].append(i)

        # job placements, and network load
        for i in range(0, num_w):
            s_id = w_switch_list[i]
            node = w_node_list[i]

            # check colocate info, and deduct co-locate worker-to-PS traffic
            colocate_info = util01.search_dict_list(node_list, 'node', node)
            de_traffic = 0
            if colocate_info is not None:
                for j in colocate_info['ps']:
                    de_traffic += job['ps_network'][j]
                    de_traffic = round(de_traffic, 1)

            tmp_dict = dict()
            node_dict = dict()
            node_dict['id'] = node.id
            node_dict['num_gpu'] = 1
            node_dict['num_cpu'] = 2
            node_dict['network'] = round(job['w_network'][i] - de_traffic, 1)
            node.add_network_load(node_dict['network'], node_dict['network'])

            node_dict['tasks'] = list()
            tmp_dict['switch'] = s_id
            tmp_dict['nodes'] = list()
            tmp_dict['nodes'].append(node_dict)
            job['placements'].append(tmp_dict)

        for i in range(0, num_ps):
            s_id = ps_switch_list[i]
            node = ps_node_list[i]

            # check colocate info, and deduct co-locate PS-to-worker traffic
            colocate_info = util01.search_dict_list(node_list, 'node', node)
            de_worker = 0
            if colocate_info is not None:
                de_worker = colocate_info['worker']

            tmp_dict = dict()
            node_dict = dict()
            node_dict['id'] = node.id
            node_dict['num_gpu'] = 0
            node_dict['num_cpu'] = 4
            node_dict['network'] = round(job['ps_network'][i]
                                         * (num_w - de_worker), 1)
            node.add_network_load(node_dict['network'], node_dict['network'])

            node_dict['tasks'] = list()
            tmp_dict['switch'] = s_id
            tmp_dict['nodes'] = list()
            tmp_dict['nodes'].append(node_dict)
            job['placements'].append(tmp_dict)

        # print(job['placements'])
        return True

    def none_placement(self, job):
        num_w = job['num_gpu']

        if self.free_gpu >= num_w:
            self.free_gpu = int(self.free_gpu - num_w)
            return True
        else:
            return False

    def check_free_gpu(self):
        if FLAGS01.scheme == 'count':
            return self.free_gpu
        else:
            free_gpu = 0
            for switch in self.switch_list:
                for node in switch.node_list:
                    free_gpu = int(free_gpu + node.check_free_gpus())
            return free_gpu

    def greedy_placement(self, job):
        '''
        greedy placement:
        '''
        num_ps = len(job['ps_network'])
        num_w = job['num_gpu']
        if num_w != 1:
            assert num_ps == num_w

        return True

    def get_node_with_gid(self, gid):
        s_id = int(math.floor(gid / self.num_node_p_switch))
        n_id = int(gid % self.num_node_p_switch)

        switch = self.switch_list[s_id]
        node = switch.node_list[n_id]
        ret = dict()
        ret['switch'] = switch
        ret['node'] = node
        return ret

    def alloc_gpus(self, job):
        '''
        allocate gpus to job
        '''
        ret = self.ms_yarn_placement(job)
        if ret is True:
            job['status'] = 'RUNNING'
        return ret

    def release_gpus(self, job):
        for placement in job['placements']:
            if ('switch' not in placement) or ('nodes' not in placement):
                job['status'] = 'ERROR'
                return False

            switch = self.switch_list[placement['switch']]
            ret = switch.release_gpus(placement['nodes'])
            if ret is False:
                job['status'] = 'ERROR'
                return False

        job['status'] = 'END'
        util01.print_fn('**** job[%d] completed' % job['job_idx'])
        return True

    '''
    release job res
    '''
    def release_job_res(self, job):
        '''
        release gpu/cpu/network
        placements:
        [{'switch': xx, 'nodes': [{'id':xx, 'num_gpu':xxx, 'num_cpu': xxx,
                                   'network': xxxx, tasks': [w0, w1, ps1]}]},
         {'switch': xx, 'nodes': [{'id':xx, 'num_gpu':xxx, 'num_cpu': xxx,
                                   'network': xxxx, 'tasks': [w2, ps2]},
                                  {'id':xx, 'num_gpu':xxx, 'num_cpu': xxx,
                                   'network_load': xxxx, 'tasks': [ps0]}]}
        ]
        '''
        print("-------进入cluster.release_job_res()--------")  # debug
        if FLAGS01.schedule == 'dlas-gpu-pack':
            job_util = job['model']['mem_util']
            for gpu_idx in job['gpus']:
                self.gpu_list[gpu_idx] = int(self.gpu_list[gpu_idx] + job_util)
                if self.gpu_list[gpu_idx] != 1:
                    util01.print_fn("Error: release gpu error in dlas-gpu-pack\n")
                    job['status'] = 'ERROR'
                    return False
            job['status'] = 'END'
            util01.print_fn('**** job[%d] completed' % job['job_idx'])
            return True

        if FLAGS01.scheme == 'count':
            self.free_gpu = (self.free_gpu + job['num_gpu'])
            if self.free_gpu > self.num_gpu:
                self.free_gpu = self.num_gpu
            job['status'] = 'END'
            util01.print_fn('**** job[%d] completed' % job['job_idx'])
            return True

        # print("job[placements]:  ", job['placements'])  # debug
        for placement in job['placements']:
            if ('switch' not in placement) or ('nodes' not in placement):
                job['status'] = 'ERROR'
                return False

            switch = self.switch_list[placement['switch']]
            ret = switch.release_job_res(placement['nodes'])
            if ret is False:
                job['status'] = 'ERROR'
                return False

        job['status'] = 'END'
        util01.print_fn('**** job[%d] completed' % job['job_idx'])
        return True


CLUSTER01 = _Cluster()


_allowed_symbols = [
    'CLUSTER'
]
