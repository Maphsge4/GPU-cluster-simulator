# -*- coding: UTF-8 -*-

from numpy import full

from server.node import _Node
import utils.flags01 as flags01
# from simulator.cluster import FLAGS
# from simulator.jobs import JOBS
import utils.util01 as util01
import client.jobs as jobs
import math

FLAGS01 = flags01.FLAGS01
JOBS01 = jobs.JOBS01


class _Switch(object):

    def __init__(self, id, num_node=0, num_gpu_p_node=0,
                 num_cpu_p_node=0, mem_p_node=0):
        self.num_node = num_node
        self.num_gpu_p_node = num_gpu_p_node
        self.num_cpu_p_node = num_cpu_p_node
        self.mem_p_node = mem_p_node
        self.id = id
        self.node_list = list()
        util01.print_fn('  Switch[%d] has %d nodes'
                        % (id, num_node))

    def add_nodes(self, num_node=0, num_gpu_p_node=0,
                  num_cpu_p_node=0, mem_p_node=0):
        if (num_node != 0 and
            num_gpu_p_node != 0 and
            num_cpu_p_node != 0 and
            mem_p_node != 0):

            self.num_node = num_node
            self.num_gpu_p_node = num_gpu_p_node
            self.num_cpu_p_node = num_cpu_p_node
            self.mem_p_node = mem_p_node

        # 生成一系列node类，加入到node_list里面
        for n in range(0, self.num_node):
            tmp_n = _Node(n, self.num_gpu_p_node,
                          self.num_cpu_p_node, self.mem_p_node)
            self.node_list.append(tmp_n)

    def alloc_gpus(self, job):
        '''
        alloc gpus to job
        '''
        pass

    def try_cross_node_alloc(self, job):
        # print('---------- 进入try_cross_node_alloc(self, job)')  # debug
        '''
        used in MS_YARN placement
        try get gpus from multiple nodes
            [need_gpu / gpu_p_node] nodes,
            and one node with [need_gpu % gpu_p_node]
        if can't find , give up, and return False
        '''
        need_gpu = job['num_gpu']
        num_full_nodes = math.floor(need_gpu / self.num_gpu_p_node)
        last_node_gpu = need_gpu % self.num_gpu_p_node
        last_node_cpu = int(last_node_gpu * 6)
        last_node = None
        idle_node_cpu = int(self.num_gpu_p_node * 6)  # w:2, ps:4

        model_size = job['model']['total_size']
        
        # 在job01.py中
        # mem info in GB
        # self.worker_mem = 5
        # self.ps_mem = 6
        # p_w_mem表示最小的mem，单位mem，给每个gpu分配的小mem
        # self.p_w_mem = 0.1

        ps_mem = JOBS01.ps_mem + need_gpu * JOBS01.p_w_mem
        # parameter server and woker memory 简称ps_w_mem
        ps_w_mem = ps_mem + JOBS01.worker_mem

        full_node_list = list()
        for node in self.node_list:
            if (node.check_free_gpus() == node.num_gpu
                and node.check_free_cpus() >= idle_node_cpu
                and node.free_mem >= (ps_w_mem * node.num_gpu)):
                # get idle node
                full_node_list.append(node)
                if len(full_node_list) == num_full_nodes:
                    # enough full nodes
                    break
        if len(full_node_list) < num_full_nodes:
            return False

        # 如果最后一个node的gpu不为空，则要给它安排最后一个node，获得最后一个node
        if last_node_gpu != 0:
            for node in self.node_list:
                if node not in full_node_list:
                    if (node.check_free_gpus() >= last_node_gpu
                        and node.check_free_cpus() >= last_node_cpu
                        and node.free_mem >= (ps_w_mem * last_node_gpu)):

                        # get last node
                        last_node = node
                        break
            if last_node is None:
                # 说明最后剩下的gpu需求 没有node给它提供。返回false
                return False
            
        # 到目前为止，没有被return， 说明有可以满足cpu gpu mem要求的node资源
        # 则继续进行资源的counting和job的placement
        ''' can allocate, do resource counting and record job placement '''
        node_list = list()
        idx = 0
        for node in full_node_list:
            node.alloc_job_res(node.num_gpu, idle_node_cpu)
            node.free_mem -= ps_w_mem * node.num_gpu
            node_dict = dict()
            node_dict['id'] = node.id
            node_dict['num_gpu'] = node.num_gpu
            node_dict['num_cpu'] = idle_node_cpu
            node_dict['mem'] = ps_w_mem * node.num_gpu

            # traffic = round(model_size * node.num_gpu, 1)
            # for i in range(0, node.num_gpu):
            #     traffic += traffic + job['ps_network'][idx]
            #     traffic = round(traffic, 1)
            #     idx += 1

            # worker traffic
            traffic = round(model_size * node.num_gpu, 1)
            # ps traffic
            for i in range(0, node.num_gpu):
                # add ps traffic
                # send to (need - local_gpu) workers,
                # no need for local PS-to-worker
                traffic += job['ps_network'][idx] * (need_gpu - node.num_gpu)
                # remove co-locate worker traffic
                # no need for local worker-to-PS
                traffic -= job['ps_network'][idx] * node.num_gpu
                traffic = round(traffic, 1)
                idx += 1
            node_dict['network'] = traffic
            node.add_network_load(traffic, traffic)

            node_dict['tasks'] = list()
            node_list.append(node_dict)

        if last_node_gpu != 0:
            last_node.alloc_job_res(last_node_gpu, last_node_cpu)
            last_node.free_mem -= ps_w_mem * last_node_gpu
            node_dict = dict()
            node_dict['id'] = last_node.id
            node_dict['num_gpu'] = last_node_gpu
            node_dict['num_cpu'] = last_node_cpu
            node_dict['mem'] = ps_w_mem * last_node_gpu

            traffic = round(model_size * last_node_gpu, 1)
            # for i in range(0, last_node_gpu):
            #     traffic += job['ps_network'][idx]
            #     traffic = round(traffic, 1)
            #     idx += 1
            for i in range(0, last_node_gpu):
                # send to (need-last_gpu), no need for local PS-to-worker
                traffic += job['ps_network'][idx] * (need_gpu - last_node_gpu)
                # no need for local worker-to-PS
                traffic -= job['ps_network'][idx] * last_node_gpu
                traffic = round(traffic, 1)
                idx += 1
            node_dict['network'] = traffic
            last_node.add_network_load(traffic, traffic)

            node_dict['tasks'] = list()
            node_list.append(node_dict)

        JOBS01.create_multi_nodes_placement(job, self.id, node_list)
        # print('---------- 离开try_cross_node_alloc(self, job)')  # debug
        return True

    def try_single_node_alloc(self, job):
        '''
        used in MS_YARN placement
        try get gpus from a single node
        if can't find a node, give up, and return False
        '''
        need_gpu = job['num_gpu']
        if len(job['ps_network']) == 0 and job['num_gpu'] == 1:
            need_cpu = int(need_gpu * 2)  # worker:2  为什么要2个worker？
        else:
            need_cpu = int(need_gpu * 6)  # worker:2, ps:4

        for node in self.node_list:  # 依次查看每个node
            if ((node.check_free_gpus() >= need_gpu) and
                (node.check_free_cpus() >= need_cpu) and
                (node.free_mem >= JOBS01.worker_mem)):
                # if node.alloc_gpus(need_gpu) == False:
                if node.alloc_job_res(need_gpu, need_cpu) is False:
                    continue
                # 这儿把每个job需要的内存固定都设置成5GB了，貌似是有可以优化的空间
                node.free_mem = node.free_mem - JOBS01.worker_mem
                traffic = JOBS01.create_single_node_placement(job, self.id,
                                                              node.id, need_gpu,
                                                              need_cpu, JOBS01.worker_mem)
                # print("traffic:", traffic)   # debug 这个值肯定是0，因为single machine, no network traffic                                           
                # node.add_network_load(traffic, traffic)

                return True
            else:
                continue # all-or-nothing方法，一个结点要么全给要么全不给，不会跨节点

        return False

    def ms_yarn_alloc_res(self, job):
        print("进入ms_yarn_alloc_res函数") # debug
        '''
        ms_yarn allocates res from a single switch,
        if no enough gpus, give up, return False (all-or-nothing)

        if need_gpu > gpu_p_node
            then get [need_gpu / gpu_p_node] nodes,
                     and one node with [need_gpu % gpu_p_node]
        if need_gpu <= gpu_p_node
            then
        '''
        # print(job['num_gpu']) # debug
        need_gpu = job['num_gpu']
        ret = False
        print('need_gpu=',need_gpu,'self.num_gpu_p_node=',self.num_gpu_p_node)  # debug
        if need_gpu > self.num_gpu_p_node:
            print('---------- 进入try_cross_node_alloc(self, job)')  # debug
            ret = self.try_cross_node_alloc(job)
            print('---------- 离开try_cross_node_alloc(self, job)')  # debug
        else:
            print('---------- 进入try_single_node_alloc(self, job)')  # debug
            ret = self.try_single_node_alloc(job)
            print('---------- 离开try_single_node_alloc(self, job)')  # debug
        return ret

    def release_gpus(self, nodes):
        '''
        release gpus from nodes
        nodes:
        [{'id':xx, 'num_gpu':xxx}, {'id':xx, 'num_gpu':xxx}]
        '''

        for node_dict in nodes:
            if ('id' not in node_dict) or ('num_gpu' not in node_dict):
                return False
            node = self.node_list[node_dict['id']]
            ret = node.release_gpus(node_dict['num_gpu'])
            if ret is False:
                return False

        return True

    def release_job_res(self, nodes):
        '''
        release job resources from nodes
        nodes:
        [{'id':xx, 'num_gpu':xxx, 'num_cpu': xxx,
          'network': xxxx, 'tasks': [w2, ps2]},
         {'id':xx, 'num_gpu':xxx, 'num_cpu': xxx,
          'network': xxxx, 'tasks': [ps0]}
        ]
        '''
        print("-------进入switch.release_job_res()--------")  # debug
        for node_dict in nodes:
            if (('id' not in node_dict) or
                ('num_gpu' not in node_dict) or
                ('num_cpu' not in node_dict) or
                ('tasks' not in node_dict)):
                return False
            node = self.node_list[node_dict['id']]
            # ret = node.release_gpus(node_dict['num_gpu'])
            # print("node_dict:   ", node_dict)  # debug
            ret = node.release_job_res(node_dict)
            if ret is False:
                return False

        return True
