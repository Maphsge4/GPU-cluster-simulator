import random
import utils.util01 as util01

def random_placement(job,CLUSTER01):
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

    for w in range(0, num_w):  # 一个一个GPU来看！
        start_ngid = random.randint(0, CLUSTER01.num_node - 1)
        allocated = False
        for i in range(CLUSTER01.num_node):
            n_gid = int(int(start_ngid + i) % CLUSTER01.num_node)
            tmp_infra = CLUSTER01.get_node_with_gid(n_gid)  # 得出了几号switch，几号node
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
        start_ngid = random.randint(0, CLUSTER01.num_node - 1)
        allocated = False
        for i in range(CLUSTER01.num_node):
            n_gid = int(int(start_ngid + i) % CLUSTER01.num_node)
            tmp_infra = CLUSTER01.get_node_with_gid(n_gid)
            node = tmp_infra['node']
            switch = tmp_infra['switch']
            switch_idx = switch.id

            if node.check_free_cpus() >= 4:  # ps只申请了CPU，没有GPU
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

    # 统计每个node里面几个worker，将信息存放在node_list里面
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

    # 统计每个node里面几个ps，将信息存放在node_list里面
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
        # ps为参数服务器 从worker发送信息给PS
        colocate_info = util01.search_dict_list(node_list, 'node', node)
        de_traffic = 0
        if colocate_info is not None:
            for j in colocate_info['ps']:  # 对于每一个node里面的所有ps，求network的累加和
                de_traffic += job['ps_network'][j]
                # round 四舍五入到小数点后1位(num_digits位) round(number, num_digits)
                de_traffic = round(de_traffic, 1)

        tmp_dict = dict()
        node_dict = dict()
        node_dict['id'] = node.id
        node_dict['num_gpu'] = 1
        node_dict['num_cpu'] = 2
        node_dict['network'] = round(job['w_network'][i] - de_traffic, 1)  # 为什么要减？应该是说，node内的不占用network，不同node才需要
        node.add_network_load(node_dict['network'], node_dict['network'])  # 一个inload，一个outload
        # 可以理解为，一个模型有这么多参数，分配给所有的ps来存、同步，每个参数的传输需要这么多的网络带宽

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
                                        * (num_w - de_worker), 1)  # 要给那些不在我这个node上的GPU传参数，占用网络开销
        node.add_network_load(node_dict['network'], node_dict['network'])

        node_dict['tasks'] = list()
        tmp_dict['switch'] = s_id
        tmp_dict['nodes'] = list()
        tmp_dict['nodes'].append(node_dict)
        job['placements'].append(tmp_dict)

    # print(job['placements'])
    return True
