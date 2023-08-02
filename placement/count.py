def none_placement(job,CLUSTER):
    num_w = job['num_gpu']

    if CLUSTER.free_gpu >= num_w:
        CLUSTER.free_gpu = int(CLUSTER.free_gpu - num_w)
        return True
    else:
        return False
