import utils.util01 as util01
import placement.base

def fit_first_simulator(JOBS01,CLUSTER01,LOG01,FLAGS01):
    '''
    new jobs are added to the end of the ending queue
    but any fit job should be executed in fifo order
    '''
    # print('fth job_events= %d \n',JOBS.job_events)
    while (len(JOBS01.job_events) + len(JOBS01.pending_jobs)) > 0:
        if len(JOBS01.job_events) == 0:
            util01.print_fn("This cluster is not large enough to run the job")
            break

            event = JOBS01.job_events

        event = JOBS01.job_events[0]
        event_time = event['time']
        print('--------------------------------- Handle event[time %d]------------------------------------' % event_time)
        # for ending jobs, release gpu
        for e_job in event['end_jobs']:
            # remove from migratable jobs, if it's there
            # JOBS.remote_migratable(e_job)

            # job completes
            CLUSTER01.release_job_res(e_job)
            # CLUSTER01.release_gpus(e_job)
            LOG01.job_complete(e_job, event_time)

        # for new-start jobs, try to start
        for s_job in event['start_jobs']:
            # add into pending list
            JOBS01.move_to_pending(s_job)

        new_start_list = list()
        for p_job in JOBS01.pending_jobs:
            # ret = CLUSTER.alloc_gpus(p_job)
            if CLUSTER01.check_free_gpu() <= 0:
                break
            ret = placement.base.try_get_job_res(p_job,FLAGS01,CLUSTER01)
            if ret is True:
                ''' if remove_from_pending,
                    then will miss the next p_job in the list '''
                new_start_list.append(p_job)
                # JOBS.remove_from_pending(p_job, event_time)
                # JOBS.add_job_end_event(p_job)
                # util.print_fn('----job[%d] starts from pending'
                #               % p_job['job_idx'])
            else:
                continue

        for ns_job in new_start_list:
            JOBS01.remove_from_pending(ns_job, event_time)  # 从等待队列中离开
            JOBS01.add_job_end_event(ns_job)  # 进入结束队列的末尾
            util01.print_fn('----job[%d] starts from pending'
                            % ns_job['job_idx'])

        # sort pending jobs based on the num_gpu
        # JOBS.pending_jobs.sort(key = lambda e:e.__getitem__('num_gpu'))

        # remove time_event
        JOBS01.job_events.pop(0)
        JOBS01.job_events.sort(key=lambda e: e.__getitem__('time'))  # 按time时间顺序排序
        # JOBS.print_job_events()

        # print('====fth event_time=',event_time)
        LOG01.checkpoint(event_time)
