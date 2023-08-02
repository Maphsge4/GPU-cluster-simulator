import utils.util01 as util01
import placement.base

def one_queue_fifo_simulator(JOBS01,CLUSTER01,LOG01,FLAGS01):
    # print('---------- 进入one_queue_fifo_sim_jobs01()');  # debug
    '''
    run jobs in fifo order;
    new jobs are added to the end of the pending queue
    先来先服务，先到的job先服务
    一旦某个job超过集群承受能力，那么它之后的job都无法完成，因为“先到先服务”。同时会报This cluster is not large enough to run the job
    '''
    while (len(JOBS01.job_events) + len(JOBS01.pending_jobs)) > 0:
        # print("job events: ")  # debug
        # print(JOBS01.job_events)  # debug

        if len(JOBS01.job_events) == 0:
            # print("JOBS01.pending_jobs:", JOBS01.pending_jobs)  # debug
            util01.print_fn("This cluster is not large enough to run the job")
            break

        # 打印job_events
        # JOBS01.print_job_events() # debug
        # JOBS01.print_pending_jobs()  # debug

        event = JOBS01.job_events[0]
        # print(event)  # debug
        event_time = event['time']
        util01.print_fn('---------- Handle event[time %d]---------- ' % event_time)  # debug
        # for ending jobs, release gpu
        has_ejob = False
        for e_job in event['end_jobs']:
            # remove from migratable jobs, if it's there
            # JOBS.remote_migratable(e_job)

            # job completes
            if FLAGS01.schedule == 'gpu-demands':
                JOBS01.delete_gpu_job(e_job)
            else:
                CLUSTER01.release_job_res(e_job)  # 这个函数将job['status']设成了END
                # CLUSTER.release_gpus(e_job)
            LOG01.job_complete(e_job, event_time)
            # has_ejob = True

        # for new-start jobs, try to start
        # 把在这个时刻要开始的job先放到pending_list里面，等待分配资源
        for s_job in event['start_jobs']:
            # print(s_job)  # debug 输出的是orderedDict那个东西
            # add into pending list
            if FLAGS01.schedule == 'gpu-demands':
                s_job['end_time'] = s_job['submit_time'] + s_job['duration']
                JOBS01.add_job_end_event(s_job)
                util01.print_fn('----job[%d] starts' % s_job['job_idx'])
                # JOBS.read_job_info(s_job['job_idx'])
                JOBS01.add_gpu_job(s_job)
            else:
                JOBS01.move_to_pending(s_job)
            # print("pending jobs:", JOBS01.pending_jobs) # debug

        if FLAGS01.schedule != 'gpu-demands':
            if CLUSTER01.check_free_gpu() > 0:
                # for pending jobs, try to start
                new_start_list = list()
                for p_job in JOBS01.pending_jobs:
                    # ret = CLUSTER.alloc_gpus(p_job)
                    ret = placement.base.try_get_job_res(p_job,FLAGS01,CLUSTER01) # res是resource的意思
                    if ret is True:
                        ''' if remove_from_pending,
                            then will miss the next p_job in the list '''
                        new_start_list.append(p_job)
                        # if job is migratable, add into migratable job list
                        # JOBS.add_migratable(p_job)
                        # JOBS.remove_from_pending(p_job, event_time)
                        # JOBS.add_job_end_event(p_job)
                        # util.print_fn('----job[%d] starts from pending'
                        #               % p_job['job_idx'])
                        # JOBS.read_job_info(p_job['job_idx'])
                    else:
                        break
                for ns_job in new_start_list:
                    JOBS01.remove_from_pending(ns_job, event_time)
                    JOBS01.add_job_end_event(ns_job)
                    util01.print_fn('----job[%d] starts from pending'
                                    % ns_job['job_idx'])

        # sort pending jobs based on the num_gpu
        # JOBS.pending_jobs.sort(key = lambda e:e.__getitem__('num_gpu'))

        # remove time_event
        JOBS01.job_events.pop(0)
        JOBS01.job_events.sort(key=lambda e: e.__getitem__('time'))
        # JOBS.print_job_events()

        if FLAGS01.schedule == 'gpu-demands':
            LOG01.checkpoint_gpu_demands(event_time)
        else:
            LOG01.checkpoint(event_time)
    
    # print("job_list:")
    # print(JOBS01.job_list)  # debug
    # print('---------- 离开one_queue_fifo_simulator()');  # debug

