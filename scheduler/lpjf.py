import utils.util01 as util01
import placement.base

def longest_pending_first_sim_job1(JOBS01,CLUSTER01,LOG01,FLAGS01):
    '''
    !!!!!!  NOT USEFUL
    new job has the highest priority then pending jobs
    jobs are scheduled based on their pending time: longest first
    '''
    # pass
    while (len(JOBS01.job_events) + len(JOBS01.pending_jobs))> 0:
        if len(JOBS01.job_events) == 0:
            util01.print_fn("This cluster is not large enough to run the job")
            break
        
        event = JOBS01.job_events[0]
        event_time = event['time']
        # for ending jobs, release gpu
        for e_job in event['end_jobs']:
            #remove from migratable jobs, if it's there
            # JOBS.remote_migratable(e_job)

            #job completes
            CLUSTER01.release_job_res(e_job)
            # CLUSTER.release_gpus(e_job)
            LOG01.job_complete(e_job, event_time)
            
        # for new-start jobs, try to start
        for s_job in event['start_jobs']:
            #add into pending list
            JOBS01.move_to_pending(s_job)
            
        # sort pending jobs based on the pending_time
        JOBS01.update_pending_time(event_time)
        JOBS01.pending_jobs.sort(key=lambda e: e.__getitem__('pending_time'), reverse=True)
        # JOBS.pending_jobs.sort(key = lambda e:e.__getitem__('num_gpu'))

        # for pending jobs, try to start
        for p_job in JOBS01.pending_jobs:
            # ret = CLUSTER.alloc_gpus(p_job)
            ret = placement.base.try_get_job_res(p_job,FLAGS01,CLUSTER01)
            if ret == True:
                # if job is migratable, add into migratable job list
                # JOBS.add_migratable(p_job)
                JOBS01.remove_from_pending(p_job, event_time)
                JOBS01.add_job_end_event(p_job)
                util01.print_fn('----job[%d] starts from pending'
                                % p_job['job_idx'])
                # JOBS.read_job_info(p_job['job_idx'])
            else:
                break
            
        # remove time_event
        JOBS01.job_events.pop(0)
        JOBS01.job_events.sort(key = lambda e:e.__getitem__('time'))
        # JOBS.print_job_events()

        LOG01.checkpoint(event_time)
