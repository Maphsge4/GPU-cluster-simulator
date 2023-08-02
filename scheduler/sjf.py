import utils.util01 as util01
import placement.base
import sys

def smallest_first_sim_jobs01(gputime,JOBS01,CLUSTER01,LOG01,FLAGS01):
    '''
    new jobs are added to the end of the ending queue
    but in the queue, shortest (gpu) job first be served, until no resource
    '''

    end_events = list()
    while (len(JOBS01.job_events) + len(JOBS01.runnable_jobs)) > 0:
        if (len(JOBS01.job_events) + len(end_events)) == 0:
            util01.print_fn("This cluster is not large enough to run the job")
            break

        # decide which is the next event: start or end  ?
        start_time = sys.maxsize
        if len(JOBS01.job_events) > 0:
            start_event = JOBS01.job_events[0]
            start_time = start_event['time']
            
        end_time = sys.maxsize  # python3改 之前是sys.maxint
        if len(end_events) > 0:
            end_event = end_events[0]
            end_time = end_event['time']

        if end_time < start_time:
            event_time = end_time
            event = end_events[0]
        elif end_time > start_time:
            event_time = start_time
            # print("start-time %d, end_time %d" % (start_time, end_time))
            event = JOBS01.job_events.pop(0)
        elif end_time == start_time and end_time != sys.maxsize:
            event_time = start_time
            event = JOBS01.job_events.pop(0)
            event['end_jobs'] = end_events[0]['end_jobs']

        # 如果等于，则程序继续一下；如果不等于，则报错
        assert event_time == event['time']

        # for ending jobs, release gpu
        if 'end_jobs' in event:
            for e_job in event['end_jobs']:
                # job completes
                CLUSTER01.release_job_res(e_job)
                # CLUSTER.release_gpus(e_job)
                LOG01.job_complete(e_job, event_time)
                JOBS01.runnable_jobs.remove(e_job)

        # for new-start jobs, add to runnable
        if 'start_jobs' in event:
            for s_job in event['start_jobs']:
                # add into runnable list with pending status
                JOBS01.move_to_runnable(s_job)
                s_job['remaining_time'] = s_job['duration']
                s_job['remaining_gputime'] = int(s_job['remaining_time']
                                                 * s_job['num_gpu'])
                util01.print_fn('---- job[%d] is added'
                                % s_job['job_idx'])

        for rjob in JOBS01.runnable_jobs:
            if 'RUNNING' == rjob['status']:
                tmp = int(event_time - rjob['last_check_time']) 
                rjob['total_executed_time'] = int(rjob['total_executed_time'] + tmp)
                rjob['last_check_time'] = event_time
                rjob['remaining_time'] = int(rjob['duration']
                                             - rjob['total_executed_time'])
                if gputime:
                    rjob['remaining_gputime'] = int(rjob['remaining_time']
                                                    * rjob['num_gpu'])
            elif 'PENDING' == rjob['status']:
                tmp = int(event_time - rjob['last_check_time']) 
                rjob['pending_time'] = int(rjob['pending_time'] + tmp)
                rjob['last_check_time'] = event_time
            elif 'END' == rjob['status']: #almost impossible
                JOBS01.runnable_jobs.remove(rjob)
                pass
        #sort jobs with shortest first
        # if gputime:
        #     JOBS.runnable_jobs.sort(key = lambda e:e.__getitem__('remaining_gputime'))
        # else:
        #     JOBS.runnable_jobs.sort(key = lambda e:e.__getitem__('remaining_time'))

        JOBS01.runnable_jobs.sort(key=lambda e: e.__getitem__('num_gpu'))
        run_jobs = list()
        preempt_jobs = list()
        #scan / execute jobs one by one
        CLUSTER01.empty_infra()
        for rjob in JOBS01.runnable_jobs:
            if 'RUNNING' == rjob['status']:
                if 'placements' in rjob: 
                    del rjob['placements'][:]
            ret = placement.base.try_get_job_res(rjob,FLAGS01,CLUSTER01) 
            if True == ret:
                # rjob['status'] = 'RUNNING'
                # if 0 == rjob['start_time'] and 0 != rjob['submit_time']:
                #     rjob['start_time'] = event_time
                if sys.maxsize == rjob['start_time']:
                    rjob['start_time'] = event_time
                if rjob['status'] == 'PENDING':
                    run_jobs.append(rjob)

            else:
                # rjob['status'] = 'PENDING'
                if rjob['status'] == 'RUNNING':
                    preempt_jobs.append(rjob)
                continue

        for job in preempt_jobs:
            job['status'] = 'PENDING'
            job['preempt'] = int(job['preempt'] + 1)
            job['duration'] += 10  # 抢占开销
        for job in run_jobs:
            job['status'] = 'RUNNING'
            job['resume'] = int(job['resume'] + 1)
            job['duration'] += 10  # 抢占开销

        # get the next end_event
        del end_events[:]
        for rjob in JOBS01.runnable_jobs:
            if 'RUNNING' == rjob['status']:
                end_time = int(event_time + rjob['remaining_time'])
                tmp_dict = util01.search_dict_list(end_events, 'time', end_time)
                if tmp_dict == None:
                    #not found, add the time into to job_events
                    tmp_dict = dict()
                    tmp_dict['time'] = end_time
                    tmp_dict['end_jobs'] = list()
                    tmp_dict['end_jobs'].append(rjob)
                    end_events.append(tmp_dict)
                else:
                    tmp_dict['end_jobs'].append(rjob)
        end_events.sort(key = lambda e:e.__getitem__('time'))


        LOG01.checkpoint(event_time)
