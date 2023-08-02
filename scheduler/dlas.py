import utils.util01 as util
import sys
import math
import copy

def get_gittins_index(a,JOBS):
    job_info = JOBS.job_dist_data
    # print("job_info: " , job_info)  # debug

    if a > job_info['data'][-2]:
        return 0
    idx = next(x[0] for x in enumerate(job_info['data']) if x[1] > a)
    return job_info['gittins'][idx]

def dlas_sim_jobs(gputime,solve_starvation,JOBS,CLUSTER,LOG,FLAGS):
    '''
    Job's executed time -- priority queue
    Q0:[0, 30min)
    Q1:[30min,1h)
    Q2:[1h, 2h)
    Q3:[2h, 00)

    in each queue, jobs are scheduled in fit-first with FIFO
    how to avoid starvation?

    TODO:  2. add move_back for avoiding starvation
    '''
    print("solve_starvation:   ",solve_starvation)  # debug
    end_events = list()  # 存放接下来第一个要结束的job信息
    next_job_jump = sys.maxsize
    while (len(JOBS.job_events) + len(JOBS.runnable_jobs)) > 0:
        if (len(JOBS.job_events) + len(end_events)) == 0:
            util.print_fn("This cluster is not large enough to run the job")
            break

        # decide which is the next event: start or end  ?
        start_event = None
        start_time = sys.maxsize
        if len(JOBS.job_events) > 0:
            start_event = JOBS.job_events[0]
            start_time = start_event['time']
        end_event = None
        end_time = sys.maxsize
        if len(end_events) > 0:
            end_event = end_events[0]
            end_time = end_event['time']

        event_time = sys.maxsize
        event = dict()
        event['time'] = sys.maxsize
        if end_time < start_time:
            event_time = end_time
            event = end_event
        elif end_time > start_time:
            event_time = start_time
            # event = JOBS.job_events.pop(0)
            event = start_event
        elif end_time == start_time and end_time != sys.maxsize:
            event_time = start_time
            # event = JOBS.job_events.pop(0)
            event = start_event
            event['end_jobs'] = end_events[0]['end_jobs']

        assert event_time == event['time']

        # decide if job_jump first or (start/end) first
        # 除了像shortest里面一样比较谁先开始/结束，还要跟下一个降级事件发生来做比较
        if event_time > next_job_jump:
            event_time = next_job_jump
            event = dict()

        util.print_fn('--------------------------------- Handle event[time %d]------------------------------------' % event_time)
        # for ending jobs, release gpu
        print("event:   ", event)  # debug
        if 'end_jobs' in event:
            for e_job in event['end_jobs']:
                CLUSTER.release_job_res(e_job)
                LOG.job_complete(e_job, event_time)
                # util.print_fn('---- job[%d] is completed' % e_job['job_idx'])
                JOBS.runnable_jobs.remove(e_job)
                JOBS.queues[e_job['q_id']].remove(e_job)

        # for new jobs, append to runnable jobs with pending status
        if 'start_jobs' in event:
            for s_job in event['start_jobs']:
                JOBS.move_to_runnable(s_job)
                s_job['q_id'] = 0  # any new start job should be in Q0
                JOBS.queues[0].append(s_job)
                util.print_fn('---- job[%d] is added' % s_job['job_idx'])
            # pop start event
            JOBS.job_events.pop(0)

        # 对于runnable_jobs的每个job，依次update executed_time
        # 作用是重新计算每个Q里面的job，即给每个job重新排优先级
        for rjob in JOBS.runnable_jobs:  # 不管running的还是pending的，都会在runnable_jobs里面
            if 'RUNNING' == rjob['status']:
                tmp = int(event_time - rjob['last_check_time'])
                rjob['total_executed_time'] = int(rjob['total_executed_time'] + tmp)
                rjob['executed_time'] = int(rjob['executed_time'] + tmp)  # decide job priority queue
                rjob['last_check_time'] = event_time

                # check demotion  降级检查
                j_gt = 0  # 执行时间作为降级的依据
                if gputime:
                    j_gt = int(rjob['executed_time'] * rjob['num_gpu'])
                else:
                    j_gt = int(rjob['executed_time'])
                cur_qid = rjob['q_id']
                if cur_qid < int(JOBS.num_queue - 1):  # not for the last queue
                    if j_gt >= JOBS.queue_limit[cur_qid]:  # 换队列的情况
                        rjob['q_id'] = int(cur_qid + 1)  # 降一级
                        JOBS.queues[rjob['q_id']].append(rjob)
                        JOBS.queues[cur_qid].remove(rjob)
                        print("job %d demote to Q%d" % (rjob['job_idx'], rjob['q_id']))

                if FLAGS.schedule == 'dlas-gpu-gittins':
                    # rjob['rank'] = cal_r_gittins_index(JOBS.job_dist_data, j_gt)
                    rjob['rank'] = get_gittins_index(j_gt,JOBS)

            elif 'PENDING' == rjob['status']:
                tmp = int(event_time - rjob['last_check_time'])
                rjob['last_check_time'] = event_time
                rjob['pending_time'] = int(rjob['pending_time'] + tmp)  # this is the total pending_time
                if rjob['executed_time'] > 0:  # if not started yet, job is always in Q0 and no need to push_back
                    rjob['last_pending_time'] = int(rjob['last_pending_time'] + tmp)  # this is the total pending_time
                # Q0 job no need to push_back, and must be a runned
                if solve_starvation > 0 and rjob['q_id'] > 0 and rjob['total_executed_time'] > 0 and rjob[
                    'executed_time'] > 0:
                    if rjob['last_pending_time'] >= int(rjob['executed_time'] * solve_starvation):  # 等待时间过长，快饿死了
                        rjob['executed_time'] = 0  # 不清零的话还会认为他执行时间很长，下次又降级了。这一列只对优先级计算有用，对于JCT等日志的记录没用
                        rjob['last_pending_time'] = 0
                        JOBS.queues[0].append(rjob)  # 升级，升到Q0里面
                        JOBS.queues[rjob['q_id']].remove(rjob)
                        rjob['q_id'] = 0
                        rjob['promote'] = int(rjob['promote'] + 1)

                if FLAGS.schedule == 'dlas-gpu-gittins':
                    if gputime:
                        j_gt = int(rjob['executed_time'] * rjob['num_gpu'])
                    else:
                        j_gt = int(rjob['executed_time'])
                    # rjob['rank'] = cal_r_gittins_index(JOBS.job_dist_data, j_gt)
                    rjob['rank'] = get_gittins_index(j_gt,JOBS)

            elif 'END' == rjob['status']:  # won't happen
                JOBS.runnable_jobs.remove(rjob)
                # util.print_fn('---- job[%d] completed' % rjob['job_idx'])
                pass

        # push job to their new queue
        # JOBS.update_priority_queues(gputime)

        ''' schedule jobs in each queue '''
        # empty_cluster resource
        CLUSTER.empty_infra()
        # for "count" placement
        # 用于记录哪些job的状态需要改变，即从running到pending或反之
        run_jobs = list()
        preempt_jobs = list()

        # if FLAGS.schedule == 'dlas-gpu-gittins':
        #     q = JOBS.queues[0]
        #     q.sort(key = lambda e:(e.__getitem__('rank'), e.__getitem__('r_submit_time')), reverse=True)

        # 开始调度，优先级的依据是Q
        for queue in JOBS.queues:  # 就是依次看每个Q，从Q0开始
            if FLAGS.schedule == 'dlas-gpu-gittins':
                queue.sort(key=lambda e: (e.__getitem__('rank'), e.__getitem__('r_submit_time')), reverse=True)
            for job in queue:
                if CLUSTER.free_gpu >= job['num_gpu']:  # 应该是free_gpu还是check_free_gpu()函数？
                    # should run
                    if job['status'] == 'PENDING':
                        # not running
                        run_jobs.append(job)  # run_jobs和preempt_jobs在循环前都是空的
                    CLUSTER.free_gpu = int(CLUSTER.free_gpu - job['num_gpu'])
                else:
                    # should NOT run，因为资源不够了
                    if job['status'] == 'RUNNING':
                        # running
                        preempt_jobs.append(job)  # run_jobs和preempt_jobs在循环前都是空的
                    continue
        
        #记录job状态发生了改变
        for job in preempt_jobs:
            job['status'] = 'PENDING'
            # if job['q_id'] == 0:
            #     job['preempt'] = int(job['preempt'] + 1)
            job['preempt'] = int(job['preempt'] + 1)
            job['duration'] += job['preempt'] // 6  # 抢占开销
        for job in run_jobs:
            job['status'] = 'RUNNING'
            job['resume'] = int(job['resume'] + 1)
            job['duration'] += job['resume'] // 6  # 抢占开销
            if job['start_time'] == sys.maxsize:
                job['start_time'] = event_time

        # sort based on the job start time
        # 对于每个Q，把里面pending的job都按顺序排到了队尾
        for queue in JOBS.queues:
            # job there are many students
            pending_job = list()
            for job in queue:
                # if sys.maxsize == job['start_time'] and job['status'] == 'PENDING':
                if job['status'] == 'PENDING':
                    pending_job.append(job)
                    # print(job['job_idx'])
            for job in pending_job:
                queue.remove(job)
            queue.extend(pending_job)    # extend表示在末尾一次性追加多个值


        # update end events and sort, and get the most recent one
        del end_events[:]

        # 更新end_events,寻找下一个即将结束的job
        min_end_time = sys.maxsize
        tmp_end_event = dict()
        for rjob in JOBS.runnable_jobs:
            if 'RUNNING' == rjob['status']:
                remaining_time = rjob['duration'] - rjob['total_executed_time']
                end_time = int(event_time + remaining_time)
                if end_time < min_end_time:  # 寻找runnable_JOBS中最先结束的那个
                    tmp_end_event['time'] = end_time
                    tmp_end_event['end_jobs'] = list()
                    tmp_end_event['end_jobs'].append(rjob)
                    min_end_time = end_time
                elif min_end_time == end_time:
                    tmp_end_event['end_jobs'].append(rjob)
        if min_end_time < sys.maxsize:
            end_events.append(tmp_end_event)

        # what's the closest queue_jump (demotion降级, and promotion升级) among all the jobs
        # 寻找即将发生升降级的job
        next_job_jump = sys.maxsize
        for rjob in JOBS.runnable_jobs:
            if 'RUNNING' == rjob['status']:
                qid = rjob['q_id']
                if qid < int(JOBS.num_queue - 1):
                    if gputime:
                        jump_time = int(
                            math.ceil((JOBS.queue_limit[qid] - rjob['executed_time']) / rjob['num_gpu']) + event_time)
                    else:
                        jump_time = int(JOBS.queue_limit[qid] - rjob['executed_time'] + event_time)
                    if jump_time < next_job_jump:
                        next_job_jump = jump_time

            elif 'PENDING' == rjob['status']:  # when pending job will be push back to Q0
                if solve_starvation > 0 and rjob['q_id'] > 0 and rjob['total_executed_time'] and rjob[
                    'executed_time'] > 0:
                    diff_time = int(rjob['executed_time'] * solve_starvation - rjob['last_pending_time'])
                    if diff_time > 0:
                        jump_time = int(diff_time + event_time)
                        if jump_time < next_job_jump:
                            next_job_jump = jump_time

        LOG.checkpoint(event_time)
