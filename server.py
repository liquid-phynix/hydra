#!/usr/bin/python

import bluelet, sys, time, itertools, functools, pager
from multiprocessing import Process, Pipe, Manager

from numpy import random

# add_job(Jobs(Fmt('sleep %p').sub(p = map(str,random.random_integers(1,10,20).tolist()))));

bluelet.null = (lambda f: lambda: (time.sleep(0.005), (yield f())))(bluelet.null)

usage = \
"""usage: %s --profile=<profile>
where <profile> corresponds to ~/.ipython/profile_<profile>"""

# decorator
def toplevel_and_alive(method):
    def _method(*args, **kwargs):
        if _hq.thread_bck.is_alive(): method(_hq, *args, **kwargs)
        else: print('background thread is not alive')
    globals()[method.__name__] = _method
    return method

class Fmt:
    """example use:
    Fmt('%a . %b . %c').sub(a = [1,2,3], b = [4,5,6]).sub(c = [7.8]) =>
    ['1 . 4 . 7', '1 . 4 . 8', '2 . 5 . 7', '2 . 5 . 8', '3 . 6 . 7', '3 . 6 . 8']"""
    def __init__(self, fmt):
        self.saturation, self.fmts = fmt.count('%'), [fmt.replace('%', '^')]
    def sub(self, **kwargs):
        trans_fmt = (functools.reduce(lambda s,k: s.replace('^'+k,'%('+k+')s'), kwargs, fmt) for fmt in self.fmts)
        repl = [dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])]
        self.fmts = [fmt % pdict for fmt in trans_fmt for pdict in repl]
        self.saturation -= len(kwargs)
        return self.fmts if self.saturation == 0 else self
    def __repr__(self):
        return '<\n' + ',\n'.join(self.fmts) + '\n>'

class Job:
    """represents a fully parameterized single job"""
    id = 1 # instance counter for comparison purposes
    def __eq__(self, other):
        return self.id == other.id
    def __init__(self, command, queue, working_dir = '.', unique_dir = False):
        self.id, Job.id = Job.id, Job.id + 1
        self.command, self.working_dir, self.unique_dir = command, working_dir, unique_dir
        self.is_executing = False
        self.output_queue = queue
    def __repr__(self):
        return '<remote command: %s>' % self.command
    def set_executing(self, managed_list):
        self.is_executing = True
        self.output_queue = managed_list
    def unset_executing(self):
        self.is_executing = False
        #        self.output_queue = ['some', 'stuff'] #self.output_queue.copy()

class Jobs:
    """represents multiple jobs with common properties"""
    def __init__(self, jobs, working_dir = '.', unique_dir = False):
        """example use:
        Jobs('/path/to/executable p1 p2', working_dir = ...)
        Jobs(Fmt(...), working_dir = ...)"""
        if not isinstance(jobs, list): jobs = [jobs]
        self.jobs, self.working_dir, self.unique_dir = jobs, working_dir, unique_dir
    def apart(self, manager):
        return [Job(job, manager.list(), self.working_dir, self.unique_dir) for job in self.jobs]
    def __repr__(self):
        return '< %d jobs of\n%s >' % (len(self.jobs), self.jobs)

class Engine:
    def __init__(self, view_of_one):
        if len(view_of_one) != 1:
            raise NotImplementedError('Engine takes a one element view')
        self.view_of_one = view_of_one
        self.id = view_of_one.targets
        self.executing_job = None
        self.hostname = self.apply(BCK.remote_system_command, 'hostname')
        def set_job_global():
            global job
            job = None
        self.apply(set_job_global) # initialize global 'job'        
    def apply(self, f, *args, **kwargs):
        return self.view_of_one.apply_sync(f, *args, **kwargs)
    def apply_async(self, f, *args, **kwargs):
        return self.view_of_one.apply_async(f, *args, **kwargs)
    def __repr__(self):
        return '<%s : %d>' % (self.hostname, self.id)
    def start_job(self, executing_job):
        self.executing_job = executing_job
        self.apply(BCK.start_job, executing_job.command)
        while True:
            async = self.apply_async(BCK.remote_command, 'relay_stdout')
            while not async.ready():
                yield bluelet.null()
            poll_code, stdout_line = async.result
            self.executing_job.output_queue.append(stdout_line.strip().decode())
            if poll_code is not None:
                self.executing_job.unset_executing()
                break
        yield bluelet.end()

class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass

class BCK:
    def __init__(self, pipe, profile, jobs_idle, jobs_executing, jobs_finished):
        from IPython.parallel import Client
        self.jobs_idle, self.jobs_executing, self.jobs_finished = jobs_idle, jobs_executing, jobs_finished
        self.pipe, self.profile = pipe, profile
        self.client = Client(profile = profile)
        self.engines_idle = [Engine(self.client[id]) for id in self.client.ids]
        self.engines_executing = []
        self.run = True
        self.run_scheduling = False

    def app(self):
        print('client: %d engines, with id\'s %s are up' % (len(self.client.ids), self.client.ids))       
        for engine in self.engines_idle: print('id %d on %s' % (engine.id, engine.hostname))
        self.pipe.send(Ready('all systems are a go'))
        yield bluelet.call(self.scheduler())
    def bluelet(self):
        bluelet.run(self.app())
    def scheduler(self):
        while self.run:
            if not self.pipe.poll():
                yield bluelet.null()
                if self.run_scheduling:
                    if len(self.engines_idle) > 0 and len(self.jobs_idle) > 0:
                        yield bluelet.spawn(self.schedule_job())
            else: BCK.__dict__[self.pipe.recv()](self)
        yield bluelet.end()
    def schedule_job(self):
        unlucky = self.engines_idle.pop()
        self.engines_executing.append(unlucky)

        print('for')
        for _,j in self.jobs_idle.items():
            print(type(j.output_queue))
        print('end for')

        
        _,lucky = self.jobs_idle.popitem()

                    
        print('schedule_job')
        print(type(lucky.output_queue))
        print(type(self.jobs_executing))
        self.jobs_executing[lucky.id] = lucky

        print(type(self.jobs_executing[lucky.id].output_queue))
        
        yield bluelet.call(unlucky.start_job(lucky))
        
        del self.jobs_executing[lucky.id]
        self.jobs_finished[lucky.id] = lucky
        unlucky.executing_job = None
        self.engines_executing.remove(unlucky)
        self.engines_idle.append(unlucky)
        
        yield bluelet.end()
        #        print('starting %s on %s' % (lucky, unlucky))
    def stop_monitor(self, ack = True):
        self.run = False
        for engine in self.engines_executing:
            #            print('engine: %s stop' % engine)
            engine.apply(BCK.remote_command, 'stop_process')
            print('app::stop_monitor::stopped<%d>' % engine.id)
        if ack: self.pipe.send('stop_monitor')
    def shutdown_all(self):
        self.stop_monitor(ack = False)
        self.client.shutdown(hub = True)
        self.pipe.send('shutdown_all')
    def list_engines(self):
        pr = '''
--- executing ---
%s
---   idle    ---
%s''' % ('\n'.join(map(str,self.engines_executing)), '\n'.join(map(str, self.engines_idle)))
        print(pr)
        self.pipe.send('list_engines')
    def start_scheduling(self):
        self.status_report(ack = False)
        self.run_scheduling = True
        self.pipe.send('start_scheduling')
    def stop_scheduling(self):
        self.status_report(ack = False)
        self.run_scheduling = False
        self.pipe.send('stop_scheduling')
    def status_report(self, ack = True):
        print('%d executing job(s)' % len(self.jobs_executing))
        print('%d finished job(s)' % len(self.jobs_finished))
        print('%d idle job(s)' % len(self.jobs_idle))
        if ack: self.pipe.send('status_report')
        
################################################################################
# REMOTE
    @staticmethod
    def start_job(command):
        from subprocess import Popen, PIPE
        global job
        job = Popen(command.split(' '), stdout = PIPE)
    @staticmethod
    def remote_command(command):
        global job
        if job is None:
            return
        #            raise ValueError('\'job\' cannot be None')
        if command == 'stop_process':
            job.kill()
            ret = job.wait()
            job = None
            return ret
        if command == 'relay_stdout':
            return job.poll(), job.stdout.readline()
        else: raise ValueError('Wrong command <%s>' % command)
    @staticmethod
    def remote_system_command(cmd):
        import subprocess
        p = subprocess.Popen(cmd.split(' '), stdout = subprocess.PIPE)
        return p.stdout.readall().strip().decode()
# END REMOTE
################################################################################

class HQ:
    def __init__(self, profile):
        print('using profile: %s' % profile)
        self.pipe, pipe_bck = Pipe()
        self.manager = Manager()
        self.jobs_idle = self.manager.dict()
        self.jobs_executing = self.manager.dict()
        self.jobs_finished = self.manager.dict()
        self.thread_bck = Process(target = lambda:
                                  BCK(pipe_bck, profile,
                                      self.jobs_idle,
                                      self.jobs_executing,
                                      self.jobs_finished).bluelet())
        self.thread_bck.start()
    def ready(self):
        while True:
            msg = self.pipe.recv()
            print(msg.msg)
            if isinstance(msg, Ready):
                break
################################################################################
# LOCAL
    @toplevel_and_alive
    def stop_monitor(self):
        self.pipe.send('stop_monitor')
        if self.pipe.recv() != 'stop_monitor':
            raise ValueError('response - query kind differs')
        self.thread_bck.join()
    @toplevel_and_alive
    def list_engines(self):
        self.pipe.send('list_engines')
        if self.pipe.recv() != 'list_engines':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def start_sched(self):
        self.pipe.send('start_scheduling')
        if self.pipe.recv() != 'start_scheduling':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def list_jobs(self):
        pr = '''
--- executing ---
%s
--- idle      ---
%s
--- finished  ---
%s''' % ('\n'.join('job <%d> => %s' % (k, str(v)) for k, v in self.jobs_executing.items()),
         '\n'.join('job <%d> => %s' % (k, str(v)) for k, v in self.jobs_idle.items()),
         '\n'.join('job <%d> => %s' % (k, str(v)) for k, v in self.jobs_finished.items()),)
        print(pr)
    @toplevel_and_alive
    def stop_sched(self):
        self.pipe.send('stop_scheduling')
        if self.pipe.recv() != 'stop_scheduling':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def add_job(self, jobs):
        if isinstance(jobs, Jobs):
            for job in jobs.apart(self.manager):
                print('inside')
                print(type(self.manager.list()))
                print(type(job.output_queue))
                self.jobs_idle[job.id] = job
                print(type(self.jobs_idle[job.id]))
                print(type(self.jobs_idle[job.id].output_queue))
        else: raise NotImplementedError('add_job only accepts \'Jobs\'')
            
        print('addjob for')
        for _,j in self.jobs_idle.items():
            print(type(j))
            print(type(j.output_queue))
        print('addjob end for')

            
    @toplevel_and_alive
    def status_report(self):
        #        if len(self.jobs_executing) > 0:
            #            print('HQ')
            #            print(type(self.jobs_executing[0].output_queue))
        self.pipe.send('status_report')
        if self.pipe.recv() != 'status_report':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def shutdown_all(self):
        self.pipe.send('shutdown_all')
        if self.pipe.recv() != 'shutdown_all':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def follow_job(self, id):
        if id in self.jobs_executing: 
            print(type(self.jobs_executing))
            print(type(self.jobs_executing[id]))
            print(type(self.jobs_executing[id].output_queue))
            raise ValueError
            pager.Pager(self.jobs_executing[id].output_queue).run()
        elif id in self.jobs_finished:
            pager.Pager(self.jobs_finished[id].output_queue).run()
        else: print('job id not found')
# END LOCAL
################################################################################

def main():
    script_name = sys.argv[0].split('/')[-1]
    profile = None
    for arg in sys.argv[1:]:
        if arg.startswith('--profile='):
            profile = arg.partition('--profile=')[2]
            break
    if profile is None:
        print(usage % script_name)
        sys.exit(-1)
    global _hq
    _hq = HQ(profile)
    _hq.ready()

if __name__ == '__main__':
    main()
    import IPython
    ipshell = IPython.frontend.terminal.embed.InteractiveShellEmbed()
    ipshell.confirm_exit, ipshell.display_banner = False, False
    ipshell()
    if _hq.thread_bck.is_alive(): stop_monitor()
    sys.exit(0)

# ipcluster start --profile=ssh
# > ipcluster_config.py
# c = get_config()
# c.IPClusterStart.controller_launcher_class = 'LocalControllerLauncher'
# c.IPClusterStart.engine_launcher_class = 'SSHEngineSetLauncher'
# c.IPClusterEngines.copy_config_files = False
# c.LocalControllerLauncher.controller_args = ['--log-to-file', '--log-level=DEBUG', '--ip=10.0.0.254']
# c.SSHEngineSetLauncher.engine_args = ['--tcp=10.0.0.254', '--log-to-file', '--log-level=DEBUG']
# c.SSHEngineSetLauncher.engines = {
#     'gpu02' : 1,
#     'gpu03' : 1,
#     'gpu07' : 1,
#     'gpu08' : 1,
#     'gpu09' : 1,
#     'gpu10' : 1
# }

# import multiprocessing as m
# import subprocess as s
# import tempfile, os, time
# def less():
#     with tempfile.TemporaryDirectory() as td:
#         fifo_name = td + '/less.fifo'
#         os.mkfifo(fifo_name)
#         def target():
#             with open(fifo_name, 'wb') as w:
#                 while True:
#                     w.write((str(time.time()) + '\n').encode())
#                     w.flush()
#                     time.sleep(0.1)
#         p = m.Process(target = target)
#         p.start()
#         os.system('unbuffer less -f %s' % fifo_name)
#         os.unlink(fifo_name)
#         p.terminate()
#         p.join()


# def app():
#     def task():
#         i = 0
#         while i < 100:
#             print(i)
#             i += 1
#             yield bluelet.null()
#         yield bluelet.end(i)
#     # def spawn():
#     #     ret = yield bluelet.spawn(task())
#     #     yield bluelet.end(ret)
#     ret = yield bluelet.call(task())
#     print('ret: %s' % ret)
#     yield bluelet.end(ret)



# class C:
#     id = 1
#     def __init__(self, lp):
#         self.lp = lp
     
# import multiprocessing as man
# m1 = man.Manager()
# m2 = man.Manager()

# dp = m1.dict()
# lp = m1.list()

# c = C(m2.list())

# dp[0] = c
# lp.append(c)

# print(type(dp[0].lp))
# print(type(lp[0].lp))
