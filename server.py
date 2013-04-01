#!/usr/bin/python

import bluelet, sys, time, itertools, functools, tempfile, pager, os
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
    def __init__(self, command, working_dir = '.', unique_dir = False):
        self.id, Job.id = Job.id, Job.id + 1
        self.command, self.working_dir, self.unique_dir = command, working_dir, unique_dir
        self.is_executing = False
        self.output_queue = []
        self.follow = False
    def __repr__(self):
        return '<remote command: %s>' % self.command
    def set_executing(self):
        self.is_executing = True
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
    def apart(self):
        return [Job(job, self.working_dir, self.unique_dir) for job in self.jobs]
    def __repr__(self):
        return '< %d jobs of\n%s >' % (len(self.jobs), self.jobs)

class Engine:
    def __init__(self, view_of_one, jobs_executing):
        if len(view_of_one) != 1:
            raise NotImplementedError('Engine takes a one element view')
        self.view_of_one = view_of_one
        self.id = view_of_one.targets
        self.jobs_executing = jobs_executing
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
    def start_job(self, job_id):
        self.apply(BCK.start_job, self.jobs_executing[job_id].command)
        while True:
            async = self.apply_async(BCK.remote_command, 'relay_stdout')
            while not async.ready():
                yield bluelet.null()
            poll_code, stdout_line = async.result
            job = self.jobs_executing[job_id]
            #            print('engine: follow: %s, line received: %s' % (self.jobs_executing[job_id].follow, stdout_line))
            job.output_queue.append(stdout_line.strip().decode())
            #            print('from job: %s' % '>\n<'.join(self.executing_job.output_queue))
            if job.follow and PIPE is not None:
                PIPE.write(stdout_line)
                PIPE.flush()
                #                os.write(job.follow, b'adfaidohfusfhgdsaiuyhgfsduiyfs\n')
                #                with open(job.follow, 'wb') as fd:
                #                job.follow.write(stdout_line)
                #                fd.flush()
                #                print('written %s' % stdout_line)
            if poll_code is not None:
                job.unset_executing()
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
        self.engines_idle = [Engine(self.client[id], jobs_executing) for id in self.client.ids]
        self.engines_executing = []
        self.run = True
        self.run_scheduling = False
        global PIPE
        PIPE = False

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
            else:
                recv = self.pipe.recv()
                BCK.__dict__[recv[0]](self, *recv[1:])
        yield bluelet.end()
    def schedule_job(self):
        unlucky = self.engines_idle.pop()
        self.engines_executing.append(unlucky)
        _,lucky = self.jobs_idle.popitem()
        self.jobs_executing[lucky.id] = lucky
        yield bluelet.call(unlucky.start_job(lucky.id))        
        del self.jobs_executing[lucky.id]
        self.jobs_finished[lucky.id] = lucky
        unlucky.executing_job = None
        self.engines_executing.remove(unlucky)
        self.engines_idle.append(unlucky)
        yield bluelet.end()
    def stop_monitor(self, ack = True):
        self.run = False
        for engine in self.engines_executing:
            engine.apply(BCK.remote_command, 'stop_process')
            print('%s stopped' % engine)
        if ack: self.pipe.send('stop_monitor')
    def shutdown_all(self):
        self.stop_monitor(ack = False)
        self.client.shutdown(hub = True)
        self.pipe.send('shutdown_all')
    def list_engines(self):
        pr = '''
--- executing ---
%s
--- idle      ---
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
    def follow(self, id, fifo):
        job = None
        if id in self.jobs_executing:
            job = self.jobs_executing[id]
            global PIPE
            PIPE = open(fifo, 'wb')
            job.follow = True
            self.jobs_executing[id] = job
            self.pipe.send('follow')
        if id in self.jobs_finished:
            job = self.jobs_finished[id]
            print('job finished %s' % ('\-'.join(self.jobs_finished[id].output_queue)))
            global PIPE
            PIPE = open(fifo, 'wb')
            self.pipe.send('follow')
            for line in job.output_queue:
                print(line)
                PIPE.write(line + b'\n')
            PIPE.flush()
            #            PIPE.close()
            print('content passed')
        if job is None: raise ValueError('follow: \'job\' cannot be None')
    def unfollow(self, id):
        if id in self.jobs_executing:
            job = self.jobs_executing[id]
            job.follow = False
            self.jobs_executing[id] = job
            self.pipe.send('unfollow')
        else: raise ValueError('unfollow: \'job\' cannot be None')
        
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
            return 0, b''
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
        manager = Manager()
        self.jobs_idle = manager.dict()
        self.jobs_executing = manager.dict()
        self.jobs_finished = manager.dict()
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
        self.pipe.send(('stop_monitor',))
        if self.pipe.recv() != 'stop_monitor':
            raise ValueError('response - query kind differs')
        self.thread_bck.join()
    @toplevel_and_alive
    def list_engines(self):
        self.pipe.send(('list_engines',))
        if self.pipe.recv() != 'list_engines':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def start_sched(self):
        self.pipe.send(('start_scheduling',))
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
        self.pipe.send(('stop_scheduling',))
        if self.pipe.recv() != 'stop_scheduling':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def add_job(self, jobs):
        if isinstance(jobs, Jobs):
            for job in jobs.apart():
                self.jobs_idle[job.id] = job
        else: raise NotImplementedError('add_job only accepts \'Jobs\'')
    @toplevel_and_alive
    def status_report(self):
        self.pipe.send(('status_report',))
        if self.pipe.recv() != 'status_report':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def shutdown_all(self):
        self.pipe.send(('shutdown_all',))
        if self.pipe.recv() != 'shutdown_all':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def follow_job(self, id):
        with tempfile.TemporaryDirectory() as td:
            fifo = td + '/pager.fifo'
            os.mkfifo(fifo)
            if id in self.jobs_executing or id in self.jobs_finished:
                self.pipe.send(('follow', id, fifo))
            else: print('job id not found')
            r = open(fifo, 'rb')
            if self.pipe.recv() != 'follow': raise ValueError('following job failed')
            print('th1 before Pager')
            pager.Pager(r).run()
            if id in self.jobs_executing:
                self.pipe.send(('unfollow', id))
                if self.pipe.recv() != 'unfollow': raise ValueError('unfollowing job failed')
            os.unlink(fifo)
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
