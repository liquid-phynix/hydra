#!/usr/bin/python

import bluelet, sys, time, itertools, functools
from multiprocessing import Process, Pipe, Manager

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

class PrimitiveJob:
    pass

class Fmt:
    """example use:
    Fmt('%a . %b . %c').sub(a = [1,2,3], b = [4,5,6]).sub(c = [7.8]) =>
    ['1 . 4 . 7', '1 . 4 . 8', '2 . 5 . 7', '2 . 5 . 8', '3 . 6 . 7', '3 . 6 . 8']
    """
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
    def __init__(self, command, working_dir, unique_dir):
        self.command, self.working_dir, self.unique_dir = command, working_dir, unique_dir
    def __repr__(self):
        return '< remote command: %s >' % self.command
    
class Jobs:
    """represents multiple jobs with common properties"""
    def __init__(self, jobs, working_dir = '.', unique_dir = False):
        """example use:
        Jobs('/path/to/executable p1 p2', working_dir = ...)
        Jobs(Fmt(...), working_dir = ...)
        """
        if not isinstance(jobs, list): jobs = [jobs]
        self.jobs, self.working_dir, self.unique_dir = jobs, working_dir, unique_dir
    def apart(self):
        return [Job(job, self.working_dir, self.unique_dir) for job in self.jobs]
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
    def __repr__(self):
        return '<%s : %d>' % (self.hostname, self.id)
    def start_job(self, executing_job):
        self.executing_job = executing_job
        self.apply(BCK.start_job, executing_job.command)
        
class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass

class BCK:
    def __init__(self, pipe, profile, jobs_idle, jobs_finished):
        from IPython.parallel import Client
        self.jobs_idle = jobs_idle
        self.jobs_finished = jobs_finished
        self.pipe = pipe
        self.profile = profile
        self.client = Client(profile = profile)
        self.engines_idle = [Engine(self.client[id]) for id in self.client.ids]
        self.engines_executing = []
        self.run = True
        self.run_scheduling = False

    def app(self):
        print('client: %d engines, with id\'s %s are up' % (len(self.client.ids), self.client.ids))       
        for engine in self.engines_idle:
            print('id %d on %s' % (engine.id, engine.hostname))

        self.pipe.send(Ready('all systems are a go'))
         # for engine in self.engines:
        #     yield bluelet.spawn(self.process_job(engine))
        yield bluelet.spawn(self.check_finished_jobs())
        while self.run:
            if not self.pipe.poll():
                yield bluelet.null()
                if self.run_scheduling:
                    # do the scheduling
                    if len(self.engines_idle) > 0 and len(self.jobs_idle) > 0:
                        self.schedule_job()
            else:
                command = self.pipe.recv()
                BCK.__dict__[command](self)
        yield bluelet.end()
    def bluelet(self):
        bluelet.run(self.app())
    def schedule_job(self):
        unlucky = self.engines_idle.pop()
        lucky = self.jobs_idle.pop()
        self.engines_executing.append(unlucky)
        unlucky.start_job(lucky)
        #        print('starting %s on %s' % (lucky, unlucky))
    def stop_monitor(self):
        self.run = False
        for engine in self.engines_executing:
            engine.apply(BCK.remote_command, 'stop_process')
            print('app::stop_monitor::stopped<%d>' % engine.id)
        self.pipe.send('stop_monitor')
    def list_engines(self):
        pr = '''
    executing:
%s
    idle:
%s''' % ('\n'.join(map(str,self.engines_executing)), '\n'.join(map(str, self.engines_idle)))
        print(pr)
        self.pipe.send('list_engines')
    def check_finished_jobs(self):
        while self.run:
            for engine in self.engines_executing:
                retcode = engine.apply(BCK.poll_job)
                if  retcode is not None:
                    print('\njob %s finished on engine %s' % (engine.executing_job, engine))
                    self.jobs_finished.append(engine.executing_job)
                    engine.executing_job = None
                    self.engines_executing.remove(engine)
                    self.engines_idle.append(engine)
                    break
            yield bluelet.null()
    def start_scheduling(self):
        self.status_report(ack = False)
        self.run_scheduling = True
        self.pipe.send('start_scheduling')
    def stop_scheduling(self):
        self.status_report(ack = False)
        self.run_scheduling = False
        self.pipe.send('stop_scheduling')
    def process_job(self, engine):
        while self.run:
            ar = engine.apply(BCK.remote_command, 'relay_stdout')
            while not ar.ready():
                yield bluelet.null()
                #            self.pipe.send(Msg(ar.result))
        yield bluelet.end()
    def status_report(self, ack = True):
        print('%d executing job(s)' % sum(engine.executing_job is not None for engine in self.engines_executing))
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
    def poll_job(*params):
        global job
        print('poll %s' % job.poll())
        return job.poll()
    @staticmethod
    def remote_command(command):
        global job
        if job is None: return
        if command == 'relay_stdout':
            return job.stdout.readline()
        elif command == 'stop_process':
            job.kill()
            ret = job.wait()
            job = None
            return ret
        else:
            raise ValueError('Wrong command <%s>' % command)
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
        job_manager = Manager()
        self.jobs_idle = job_manager.list()
        self.jobs_finished = job_manager.list()
        self.thread_bck = Process(target = lambda: BCK(pipe_bck, profile, self.jobs_idle, self.jobs_finished).bluelet())
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
    def stop_sched(self):
        self.pipe.send('stop_scheduling')
        if self.pipe.recv() != 'stop_scheduling':
            raise ValueError('response - query kind differs')
    @toplevel_and_alive
    def add_job(self, jobs):
        if isinstance(jobs, Jobs):
            for job in jobs.apart():
                self.jobs_idle.append(job)
        elif isinstance(job, Job):
            self.jobs_idle.append(job)            
        else:
            raise NotImplementedError('only Jobs is implemented yet')
    @toplevel_and_alive
    def status_report(self):
        self.pipe.send('status_report')
        if self.pipe.recv() != 'status_report':
            raise ValueError('response - query kind differs')
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
    if _hq.thread_bck.is_alive():
        stop_monitor()
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
