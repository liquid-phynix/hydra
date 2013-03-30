#!/usr/bin/python

# ez miert rossz?
# [jt for jt in itertools.product(*[((pname, value) for value in values) for (pname, values) in zip(args[::2], args[1::2])])]
        
import bluelet, time, itertools
import multiprocessing
from multiprocessing import Process, Pipe

bluelet.null = (lambda f: lambda: (time.sleep(0.005), (yield f())))(bluelet.null)
 
usage = \
"""usage: %s --profile=<profile>
where <profile> corresponds to ~/.ipython/profile_<profile>"""

# decorator
def toplevel(method):
    def _method(*args, **kwargs):
        method(_hq, *args, **kwargs)
    globals()[method.__name__] = _method
    return method

class Job:
    """use it like this:
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', 'p1', [1, 2, 3], 'p2', [4, 5, 6], 'p3', [8, 9])
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', ('p1', 'p2'), ([1, 2, 3], [4, 5, 6]), 'p3', [8, 9])
    j.show()
"""    
    def __init__(self, fmt, *args):
        def gen():
            for (pname, values) in zip(args[::2], args[1::2]):
                if isinstance(pname, tuple): yield [x for x in zip(*[[(_pname, value) for value in _values] for (_pname, _values) in zip(pname, values)])]
                else: yield [(pname, value) for value in values]
        def flatten(tpl):
            for t in tpl:
                if isinstance(t[0], tuple):
                    for tt in t: yield tt
                else: yield t        
        jobs_dicts = [dict(flatten(jt)) for jt in itertools.product(*gen())]
        self.jobs = [fmt % d for d in jobs_dicts]
        self.working_dir = '.'
        self.unique_dir = None
    def show(self):
        for job in self.jobs:
            print(job)
    def count(self):
        return len(self.jobs)
    def del_job(self, job):
        self.jobs.remove(job)

class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass

def apply(v, f, *args, **kwargs):
    if len(v) == 1: return v.apply_sync(f, *args, **kwargs)
    else: raise NotImplementedError('apply takes only a one element view')
    
class Manager:
    def __init__(self, pipe, profile, managed_jobs):
        from IPython.parallel import Client
        self.pipe = pipe
        self.profile = profile
        self.client = Client(profile = profile)
        self.engines = [self.client[id] for id in self.client.ids]
        self.run = True
        self.managed_jobs = managed_jobs
    def app(self):
        print('client: %d engines, with id\'s %s are up' % (len(self.client.ids), self.client.ids))
        def set_job_global():
            global job
            job = None
        for engine in self.engines:
            print('id %d from %s' % (engine.targets, apply(engine, Manager.remote_system_command, 'hostname')))
            # init 'job' global
            apply(engine, set_job_global)
            
        # for i, engine in enumerate(self.engines):
        #     ans = engine.apply_sync(Manager.start_job, i)
        #     self.pipe.send(Msg('job %d started, received: %s' % (i, ans)))

        self.pipe.send(Ready('all systems are a go'))
        # for engine in self.engines:
        #     yield bluelet.spawn(self.process_job(engine))
        while self.run:
            if not self.pipe.poll():
                yield bluelet.null()
            else:
                command = self.pipe.recv()
                Manager.__dict__[command](self)
        yield bluelet.end()
    def bluelet(self):
        bluelet.run(self.app())
    def stop_monitor(self):
        self.run = False
        for i, engine in enumerate(self.engines):
            apply(engine, Manager.remote_command, 'stop_process')
            print('app::stop_monitor::stopped<%d>' % i)
    def list_engines(self):
        self.pipe.send(str(self.engines))
    def start_sched(self):
        pass
    def process_job(self, engine):
        while self.run:
            ar = apply(engine, Manager.remote_command, 'relay_stdout')
            while not ar.ready():
                yield bluelet.null()
                #            self.pipe.send(Msg(ar.result))
        yield bluelet.end()
################################################################################
# REMOTE
    @staticmethod
    def start_job(num):
        from subprocess import Popen, PIPE
        global job
        job = Popen(['/home/mcstar/src/sched/lilscript.sh', str(num)], stdout = PIPE)
        return 'OK: %d' % num
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
        print('using profile *%s*' % profile)
        self.pipe, pipe_bck = Pipe()
        job_manager = multiprocessing.Manager()
        self.managed_jobs = job_manager.list()
        def bck():
            Manager(pipe_bck, profile, self.managed_jobs).bluelet()
        self.thread_bck = Process(target = bck)
        self.thread_bck.start()
    def ready(self):
        while True:
            msg = self.pipe.recv()
            print(msg.msg)
            if isinstance(msg, Ready):
                break
################################################################################
# LOCAL
    @toplevel
    def stop_monitor(self):
        if self.thread_bck.is_alive():
            self.pipe.send('stop_monitor')
            self.thread_bck.join()
    @toplevel
    def list_engines(self):
        if self.thread_bck.is_alive():
            self.pipe.send('list_engines')
            print(self.pipe.recv())
    @toplevel
    def start_sched(self):
        if self.thread_bck.is_alive():
            self.pipe.send('start_scheduling')
# END LOCAL
################################################################################

def main():
    import sys
    script_name = sys.argv[0].split('/')[-1]
    profile = None
    for arg in sys.argv[1:]:
        if arg.startswith('--profile='):
            profile = arg.partition('--profile=')[2]
            break
    if profile is None:
        print(usage % (script_name))
        sys.exit(-1)
    global _hq
    _hq = HQ(profile)
    _hq.ready()

if __name__ == '__main__':
    main()
    from IPython.config.loader import Config
    from IPython import embed
    import sys
    cfg = Config()
    cfg.TerminalInteractiveShell.confirm_exit = False
    embed(config = cfg)
    stop_monitor()
    sys.exit(0)

# ipcluster start --profile=ssh
#
# > ipcluster_config.py
#
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

