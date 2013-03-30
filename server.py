#!/usr/bin/python

import bluelet, time, itertools
import multiprocessing
from multiprocessing import Process, Pipe
from functools import reduce

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
    def __init__(self, fmt):
        self.saturation, self.fmts = fmt.count('%'), [fmt.replace('%', '^')]
    def sub(self, **kwargs):
        trans_fmt = (reduce(lambda s,k: s.replace('^'+k,'%('+k+')s'), kwargs, fmt) for fmt in self.fmts)
        repl = [dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])]
        self.fmts = [fmt % pdict for fmt in trans_fmt for pdict in repl]
        self.saturation -= len(kwargs)
        return self.fmts if self.saturation == 0 else self
    def __repr__(self):
        return '<\n' + ',\n'.join(self.fmts) + '\n>'

class Jobs:
    """use it like this:
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', 'p1', [1, 2, 3], 'p2', [4, 5, 6], 'p3', [8, 9])
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', ('p1', 'p2'), ([1, 2, 3], [4, 5, 6]), 'p3', [8, 9])
    j.show()
    """
    @staticmethod
    def convert_next_param(s):
        try:
            percent = s.index('%')
            try: end = s.index(' ', percent)
            except ValueError: end = len(s)
            return s[:percent] + '{' + s[percent+1:end] + '}' + s[end:len(s)]
        except ValueError: return s
    def __init__(self, fmt):
        self.fmts = [fmt]
    def sub(**kwargs):
        def trans_fmt():
            for fmt in self.fmts:
                for k in kwargs:
                    fmt = fmt.replace('%'+k,'%('+k+')s')
                yield ftm
        self.fmts = [fmt for fmt in trans_fmt()]
        repl = dict(kwtpl) for kwtpl in zip(*[[(k,v) for v in vs] for k,vs in kwargs.items()])
        self.fmts = [ftm % pdict for fmt in self.fmt for pdict in repl]
        # [(k,v)  for v in vs]
        # rep_dict = dict()
        # self.fmts = [fmt % {k : v} for fmt in self.fmts]
            
            
        #     self.fmts = [fmt.replace('%'+k,'{'+k+'}').format for fmt in self.fmts]
        
        def gen():
            for (pname, values) in zip(args[::2], args[1::2]):
                if isinstance(pname, tuple):
                    yield [x for x in zip(*[[(_pname, value) for value in _values] for (_pname, _values) in zip(pname, values)])]
                else:
                     yield [(pname, value) for value in values]
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


class Job:
    """use it like this:
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', 'p1', [1, 2, 3], 'p2', [4, 5, 6], 'p3', [8, 9])
    j = Job('lilscript --p1=%(p1)s --p2=%(p2)s --p3=%(p3)s', ('p1', 'p2'), ([1, 2, 3], [4, 5, 6]), 'p3', [8, 9])
    j.show()
"""    
    def __init__(self, fmt, *args):
        def gen():
            for (pname, values) in zip(args[::2], args[1::2]):
                if isinstance(pname, tuple):
                    yield [x for x in zip(*[[(_pname, value) for value in _values] for (_pname, _values) in zip(pname, values)])]
                else:
                     yield [(pname, value) for value in values]
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

class Engine:
    def __init__(self, view_of_one):
        if len(view_of_one) != 1:
            raise NotImplementedError('Engine takes a one element view')
        self.view_of_one = view_of_one
        self.id = view_of_one.targets
        self.executing_job = None
    def apply(self, f, *args, **kwargs):
        return self.view_of_one.apply_sync(f, *args, **kwargs)
        
class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass
    
class Manager:
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

    def app(self):
        print('client: %d engines, with id\'s %s are up' % (len(self.client.ids), self.client.ids))
        def set_job_global():
            global job
            job = None
        for engine in self.engines:
            print('id %d on %s' % (engine.id, engine.apply(Manager.remote_system_command, 'hostname')))
            engine.apply(set_job_global) # initialize global 'job'
            
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
        for engine in self.engines:
            engine.apply(Manager.remote_command, 'stop_process')
            print('app::stop_monitor::stopped<%d>' % engine.id)
    def list_engines(self):
        self.pipe.send(str(self.engines))
    def start_scheduling(self):
        pass
    def process_job(self, engine):
        while self.run:
            ar = engine.apply(Manager.remote_command, 'relay_stdout')
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
        print('using profile: %s' % profile)
        self.pipe, pipe_bck = Pipe()
        job_manager = multiprocessing.Manager()
        self.jobs_idle = job_manager.list()
        self.jobs_finished = job_manager.list()
        self.thread_bck = Process(target = lambda: Manager(pipe_bck, profile, self.jobs_idle, self.jobs_finished).bluelet())
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
        self.thread_bck.join()
    @toplevel_and_alive
    def list_engines(self):
        self.pipe.send('list_engines')
        print(self.pipe.recv())
    @toplevel_and_alive
    def start_sched(self):
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
