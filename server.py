#!/usr/bin/python

import bluelet, sys, time, itertools, functools, tempfile, os
from multiprocessing import Process, Pipe, Manager, SimpleQueue

from Pager import Pager
from Job import Jobs
from Fmt import Fmt
from Bck import BCK, Engine, Msg, Ready, Piper
#from numpy import random

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


class HQ:
    def __init__(self, profile):
        print('using profile: %s' % profile)
        self.pipe, pipe_bck = Pipe()
        manager = Manager()
        self.jobs_idle = manager.dict()
        self.jobs_executing = manager.dict()
        self.jobs_finished = manager.dict()
        q = SimpleQueue()
        self.pager_queue = Piper(q)
        self.thread_bck = Process(target = lambda:
                                  BCK(pipe_bck, profile,
                                      self.pager_queue,
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
        if id in self.jobs_executing or id in self.jobs_finished:
            self.pipe.send(('follow', id))
        else:
            print('job id not found')
            return
        if self.pipe.recv() != 'follow': raise ValueError('following job failed')
        Pager(self.pager_queue).run()
        if id in self.jobs_executing:
            self.pipe.send(('unfollow', id))
            if self.pipe.recv() != 'unfollow': raise ValueError('unfollowing job failed')
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
