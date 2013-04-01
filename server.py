#!/usr/bin/python

import bluelet, sys, time, itertools, functools, tempfile, os
from multiprocessing import Process, Pipe, Manager

from Engine import Engine
from Pager import Pager
from Job import Jobs
from Fmt import Fmt
from Bck import BCK
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




class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass

from Bck import BCK
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
            job.output_queue.append(stdout_line.strip().decode())
            self.jobs_executing[job_id] = job
            if self.jobs_executing[job_id].follow and PIPE:
                PIPE.write(stdout_line)
                PIPE.flush()
            if poll_code is not None:
                job = self.jobs_executing[job_id]
                job.unset_executing()
                self.jobs_executing[job_id] = job
                break
        yield bluelet.end()

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
            # self.jobs_executing.update()
            # self.jobs_finished.update()
            # if id in self.jobs_executing:
            #     print('job executing: %s' % ('\n'.join(self.jobs_executing[id].output_queue)))
            # if id in self.jobs_finished:
            #     print('job finished: %s' % ('\n'.join(self.jobs_finished[id].output_queue)))
            if id in self.jobs_executing or id in self.jobs_finished:
                self.pipe.send(('follow', id, fifo))
            else: print('job id not found')
            r = open(fifo, 'rb')
            if self.pipe.recv() != 'follow': raise ValueError('following job failed')
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
