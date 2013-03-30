#!/usr/bin/python

import bluelet, time
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

# class Proxy:
#     def __init__(self, method, *args):
#         self.method = method
#         self.args = args
#     def __call__(self, obj):
#         self.method(obj, *self.args)

class Msg:
    def __init__(self, msg):
        self.msg = msg
class Ready(Msg): pass

# class Command: pass
# class Exit(Command): pass
# class ListEngines(Command): pass
# class Start(Command): pass

class Manager:
    
    def __init__(self, pipe, profile):
        from IPython.parallel import Client
        self.pipe = pipe
        self.profile = profile
        self.client = Client(profile = profile)
        self.engines = [self.client[id] for id in self.client.ids]
        self.run = True

    def app(self):
        for i, engine in enumerate(self.engines):
            ans = engine.apply_sync(Manager.start_job, i)
            self.pipe.send(Msg('job %d started, received: %s' % (i, ans)))
        self.pipe.send(Ready('all systems are a go'))
        for engine in self.engines:
            yield bluelet.spawn(self.process_job(engine))
        print('th1: Manager <> %s' % str(Manager.list_engines))
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
            engine.apply_sync(Manager.transmit_output, 'exit')
            print('app::stop_monitor::stopped<%d>' % i)

    def list_engines(self):
        self.pipe.send(str(self.engines))

    def start_sched(self):
        pass
        
    def process_job(self, engine):
        while self.run:
            ar = engine.apply_async(Manager.transmit_output)
            while not ar.ready():
                yield bluelet.null()
                #            self.pipe.send(Msg(ar.result))
        yield bluelet.end()

    @staticmethod
    def start_job(num):
        from subprocess import Popen, PIPE
        global job
        job = Popen(['/home/mcstar/src/sched/lilscript.sh', str(num)], stdout = PIPE)
        return 'OK: %d' % num

    @staticmethod
    def transmit_output(command = None):
        if command is None:
            return job.stdout.readline()
        elif command == 'exit':
            job.kill()
            ret = job.wait()
            return ret
        else:
            raise ValueError('Wrong command <%s>' % command)

class HQ:
    
    def __init__(self, profile):
        print('using profile <%s>' % profile)
        self.pipe, pipe_bck = Pipe()
        def bck():
            Manager(pipe_bck, profile).bluelet()
        self.thread_bck = Process(target = bck)
        self.thread_bck.start()
        
    def ready(self):
        while True:
            msg = self.pipe.recv()
            print(msg.msg)
            if isinstance(msg, Ready):
                break
            
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
    print('th2: Manager <> %s' % str(Manager.list_engines))
    main()
    from IPython.config.loader import Config
    from IPython import embed
    import sys
    cfg = Config()
    cfg.TerminalInteractiveShell.confirm_exit = False
    embed(config = cfg)
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

# def remsys(cmd):
#     import subprocess
#     p=subprocess.Popen(cmd.split(' '), stdout=subprocess.PIPE)
#     return p.stdout.readlines()
# v.apply_sync(remsys,'users')
