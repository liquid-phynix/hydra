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

        #        print('job finished: %s' % ('\n'.join(self.jobs_executing[lucky.id].output_queue)))
        self.jobs_finished[lucky.id] = self.jobs_executing[lucky.id]
        #        print('job finished: %s' % ('\n'.join(self.jobs_finished[lucky.id].output_queue)))
        del self.jobs_executing[lucky.id]
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
        global PIPE
        if id in self.jobs_executing:
            job = self.jobs_executing[id]
            PIPE = open(fifo, 'wb')
            job.follow = True
            self.jobs_executing[id] = job
            self.pipe.send('follow')
        if id in self.jobs_finished:
            #            job = self.jobs_finished[id]
            #            print('job finished %s' % ('\-'.join(self.jobs_finished[id].output_queue)))
            PIPE = open(fifo, 'wb')
            self.pipe.send('follow')
            for line in self.jobs_finished[id].output_queue:
                print(line)
                PIPE.write(line.encode() + b'\n')
                PIPE.flush()
            PIPE.close()
                #        if job is None: raise ValueError('follow: \'job\' cannot be None')
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
