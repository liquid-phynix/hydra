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
