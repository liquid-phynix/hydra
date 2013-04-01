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
