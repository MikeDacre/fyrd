import cluster
env = cluster.get_cluster_environment()

def test_queue_creation():
    """Test Queue object creation."""
    assert env == 'torque' or env == 'slurm' or env == 'normal'
    cluster.check_queue()
    queue = cluster.Queue()
    len(queue)

def test_job_creation():
    """Make a job and print it."""
    if env == 'normal':
        job = cluster.Job('echo hi', threads=4)
    else:
        job = cluster.Job('echo hi', cores=2, time='00:02:00', mem='2000')
    return job

def test_job_execution():
    """Run a job"""
    job = test_job_creation()
    job.submit()
    code, stdout, stderr = job.get()
    assert code == 0
    assert stdout == 'hi'
    assert stderr == ''

