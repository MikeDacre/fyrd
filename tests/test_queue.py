import os
import cluster
env = cluster.get_cluster_environment()

def write_to_file(string, file):
    """Write a string to a file."""
    with open(file, 'w') as fout:
        fout.write(string + '\n')


def test_queue_inspection():
    """Make sure that if qsub or sbatch are available, the queue is right."""
    if cluster.run.which('sbatch'):
        assert cluster.QUEUE == 'slurm'
    elif cluster.run.which('qsub'):
        assert cluster.QUEUE == 'torque'
    else:
        assert cluster.QUEUE == 'normal'
    assert env == cluster.QUEUE


def test_queue_creation():
    """Test Queue object creation."""
    assert env == 'torque' or env == 'slurm' or env == 'normal'
    cluster.check_queue()
    queue = cluster.Queue()
    assert queue.qtype == env
    len(queue)


def test_job_creation():
    """Make a job and print it."""
    if env == 'normal':
        job = cluster.Job('echo hi', threads=4)
    else:
        job = cluster.Job('echo hi', cores=2, time='00:02:00', mem='2000')
    assert job.qtype == env
    return job


def test_job_execution():
    """Run a job"""
    job = test_job_creation()
    job.submit()
    code, stdout, stderr = job.get()
    assert code == 0
    assert stdout.split('\n')[1:4] == ['Running echo', 'hi', 'Done']
    assert stderr == ''
    return job


def test_job_cleaning():
    """Delete intermediate files."""
    job = test_job_execution()
    job.clean()
    assert 'echo.cluster' not in os.listdir('.')


def test_function_submission():
    """Submit a function."""
    job = cluster.Job(write_to_file, ('42', 'bobfile'))
    job.submit()
    code, stdout, stderr = job.get()
    print(code, stdout, stderr)
    assert stdout.split('\n')[1:3] == ['Running write_to_file', 'Done']
    assert stderr == ''
    with open('bobfile') as fin:
        assert fin.read().rstrip() == '42'
    os.remove('bobfile')
    job.clean()
