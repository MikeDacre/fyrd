"""
Functions and attributes to allow torque queue functionality.
"""
import xml.etree.ElementTree as _ET

from .. import run as _run
from .. import logme as _logme
from ..batch import Batch as _Batch


class BatchSystem(_Batch):

    """Methods needed for the torque batch system."""

    name        = 'torque'
    submit_cmnd = 'qsub'
    queue_cmnd  = 'qstat -x'
    arg_prefix  = '#PBS'

    identifying_scripts = ['qsub', 'qstat']

    # Define torque-to-slurm mappings
    status_dict = {
        'C': 'completed',
        'E': 'completing',
        'H': 'held',  # Not a SLURM state
        'Q': 'pending',
        'R': 'running',
        'T': 'suspended',
        'W': 'running',
        'S': 'suspended',
    }

    def queue_parser(user=None, partition=None):
        """Iterator for torque queues.

        Use the `qstat -x` command to get an XML queue for compatibility.

        Args:
            user (str):     optional user name to pass to qstat to filter queue
            partiton (str): optional partition to filter the queue with

        Yields:
            tuple: job_id, array_index, name, userid, partition, state, nodelist,
                numnodes, ntpernode, exit_code

        numcpus is currently always 1 as most torque queues treat every core as a
        node.
        """
        # Sanitize arguments
        if user:
            user = _run.listify(user)
        if partition:
            partition = _run.listify(partition)

        # Get info from queue, try twice if initial string is empty
        try_count = 0
        while True:
            try:
                xmlqueue = _ET.fromstring(self.fetch_queue)
            except _ET.ParseError:
                # ElementTree throws error when string is empty
                if try_count == 1:
                    xmlqueue = None
                    break
                else:
                    sleep(1)
                    try_count += 1

        # Create QueueJob objects for all entries that match user
        if xmlqueue is not None:
            for xmljob in xmlqueue:
                # Skip this item if we are filtering by user
                job_owner = xmljob.find('Job_Owner').text.split('@')[0]
                if user and job_owner not in user:
                    continue

                # Skip this item if we are filtering by user
                job_queue = xmljob.find('queue').text
                if partition and job_queue not in partition:
                    continue

                # Get IDs
                job_id = xmljob.find('Job_Id').text.split('.')[0]
                if '[' in job_id:
                    job_id, array_id = job_id.split('[')
                    array_id = array_id.strip('[]')
                    if array_id:
                        array_id = int(array_id)
                    else:
                        array_id = 0
                else:
                    array_id = None
                try:
                    job_id = int(job_id)
                except ValueError:
                    # Allow string job IDs
                    pass

                # Get other info
                job_name  = xmljob.find('Job_Name').text
                job_state = self.status_dict[xmljob.find('job_state').text]
                _logme.log('Job {} state: {}'.format(job_id, job_state),
                        'debug')

                # Parse nodes
                ndsx = xmljob.find('exec_host')
                nds = ndsx.text.split('+') if ndsx else []
                nodes = []
                for node in nds:
                    if '-' in node:
                        nm, num = node.split('/')
                        for i in range(*[int(i) for i in num.split('-')]):
                            nodes.append(nm + '/' + str(i).zfill(2))
                    else:
                        nodes.append(node)
                # I assume that every 'node' is a core, as that is the
                # default for torque, but it isn't always true
                job_threads  = len(nodes)

                # Get exit code
                exitcode = xmljob.find('exit_status')
                if hasattr(exitcode, 'text'):
                    exitcode = int(exitcode.text)
                else:
                    exitcode = None

                yield (job_id, array_id, job_name, job_owner, job_queue,
                       job_state, nodes, job_threads, 1, exitcode)

    def id_from_stdout(self, stdout):
        """Extract a torque job ID from the STDOUT of self.queue_cmnd."""
        return int(stdout.split('.')[0])

    def submit_args(_=None, dependencies=None):
        """Use kwds and dependencies to create args for qsub.

        Args:
            dependencies (list): A list of job IDs to wait for

        Returns:
            str: A string of submission arguments to be appended to the
                    call to qsub
        """
        if dependencies:
            args = '-W depend={}'.format(
                ','.join(['afterok:' + str(d) for d in dependencies]))
            args = ['qsub', depends, self.submission.file_name]
        else:
            args = ''
        return args

    def format_script(self, kwds):
        """Create a submission script for qsub.

        Args:
            kwds (dict): Allowable keyword arguments for a fyrd Job

        Returns:
            str: A formatted submission script
        """
        outstr = '#!/bin/bash'
        # Torque requires special handling of nodes and cores
        nodes   = int(option_dict.pop('nodes')) if 'nodes' in option_dict else 1
        cores   = int(option_dict.pop('cores')) if 'cores' in option_dict else 1
        outstr += '#PBS -l nodes={}:ppn={}'.format(nodes, cores)
        # Features parsing, must be on same line as node request
        features = kwds.pop('features') if 'features' in kwds else None
        if features
            outstr += ':' + ':'.join(
                run.opt_split(option_dict.pop('features'), (',', ':')))
        if 'qos' in option_dict:
            outstr += ',qos={}'.format(option_dict.pop('qos'))
        return outstr + '\n' + _options.options_to_string(kwds)


###############################################################################
#                             Sample qsub output                              #
###############################################################################

QUEUE_OUTPUT = r"""<Data><Job><Job_Id>665199.superserver.stanford.edu</Job_Id><Job_Name>int_tmux</Job_Name><Job_Owner>johnfox@wow-server</Job_Owner><resources_used><cput>00:15:23</cput><energy_used>0</energy_used><mem>17592kb</mem><vmem>89780kb</vmem><walltime>335:34:49</walltime></resources_used><job_state>R</job_state><queue>interactive</queue><server>superserver.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1479339279</ctime><Error_Path>wow-server:/home/johnfox/.int_tmux.error</Error_Path><exec_host>node01/0</exec_host><Hold_Types>n</Hold_Types><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1479351099</mtime><Output_Path>wow-server:/dev/null</Output_Path><Priority>0</Priority><qtime>1479339279</qtime><Rerunable>True</Rerunable><Resource_List><mem>4gb</mem><nice>-5</nice><nodect>1</nodect><nodes>1:ppn=1</nodes></Resource_List><session_id>797</session_id><Shell_Path_List>/bin/bash</Shell_Path_List><euser>johnfox</euser><egroup>johnfox</egroup><queue_type>E</queue_type><etime>1479339279</etime><exit_status>-3</exit_status><start_time>1479339665</start_time><start_count>2</start_count><fault_tolerant>False</fault_tolerant><job_radix>0</job_radix><submit_host>wow-server.stanford.edu</submit_host><request_version>1</request_version></Job><Job><Job_Id>667999.superserver.stanford.edu</Job_Id><Job_Name>map_It3_Ir4</Job_Name><Job_Owner>robby@node07</Job_Owner><job_state>Q</job_state><queue>batch</queue><server>superserver.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1479779395</ctime><Error_Path>node07:/science/robby/crispr/analysis/./Output/by2rm_tech_pilot_miseq/jobs_files/map_It3_Ir4.error</Error_Path><Hold_Types>n</Hold_Types><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1479779395</mtime><Output_Path>node07:/science/robby/crispr/analysis/./Output/by2rm_tech_pilot_miseq/jobs_files/map_It3_Ir4.out</Output_Path><Priority>0</Priority><qtime>1479779395</qtime><Rerunable>True</Rerunable><Resource_List><mem>32gb</mem><ncpus>1</ncpus><nice>-1</nice><walltime>23:59:00</walltime></Resource_List><Shell_Path_List>/bin/bash</Shell_Path_List><euser>robby</euser><egroup>robby</egroup><queue_type>E</queue_type><etime>1479779395</etime><submit_args>-N map_It3_Ir4 -o ./Output/by2rm_tech_pilot_miseq/jobs_files/map_It3_Ir4.out -e ./Output/by2rm_tech_pilot_miseq/jobs_files/map_It3_Ir4.error -V -S /bin/bash -l mem=32gb,walltime=23:59:00,ncpus=1 /science/robby/crispr/analysis/.snakemake/tmp.S6Z4HE/snakejob.map_reads.0.sh</submit_args><fault_tolerant>False</fault_tolerant><job_radix>0</job_radix><submit_host>node07.superserver.stanford.edu</submit_host><request_version>1</request_version></Job><Job><Job_Id>673174.superserver.stanford.edu</Job_Id><Job_Name>STDIN</Job_Name><Job_Owner>dacre@wow-server</Job_Owner><resources_used><cput>00:00:00</cput><energy_used>0</energy_used><mem>0kb</mem><vmem>0kb</vmem><walltime>00:00:00</walltime></resources_used><job_state>C</job_state><queue>tiny</queue><server>superserver.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1480547421</ctime><Error_Path>wow-server.stanford.edu:/home/dacre/STDIN.e673174</Error_Path><exec_host>node04/0</exec_host><Hold_Types>n</Hold_Types><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1480547422</mtime><Output_Path>wow-server.stanford.edu:/home/dacre/STDIN.o673174</Output_Path><Priority>0</Priority><qtime>1480547421</qtime><Rerunable>True</Rerunable><Resource_List><mem>4gb</mem><nice>-1</nice></Resource_List><session_id>21298</session_id><Variable_List>PBS_O_QUEUE=route,PBS_O_HOME=/home/dacre,PBS_O_LOGNAME=dacre,PBS_O_PATH=/home/dacre/.pyenv/plugins/pyenv-virtualenv/shims:/home/dacre/.pyenv/shims:/home/dacre/.pyenv/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/home/dacre/bin:/home/dacre/usr/bin:/usr/local/MATLAB/R2013a/bin:/usr/lib/jvm/default/bin:/usr/bin/site_perl:/usr/bin/vendor_perl:/usr/bin/core_perl,PBS_O_MAIL=/var/mail/dacre,PBS_O_SHELL=/bin/zsh,PBS_O_LANG=en_US.UTF-8,PBS_O_WORKDIR=/home/dacre,PBS_O_HOST=wow-server.stanford.edu,PBS_O_SERVER=superserver.stanford.edu</Variable_List><euser>dacre</euser><egroup>dacre</egroup><queue_type>E</queue_type><etime>1480547421</etime><exit_status>0</exit_status><start_time>1480547422</start_time><start_count>1</start_count><fault_tolerant>False</fault_tolerant><comp_time>1480547422</comp_time><job_radix>0</job_radix><total_runtime>0.329805</total_runtime><submit_host>wow-server.stanford.edu</submit_host><request_version>1</request_version></Job><Job><Job_Id>673175[].superserver.stanford.edu</Job_Id><Job_Name>t.sh</Job_Name><Job_Owner>dacre@wow-server</Job_Owner><job_state>C</job_state><queue>jolly</queue><server>superserver.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1480547753</ctime><Error_Path>wow-server.stanford.edu:/home/dacre/t.sh.e673175</Error_Path><Hold_Types>n</Hold_Types><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1480547796</mtime><Output_Path>wow-server.stanford.edu:/home/dacre/t.sh.o673175</Output_Path><Priority>0</Priority><qtime>1480547753</qtime><Rerunable>True</Rerunable><Resource_List><mem>4gb</mem><nice>-1</nice></Resource_List><Variable_List>PBS_O_QUEUE=route,PBS_O_HOME=/home/dacre,PBS_O_LOGNAME=dacre,PBS_O_PATH=/home/dacre/.pyenv/plugins/pyenv-virtualenv/shims:/home/dacre/.pyenv/shims:/home/dacre/.pyenv/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/home/dacre/bin:/home/dacre/usr/bin:/usr/local/MATLAB/R2013a/bin:/usr/lib/jvm/default/bin:/usr/bin/site_perl:/usr/bin/vendor_perl:/usr/bin/core_perl,PBS_O_MAIL=/var/mail/dacre,PBS_O_SHELL=/bin/zsh,PBS_O_LANG=en_US.UTF-8,PBS_O_WORKDIR=/home/dacre,PBS_O_HOST=wow-server.stanford.edu,PBS_O_SERVER=superserver.stanford.edu</Variable_List><euser>dacre</euser><egroup>dacre</egroup><queue_type>E</queue_type><etime>1480547753</etime><submit_args>-t 1-20 t.sh</submit_args><job_array_request>1-20</job_array_request><fault_tolerant>False</fault_tolerant><job_radix>0</job_radix><submit_host>wow-server.stanford.edu</submit_host><request_version>1</request_version></Job><Job><Job_Id>673175[19].fruster.stanford.edu</Job_Id><Job_Name>t.sh-19</Job_Name><Job_Owner>dacre@fraser-server</Job_Owner><resources_used><cput>00:00:00</cput><energy_used>0</energy_used><mem>2888kb</mem><vmem>13988kb</vmem><walltime>00:00:01</walltime></resources_used><job_state>C</job_state><queue>batch</queue><server>fruster.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1480547753</ctime><Error_Path>fraser-server.stanford.edu:/home/dacre/t.sh.e673175-19</Error_Path><exec_host>node11/1</exec_host><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1480547755</mtime><Output_Path>fraser-server.stanford.edu:/home/dacre/t.sh.o673175-19</Output_Path><Priority>0</Priority><qtime>1480547753</qtime><Rerunable>True</Rerunable><Resource_List><mem>4gb</mem><nice>-1</nice></Resource_List><session_id>2229</session_id><Variable_List>PBS_ARRAYID=19,PBS_O_QUEUE=route,PBS_O_HOME=/home/dacre,PBS_O_LOGNAME=dacre,PBS_O_PATH=/home/dacre/.pyenv/plugins/pyenv-virtualenv/shims:/home/dacre/.pyenv/shims:/home/dacre/.pyenv/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/home/dacre/bin:/home/dacre/usr/bin:/usr/local/MATLAB/R2013a/bin:/usr/lib/jvm/default/bin:/usr/bin/site_perl:/usr/bin/vendor_perl:/usr/bin/core_perl,PBS_O_MAIL=/var/mail/dacre,PBS_O_SHELL=/bin/zsh,PBS_O_LANG=en_US.UTF-8,PBS_O_WORKDIR=/home/dacre,PBS_O_HOST=fraser-server.stanford.edu,PBS_O_SERVER=fruster.stanford.edu</Variable_List><euser>dacre</euser><egroup>dacre</egroup><queue_type>E</queue_type><etime>1480547753</etime><exit_status>0</exit_status><submit_args>-t 1-20 t.sh</submit_args><job_array_id>19</job_array_id><start_time>1480547754</start_time><start_count>1</start_count><fault_tolerant>False</fault_tolerant><comp_time>1480547755</comp_time><job_radix>0</job_radix><total_runtime>0.642600</total_runtime><submit_host>fraser-server.stanford.edu</submit_host><request_version>1</request_version></Job><Job><Job_Id>673175[20].fruster.stanford.edu</Job_Id><Job_Name>t.sh-20</Job_Name><Job_Owner>dacre@fraser-server</Job_Owner><resources_used><cput>00:00:00</cput><energy_used>0</energy_used><mem>3112kb</mem><vmem>13988kb</vmem><walltime>00:00:01</walltime></resources_used><job_state>C</job_state><queue>batch</queue><server>fruster.stanford.edu</server><Checkpoint>u</Checkpoint><ctime>1480547753</ctime><Error_Path>fraser-server.stanford.edu:/home/dacre/t.sh.e673175-20</Error_Path><exec_host>node13/1</exec_host><Join_Path>n</Join_Path><Keep_Files>n</Keep_Files><Mail_Points>a</Mail_Points><mtime>1480547755</mtime><Output_Path>fraser-server.stanford.edu:/home/dacre/t.sh.o673175-20</Output_Path><Priority>0</Priority><qtime>1480547753</qtime><Rerunable>True</Rerunable><Resource_List><mem>4gb</mem><nice>-1</nice></Resource_List><session_id>18715</session_id><Variable_List>PBS_ARRAYID=20,PBS_O_QUEUE=route,PBS_O_HOME=/home/dacre,PBS_O_LOGNAME=dacre,PBS_O_PATH=/home/dacre/.pyenv/plugins/pyenv-virtualenv/shims:/home/dacre/.pyenv/shims:/home/dacre/.pyenv/bin:/usr/local/sbin:/usr/local/bin:/usr/bin:/home/dacre/bin:/home/dacre/usr/bin:/usr/local/MATLAB/R2013a/bin:/usr/lib/jvm/default/bin:/usr/bin/site_perl:/usr/bin/vendor_perl:/usr/bin/core_perl,PBS_O_MAIL=/var/mail/dacre,PBS_O_SHELL=/bin/zsh,PBS_O_LANG=en_US.UTF-8,PBS_O_WORKDIR=/home/dacre,PBS_O_HOST=fraser-server.stanford.edu,PBS_O_SERVER=fruster.stanford.edu</Variable_List><euser>dacre</euser><egroup>dacre</egroup><queue_type>E</queue_type><etime>1480547753</etime><exit_status>0</exit_status><submit_args>-t 1-20 t.sh</submit_args><job_array_id>20</job_array_id><start_time>1480547754</start_time><start_count>1</start_count><fault_tolerant>False</fault_tolerant><comp_time>1480547755</comp_time><job_radix>0</job_radix><total_runtime>0.578209</total_runtime><submit_host>fraser-server.stanford.edu</submit_host><request_version>1</request_version></Job></Data>"""
