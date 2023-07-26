import os
import asyncio

async def run_command(cmd):
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    #print('[{} exited with {}]'.format(cmd,
                                       #proc.returncode))
    if stdout:
        print('[stdout]\n{}'.format(stdout.decode()))
    if stderr:
        print('[stderr]\n{}'.format(stderr.decode()))

class WorkloadManager:
  def __init__(self, maxcpus=0):
    if maxcpus <= 0 :
      maxcpus = os.cpu_count()
    self._max_cpus = maxcpus
    self._used_cpus = 0
    self._condition = asyncio.Condition()
  
  async def run(self, command, container=None):
    """
    :param command: shell command to run.
    :param container: run configuration (nbcores, etc.)
    :return:
    """
    if container is None:
      nb_cores = 1
    else:
      nb_cores = container.nb_cores
    async with self._condition :
      await self._condition.wait_for(lambda : self._used_cpus + nb_cores <= self._max_cpus)
      self._used_cpus += nb_cores
    await run_command(command)
    async with self._condition :
      self._used_cpus -= nb_cores
      self._condition.notify_all()

class SrunWorkloadManager:
  def __init__(self):
    pass
  
  async def run(self, command, container=None):
    """
    Execute the command asynchronously.
    :param command: shell command to run.
    :param container: run configuration (nbcores, etc.)
    :return:
    """
    if container is None:
      nb_cores = 1
    else:
      nb_cores = container.nb_cores
    srun_command = "srun -N 1 -n " + str(nb_cores) + " " + command
    await run_command(srun_command)
