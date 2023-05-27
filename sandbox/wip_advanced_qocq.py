"""
WARNING: Travail en cours!
Bac à sable pour expérimenter.
Etat courant :
scheduler : python asyncio
work load manager : no
executor driver : py_driver.py
data manager : datamanager.py
"""
import asyncio
import functools

import os
import datamanager
import file_pickle_protocol

#TODO : à compléter
def init(*args, **kwargs):
  """
  Initialize every protocol.
  """
  datamanager.activate_protocol(file_pickle_protocol.name(),
                                os.path.join(os.getcwd(), "db"))

async def run_command(cmd):
    print("command to run:", cmd)
    proc = await asyncio.create_subprocess_shell(
        cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE)

    stdout, stderr = await proc.communicate()

    print('[{} exited with {}]'.format(cmd,
                                       proc.returncode))
    if stdout:
        print('[stdout]\n{}'.format(stdout.decode()))
    if stderr:
        print('[stderr]\n{}'.format(stderr.decode()))

def command_for_pydriver(fn, args):
    mod_name = fn.__module__ # TODO : fix __main__ module name
    if mod_name == "__main__" :
      import inspect
      file_name = os.path.basename(inspect.getfile(fn))
      mod_name = file_name[0:-3] # remove .py
    func_name = fn.__name__
    protocol = file_pickle_protocol.name() # TODO get current protocol
    protocol_config = os.path.join(os.getcwd(), "db") # TODO
    result = datamanager.result_future(func_name+"_ret")
    command = "python3 py_driver.py {} {} {} --protocol_config {} ".format(
                mod_name, func_name, protocol, protocol_config)
    command_args = ""
    for arg in args :
      command_args += "--args " + arg
    command += command_args + " --results " + result
    return command, result

async def remote_async(fn, *args):
  """
    Evaluate fn remotely in async mode.
    :param fn: standard synchronous function.
    :param args: fn args
    :return: fn evaluated as an async function.
  """
  future_args = []
  for x in args:
    if asyncio.isfuture(x):
      future_args.append(x)
    else:
      # new pyobj that needs to be added to datamanager
      o = asyncio.create_task(datamanager.arg_future(x))
      future_args.append(o)

  sync_args = await asyncio.gather(*future_args)
  print("sync_args:", sync_args)
  # TODO add work load manager
  command, result = command_for_pydriver(fn, sync_args)
  await run_command(command)
  return result

background_tasks = []

async def local_async(fn, *args, **kwargs):
  return fn(*args, **kwargs)

asynd def unfold(future_obj, n):
  """
  Split a future of a tuple into a tuple of futures.
  :param future_obj: A future waiting for a tuple of size n.
  :param n: expected size of the tuple.
  """
  # TODO
  pass

def atomic_task(f):
  @functools.wraps(f)
  def my_func(*args):
    try:
      asyncio.get_running_loop()
    except:
      return f(*args) # not an async mode
    task = asyncio.create_task(remote_async(f, *args))
    background_tasks.append(task)
    return task
  return my_func

def composed_task(f):
  @functools.wraps(f)
  def my_func(*args, **kwargs):
    try:
      asyncio.get_running_loop()
    except:
      return f(*args, **kwargs) # not an async mode
    ret = local_async(f, *args, **kwargs)
    return ret
  return my_func

async def run_async(fn):
  t1 = asyncio.create_task(fn())
  await t1
  global background_tasks
  current_tasks = background_tasks
  background_tasks = []
  while len(current_tasks) > 0:
    await asyncio.gather(*current_tasks)
    current_tasks = background_tasks
    background_tasks = []

def run(fn):
  asyncio.run(run_async(fn))
