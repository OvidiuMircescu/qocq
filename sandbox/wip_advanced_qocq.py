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

class ObjRef:
    def __init__(self, ref):
      self.ref = ref

async def command_for_pydriver(fn, args):
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
    ref_args = []
    for arg in args:
      if isinstance(arg, ObjRef):
        ref_args.append(arg.ref)
      else:
        x = await datamanager.arg_future(arg)
        ref_args.append(x)
    for arg in ref_args :
      command_args += "--args " + arg
    command += command_args + " --results " + result
    return command, ObjRef(result)

async def wait_args(*args, **kwargs):
  new_args = []
  for arg in args:
    if asyncio.isfuture(arg):
      new_args.append( await arg)
    else:
      new_args.append(arg)
  new_kwargs = {}
  for k, v in kwargs.items():
    if asyncio.isfuture(v):
      new_kwargs[k] = await v
  return new_args, new_kwargs

# TODO add kwargs
async def remote_async(fn, *args):
  """
    Evaluate fn remotely in async mode.
    :param fn: standard synchronous function.
    :param args: fn args
    :return: fn evaluated as an async function.
  """
  sync_args, sync_kwargs = await wait_args(*args)
  # TODO add work load manager
  command, result = await command_for_pydriver(fn, sync_args)
  await run_command(command)
  return result

background_tasks = []

async def async_call(fn, *args, **kwargs):
  return fn(*args, **kwargs)

async def local_async(fn, *args, **kwargs):
  """
    Evaluate fn localy in async mode.
  """
  ## TODO deal with kwargs
  sync_args, sync_kwargs = await wait_args(*args)
  result = await async_call(fn,*sync_args)
  return result

async def unfold(future_obj, n):
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
    task = asyncio.create_task( local_async(f, *args, **kwargs))
    background_tasks.append(task)
    return task
  return my_func

async def run_async(fn):
  #t1 = asyncio.create_task(fn())
  t1 = fn()
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
