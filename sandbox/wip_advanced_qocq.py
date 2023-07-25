"""
WARNING: Travail en cours!
Bac à sable pour expérimenter.
Etat courant :
scheduler : python asyncio
work load manager : no
executor driver : py_driver.py
data manager : datamanager.py
TODO error management
"""
import asyncio
import functools

import os
import datamanager
import workloadmanager
import container

wlm = None # workloadmanager object
container_manager = None

def init(transfer_protocol="py_file_pickle",
         containers_config=None):
  """
  Initialize every protocol.
  """
  datamanager.activate_protocol(transfer_protocol,
                                os.path.join(os.getcwd(), "db"))
  global wlm
  wlm = workloadmanager.WorkloadManager()
  global container_manager
  container_manager = container.ContainerManager()
  if containers_config is not None:
    container_manager.loadFile(containers_config)

class ObjRef:
    def __init__(self, ref):
      self.ref = ref

async def executor_command(fn, args, kwargs):
    mod_name = fn.__module__ # TODO : fix __main__ module name
    if mod_name == "__main__" :
      import inspect
      file_name = os.path.basename(inspect.getfile(fn))
      mod_name = file_name[0:-3] # remove .py
    func_name = fn.__name__
    protocol = datamanager.protocol_name()
    protocol_config = datamanager.protocol_config()
    # TODO multiple results
    result = datamanager.new_key(func_name+"_ret")
    command_args = ""
    ref_args = []
    for arg in args:
      if isinstance(arg, ObjRef):
        ref_args.append(arg.ref)
      else:
        x = await datamanager.save_future(arg)
        ref_args.append(x)
    ref_kwargs = {}
    for key, value in kwargs.items():
      if isinstance(value, ObjRef):
        ref_kwargs[key] = value
      else:
        x = await datamanager.save_future(value)
        ref_kwargs[key] = x
    import py_driver
    command = py_driver.create_command(mod_name, func_name,
                                       protocol, protocol_config,
                                       ref_args, ref_kwargs, [result])
    return command, ObjRef(result)

async def wait_args(*args, **kwargs):
  # TODO deal with errors
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

async def remote_async(fn, container_type, *args, **kwargs):
  """
    Evaluate fn remotely in async mode.
    :param fn: standard synchronous function.
    :param container_type: container.ContainerProperties object.
    :param args: fn args
    :param kwargs: fn kwargs
    :return: fn evaluated as an async function.
  """
  sync_args, sync_kwargs = await wait_args(*args, **kwargs)

  command, result = await executor_command(fn, sync_args, sync_kwargs)
  await wlm.submit(command, container_type)
  return result

background_tasks = []

async def async_call(fn, *args, **kwargs):
  return fn(*args, **kwargs)

async def local_async(fn, *args, **kwargs):
  """
    Evaluate fn localy in async mode.
  """
  sync_args, sync_kwargs = await wait_args(*args, **kwargs)
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

def wrapped_atomic_task(f, container_type):
  """
  :param f: standard synchronous function.
  :param container_type: name of the container type as a string.
  :return: async remote evaluation of f.
  """
  @functools.wraps(f)
  def my_func(*args, **kwargs):
    try:
      asyncio.get_running_loop()
    except:
      return f(*args, **kwargs) # not an async mode
    global container_manager
    container_obj = container_manager.getContainer(container_type)
    task = asyncio.create_task(remote_async(f, container_obj, *args, **kwargs))
    background_tasks.append(task)
    return task
  return my_func

def atomic_task(arg):
  """
  Decorator for async remote evaluation of functions.
  :param arg: function to be called or type of container.
  """
  if callable(arg):
    # decorator used without parameters. arg is the function
    return wrapped_atomic_task(arg, container.ContainerManager.defaultContainerName)
  else:
    # decorator used with parameter. arg is the container name
    return functools.partial(wrapped_atomic_task, container_type=arg)

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
