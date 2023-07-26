"""
WARNING: Travail en cours!
Bac à sable pour expérimenter.
Etat courant :
scheduler : python asyncio
work load manager : workloadmanager.py
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
import funcprop

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

def close():
  datamanager.close()

class ObjRef:
    def __init__(self, ref):
      self.ref = ref

async def executor_command(fn, nb_results, args, kwargs):
    mod_name = fn.__module__ # TODO : fix __main__ module name
    if mod_name == "__main__" :
      import inspect
      file_name = os.path.basename(inspect.getfile(fn))
      mod_name = file_name[0:-3] # remove .py
    func_name = fn.__name__
    protocol = datamanager.protocol_name()
    protocol_config = datamanager.protocol_config()
    results = []
    for i in range(nb_results):
      key = func_name+"_r"+str(i)
      results.append(datamanager.new_key(key))
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
                                       ref_args, ref_kwargs, results)
    result = [ ObjRef(i) for i in results ]
    return command, result

async def wait_args(*args, **kwargs):
  # TODO deal with errors
  new_args = []
  for arg in args:
    if asyncio.isfuture(arg):
      v = await arg
      while asyncio.isfuture(v):
        v = await v
      new_args.append(v)
    else:
      new_args.append(arg)
  new_kwargs = {}
  for k, v in kwargs.items():
    if asyncio.isfuture(v):
      new_kwargs[k] = await v
  return new_args, new_kwargs

async def remote_async(fn, container_type, nb_results, *args, **kwargs):
  """
    Evaluate fn remotely in async mode.
    :param fn: standard synchronous function.
    :param container_type: container.ContainerProperties object.
    :param nb_results: number of values returned by fn.
    :param args: fn args
    :param kwargs: fn kwargs
    :return: fn evaluated as an async function.
  """
  sync_args, sync_kwargs = await wait_args(*args, **kwargs)

  command, result = await executor_command(fn,
                                           nb_results, sync_args, sync_kwargs)
  await wlm.run(command, container_type)
  return result

background_tasks = []

async def async_call(fn, *args, **kwargs):
  return fn(*args, **kwargs)

async def local_async(fn, *args, **kwargs):
  """
    Evaluate fn localy in async mode.
  """
  sync_args, sync_kwargs = await wait_args(*args, **kwargs)
  result = await async_call(fn, *sync_args, **sync_kwargs)
  return result

async def index(futures_obj, idx):
  """
  Wait for the promise futures_obj and return futures_obj[idx].
  :param futures_obj: promise representing a tuple of ObjRef.
  :param idx: index
  """
  # TODO error management
  obj = await futures_obj
  while asyncio.isfuture(obj):
    obj = await obj
  return obj[idx]

def wrapped_atomic_task(f, container_type):
  """
  :param f: standard synchronous function.
  :param container_type: name of the container type as a string.
  :return: async remote evaluation of f.
  """
  @functools.wraps(f)
  def my_func(*args, **kwargs):
    asyncmode = False
    try:
      asyncio.get_running_loop()
      asyncmode = True
    except:
      pass
    if asyncmode == False :
      return f(*args, **kwargs)
    global container_manager
    container_obj = container_manager.getContainer(container_type)
    nb_results = funcprop.number_of_results(f)
    task = asyncio.create_task(remote_async(f, container_obj,
                                            nb_results, *args, **kwargs))
    background_tasks.append(task)
    future_result = []
    for i in range(nb_results):
      t = asyncio.create_task(index(task, i))
      future_result.append(t)
      background_tasks.append(t)
    if nb_results == 0:
      return
    elif nb_results == 1:
      return future_result[0]
    else:
      return tuple(future_result)
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
    asyncmode = False
    try:
      asyncio.get_running_loop()
      asyncmode = True
    except:
      pass
    if asyncmode == False :
      return f(*args, **kwargs) # not an async mode
    task = asyncio.create_task( local_async(f, *args, **kwargs))
    background_tasks.append(task)
    nb_results = funcprop.number_of_results(f)
    if nb_results == 0:
      return
    elif nb_results == 1:
      return task
    else:
      future_result = []
      for i in range(nb_results):
        t = asyncio.create_task(index(task, i))
        future_result.append(t)
        background_tasks.append(t)
      return tuple(future_result)
  return my_func

async def run_async(fn):
  t1 = fn()
  global background_tasks
  current_tasks = background_tasks
  background_tasks = []
  while len(current_tasks) > 0:
    await asyncio.gather(*current_tasks)
    current_tasks = background_tasks
    background_tasks = []

def run(fn):
  asyncio.run(run_async(fn))
