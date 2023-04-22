"""
Prototype with
scheduler : python asyncio
work load manager : no
      A possibility is to use a global concurrent.futures.ProcessPoolExecutor
      for local execution.
      see https://docs.python.org/3/library/concurrent.futures.html#concurrent.futures.ProcessPoolExecutor
executor driver : function 'executor'
data manager : managed by concurrent.futures.ProcessPoolExecutor

L'implémentation la plus simple de YACS pour une exécution locale sur une seule
machine, mais avec parallélisme et exécution des fonctions dans des processus
séparés.

WARNING: functions with named parameters cannot be used as atomic tasks.
"""
import asyncio
import concurrent.futures
import functools

def executor(module_name, func_name, *args):
  """
  We have to use this proxy function because decorated functions are not
  serializable with pickle and thus cannot be used directly by
  ProcessPoolExecutor.
  """
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  return func_obj(*args)

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
  await asyncio.gather(*future_args)
  sync_args = []
  for x in args:
    if asyncio.isfuture(x):
      sync_args.append(x.result())
    else:
      sync_args.append(x)
  loop = asyncio.get_running_loop()
  with concurrent.futures.ProcessPoolExecutor() as pool:
    #result = await loop.run_in_executor(pool, f, *sync_args)
    result = await loop.run_in_executor(pool, executor,
                                        fn.__module__, fn.__name__, *sync_args)
  return result

async def local_async(fn, *args, **kwargs):
  return fn(*args, **kwargs)

background_tasks = []

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
      print("sync exec ", *args)
      return f(*args, **kwargs) # not an async mode
    print("async composed_task ", f.__name__)
    ret = local_async(f, *args, **kwargs)
    return ret
  return my_func

async def run_async(fn):
  t1 = asyncio.create_task(fn())
  #print("size 1:", len(background_tasks))
  await t1
  #print("size 2:", len(background_tasks))
  global background_tasks
  current_tasks = background_tasks
  background_tasks = []
  while len(current_tasks) > 0:
    await asyncio.gather(*current_tasks)
    current_tasks = background_tasks
    background_tasks = []

def run(fn):
  asyncio.run(run_async(fn))
