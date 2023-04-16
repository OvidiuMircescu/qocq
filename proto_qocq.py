import asyncio
import concurrent.futures
import time
import functools

def executor(module_name, func_name, *args):
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  return func_obj(*args)

async def remote_async(fn, *args):
  """
    Evaluate fn remotely in async mode.
    :params fn: standard synchronous function.
    :params args: fn args
    :returns: fn evaluated as an async function.
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

# tests

@atomic_task
def f(x):
  import os
  print("pid:", os.getpid(), " sleep ", x)
  time.sleep(x)
  #with open("rapport-"+str(x)+".txt", "w") as f:
    #f.write("coucou!"+str(x))
  return x * x

async def main():
  t1 = f(2)
  t2 = f(t1)
  t3 = f(3)
  t4 = f(t1)
  #await asyncio.gather(t1, t2, t3, t4)

@composed_task
def sync_main():
  t1 = f(2)
  t2 = f(t1)
  t3 = f(3)
  t4 = f(t1)

async def main_bis():
  t1 = asyncio.create_task(remote_async(f, 2))
  t2 = asyncio.create_task(remote_async(f, t1))
  t3 = asyncio.create_task(remote_async(f, 3))
  t4 = asyncio.create_task(remote_async(f, t1))
  await asyncio.gather(t1, t2, t3, t4)
  

if __name__ == '__main__':
  import os
  print("main pid:", os.getpid())
  start_time = time.time()
  #asyncio.run(main(), debug = True)
  #asyncio.run(main_bis())
  asyncio.run(run_async(sync_main))
  end_time = time.time()
  elapsed_time = end_time - start_time
  print("Elapsed time: ", elapsed_time)
  if elapsed_time >= 6.0 and elapsed_time <= 6.1 :
    print("ok!")
  else:
    print("ça craint!")
