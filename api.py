import asyncio
_async_activated = True
import os

#TODO : find better init
_init_done = False
def _init():
  global _init_done
  if _async_activated and not _init_done :
    _init_done = True
    import file_pickle_protocol
    import datamanager
    datamanager.activate_protocol(file_pickle_protocol.name(),
                                  os.path.join(os.getcwd(), "db"))

async def run(cmd):
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

_block_tasks = []
def atomic_task(f):
  if not _async_activated:
    return f

  # TODO : return of tuple - create multiple futures
  async def my_func(*args):
    _init()
    mod_name = f.__module__
    func_name = f.__name__
    protocol = file_pickle_protocol.name()
    protocol_config = os.path.join(os.getcwd(), "db")
    future_args = []
    for arg in args :
      if asyncio.isfuture(arg):
        future_args.append(arg)
      else:
        future_args.append(datamanager.arg_future(arg, func_name))
    remote_args = []
    for arg in future_args:
      if not arg.done():
        await arg
        remote_args.append(arg.result())
    #TODO : deal with return of tuple
    result = datamanager.result_future(func_name+"_ret")
    command = "python3 py_driver.py {} {} {} --protocol_config {} ".format(
                mod_name, func_name, protocol, protocol_config)
    command_args = ""
    for arg in remote_args :
      command_args += "--args " + arg
    command += command_args + " --results " + result
    await run(command)
    return result
  return my_func

def composed_task(f):
  if not _async_activated:
    return f
  async def my_func(*args, **kwargs):
    #global _block_tasks
    #_block_tasks.append([])
    r = f(*args, **kwargs)
    #tasks = _block_tasks.pop()
    
  
    
