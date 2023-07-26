#! /usr/bin/env python3
"""
Driver utilisable pour exécuter du python en mode distribué, car il peut être
lancé par ssh ou srun sur une machine distante.
"""

def main(module_name, func_name, protocol, protocol_config, args, kwargs, results):
  #identify the protocol
  import datamanager
  datamanager.activate_protocol(protocol, protocol_config)
  py_args = []
  for arg in args:
    py_args.append(datamanager.get(arg))
  py_kwargs = {}
  for k,v in kwargs:
    py_kwargs[k] = datamanager.get(v)
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  eval_res = func_obj(*py_args, **py_kwargs)
  if len(results) == 1 :
    datamanager.save(results[0], eval_res)
  else:
    idx = 0
    for result in results :
      datamanager.save(result, eval_res[idx])
      idx += 1

def create_command(module_name, func_name, protocol, protocol_config,
                   args, kwargs, results):
  command = "python3 py_driver.py {} {} {} --protocol_config {} ".format(
              module_name, func_name, protocol, protocol_config)
  if len(args) > 0 :
    command += " --args"
  for arg in args :
    command += " '" + arg + "'"
  if len(kwargs) > 0 :
    command += " --kwargs"
  for k,v in kwargs.items():
    command += " '{}:{}'".format(k,v)
  if len(results) > 0 :
    command += " --results"
  for result in results :
    command += " '" + result + "'"
  return command

def command_from_obj(cmd_obj):
  command = "python3 py_driver.py {} {} {} --protocol_config {} ".format(
              cmd_obj.module_name(),
              cmd_obj.function_name(),
              cmd_obj.protocol_name(),
              cmd_obj.protocol_config())
  if len(cmd_obj.args()) > 0 :
    command += " --args"
  for arg in cmd_obj.args() :
    command += " '" + arg + "'"
  if len(cmd_obj.kwargs()) > 0:
    command += " --kwargs"
  for k,v in cmd_obj.kwargs().items():
    command += " '{}:{}'".format(k,v)
  if len(cmd_obj.results()) > 0:
    command += " --results"
  for result in cmd_obj.results() :
    command += " '" + result + "'"
  return command

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(
    prog='py_driver',
    description='Execute a python function.',
    )
  parser.add_argument('module',
                      help="Python module which contains the function.")
  parser.add_argument('function', help="Function to run.")
  parser.add_argument('protocol', help="Protocol for arguments and results.")
  parser.add_argument('--protocol_config',
                      help="Protocol configuration string.", default=None)
  parser.add_argument('--args', nargs='*',
                      default=[],
                      help="Positional arguments for the function.")
  parser.add_argument('--kwargs', nargs='*',
                      default=[],
                      help="Named arguments for the function. Format 'key:v'")
  parser.add_argument('--results', nargs='*',
                      default=[],
                      help="Where to store the results.")
  args = parser.parse_args()
  print("call ", args)
  kwargs = {}
  for kv in args.kwargs:
    k,v = kv.split(':', 1)
    kwargs[k] = v
  main(args.module, args.function,
       args.protocol, args.protocol_config,
       args.args, kwargs, args.results)
