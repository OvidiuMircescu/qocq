#! /usr/bin/env python3
"""
Driver utilisable pour exécuter du python en mode distribué, car il peut être
lancé par ssh ou srun sur une machine distante.
"""

def main(module_name, func_name, protocol, protocol_config, args, results):
  #identify the protocol
  import datamanager
  datamanager.activate_protocol(protocol, protocol_config)
  py_args = []
  for arg in args:
    py_args.append(datamanager.get(arg))
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  eval_res = func_obj(*py_args)
  if len(results) == 1 :
    datamanager.save(results[0], eval_res)
  else:
    idx = 0
    for result in results :
      datamanager.save(result, eval_res[idx])
      idx += 1

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(
    prog='py_driver',
    description='Execute a python function.',
    )
  parser.add_argument('module')
  parser.add_argument('function')
  parser.add_argument('protocol', help="Protocol for arguments.")
  parser.add_argument('--protocol_config',
                      help="Protocol configuration string.", default=None)
  parser.add_argument('--args', nargs='*', default=[])
  parser.add_argument('--results', nargs='*', default=[])
  args = parser.parse_args()
  print("call ", args)
  main(args.module, args.function,
       args.protocol, args.protocol_config,
       args.args, args.results)
