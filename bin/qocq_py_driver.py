#! /usr/bin/env python3
"""
Driver utilisable pour exécuter du python en mode distribué, car il peut être
lancé par ssh ou srun sur une machine distante.
"""

def main(module_name, func_name, protocol, protocol_config,
         args, results, error):
  #identify the protocol
  protocol_obj = None
  if protocol == "filepic":
    import qocq.file_pic_db as protocol_obj
  protocol_obj.connect(protocol_config)
  py_args = []
  for arg in args:
    py_args.append(protocol_obj.get(arg))
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  if error is None :
    eval_res = func_obj(*py_args)
  else:
    import traceback
    try:
      eval_res = func_obj(*py_args)
    except BaseException as ex:
      tb = traceback.format_exc()
      protocol_obj.set(error, tb)
      return
  if len(results) == 1 :
    protocol_obj.set(results[0], eval_res)
  else:
    idx = 0
    for result in results :
      protocol_obj.set(result, eval_res[idx])
      idx += 1

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(
    prog='py_driver',
    description='Execute a python function.',
    )
  parser.add_argument('module')
  parser.add_argument('function')
  parser.add_argument('protocol', help="Protocol name to get arguments and set results.")
  parser.add_argument('--protocol_config',
                      help="Protocol connection string.", default=None)
  parser.add_argument('--args', nargs='*', default=[])
  parser.add_argument('--results', nargs='*', default=[])
  parser.add_argument('--error', default=None)
  args = parser.parse_args()
  print("call ", args)
  main(args.module, args.function,
       args.protocol, args.protocol_config,
       args.args, args.results, args.error)
