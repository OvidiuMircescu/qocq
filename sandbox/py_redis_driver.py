#! /usr/bin/env python3
"""
Exemple d'utilisation de redis.
Prérequis:
- télécharger redis à https://redis.io/download/
- installation (la compilation n'a besoin d'aucun prérequis à part gcc)
- installer le module redis-py (pip install redis)
- démarrer un serveur redis (redis-server)
"""

def main(module_name, func_name, args, results):
  import importlib
  module_obj = importlib.import_module(module_name)
  func_obj = getattr(module_obj, func_name)
  import redis
  db = redis.Redis()
  py_args = []
  for arg in args:
    py_args.append(db.get(arg))
  eval_res = func_obj(*py_args)
  if len(results) == 1 :
    db.set(results[0], eval_res)
  else:
    idx = 0
    for result in results :
      db.set(result, eval_res[idx])
      idx += 1

if __name__ == '__main__':
  import argparse
  parser = argparse.ArgumentParser(
    prog='py_redis_driver',
    description='Execute a python function with paramaters from a redis database',
    )
  parser.add_argument('module')
  parser.add_argument('function')
  parser.add_argument('--args', nargs='*', default=[])
  parser.add_argument('--results', nargs='*', default=[])
  args = parser.parse_args()
  main(args.module, args.function, args.args, args.results)
