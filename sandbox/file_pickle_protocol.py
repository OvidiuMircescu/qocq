"""
Save & load data on the local file system using pickle.

Le protocole le plus naif qui soit pour échanger les données en les écrivant
sur disque.
Permet de définir l'API qu'on peut attendre d'un protocole d'échange de données.
"""
import os
import pickle

_root_path=None
def init(root_path=None):
  global _root_path
  if _root_path is None :
    if root_path is None:
      _root_path = os.getcwd()
    else:
      _root_path = root_path
      from pathlib import Path
      Path(_root_path).mkdir(parents=True, exist_ok=True)
  else:
    raise Exception("Initialization has already been done!")

def save(key, value):
  """ Store key and value into database.
  May be called from many processes, scheduler included.
  The key must be unique! Only one save is allowed for a key!
  :param key: key to identify the value.
  :param value: value to be stored
  :return: url to fetch the value
  """
  global _root_path
  if _root_path is None :
    raise Exception("Protocol not initialized!")
  valid_key = result_key(key)
  file_path = os.path.join(_root_path, valid_key)
  with open(file_path, 'wb') as f:
    pickle.dump(value, f, pickle.HIGHEST_PROTOCOL)
  return valid_key

def name():
  """ :return: name of the protocol. Only lowercase letters, numbers or '_'."""
  return "py_file_pickle"

def config():
  """ Configuration used by init.
  """
  return _root_path

def get(key):
  global _root_path
  if _root_path is None :
    raise Exception("Protocol not initialized!")
  file_path = os.path.join(_root_path, key)
  with open(file_path, 'rb') as f:
    data = pickle.load(f)
  return data

def result_key(key):
  """ If the key is already valid it should'n be changed.
  :returns: a valid key for this protocol."""
  return str(key)
