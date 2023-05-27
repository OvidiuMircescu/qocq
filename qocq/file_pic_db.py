"""
Save & load data on the local file system using pickle.

Le protocole le plus naif qui soit pour échanger les données en les écrivant
sur disque.
Permet de définir l'API qu'on peut attendre d'un protocole d'échange de données.
"""
import os
import pickle

_root_path=None

def configure(root_path=None):
  """ Start the services. Create an empty database.
  :param root_path: path to database
  """
  global _root_path
  if _root_path is None :
    if root_path is None:
      _root_path = os.getcwd()
    else:
      _root_path = root_path
      from pathlib import Path
      Path(_root_path).mkdir(parents=True, exist_ok=True)
  else:
    raise Exception("Configuration has already been done!")

def connect(root_path=None):
  """ Connect to server. Needed before using set and get.
  """
  global _root_path
  _root_path = root_path

def set(key, value):
  """ Store key and value into database.
  May be called from many processes, scheduler included.
  The key must be unique! Only one save is allowed for a key!
  :param key: key to identify the value.
  :param value: value to be stored
  :return: url to fetch the value
  """
  file_path = _internal_key(key)
  with open(file_path, 'wb') as f:
    pickle.dump(value, f, pickle.HIGHEST_PROTOCOL)
  return file_path

def get(key):
  """ Get the value stored for a key.
  :param key: key to identify the value.
  """
  file_path = _internal_key(key)
  with open(file_path, 'rb') as f:
    data = pickle.load(f)
  return data

def remove(key):
  file_path = _internal_key(key)
  if os.path.isfile(file_path):
    os.remove(file_path)

def _internal_key(key):
  """ If the key is already valid it should'n be changed.
  :returns: a valid key for this protocol.
  """
  # TODO more checks may be added.
  global _root_path
  if _root_path is None :
    raise Exception("Protocol not initialized!")
  return os.path.join(_root_path, str(key))

def name():
  """ :return: name of the protocol. Only lowercase letters, numbers or '_'."""
  return "filepic"
