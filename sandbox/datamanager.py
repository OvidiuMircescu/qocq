"""
Read & write results.
"""

import asyncio

_protocol = None
_next_id = 0

def activate_protocol(protocol, *args, **kwargs):
  global _protocol
  if _protocol is None :
    import file_pickle_protocol
    if protocol == file_pickle_protocol.name():
      _protocol = file_pickle_protocol
      _protocol.init(*args, **kwargs)
    else:
      raise Exception("Unknown protocol {}!".format(protocol))
  else:
    raise Exception("Protocol already defined!")

def close():
  global _protocol
  if _protocol:
    _protocol.close()

def protocol_name():
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  return _protocol.name()

def protocol_config():
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  return _protocol.config()

async def save_future(value, key_prefix=None):
  """ Save a value into the database.
      Called from the scheduler, in the main thread.
      No multithread protection needed.
  """
  global _next_id
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  if key_prefix is None:
    new_key = "_"+str(_next_id)
  else:
    new_key = str(key_prefix) + "_"+str(_next_id)
  _next_id += 1
  protocol_key = _protocol.save(new_key, value)
  return protocol_key

def new_key(key_prefix=None):
  """ Create a key where a result can be saved.
      Called from the scheduler, in the main thread.
      No multithread protection needed.
  """
  global _next_id
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  if key_prefix is None:
    new_key = "_"+str(_next_id)
  else:
    new_key = str(key_prefix) + "_"+str(_next_id)
  _next_id += 1
  protocol_key = _protocol.result_key(new_key)
  return protocol_key


def get(key):
  """ Called when running the executor, in the working task in a remote process.
  """
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  return _protocol.get(key)

def save(key, value):
  """ Called when running the executor, in the working task in a remote process.
  """
  global _protocol
  if _protocol is None :
    raise Exception("Protocol not defined!")
  _protocol.save(key, value)
