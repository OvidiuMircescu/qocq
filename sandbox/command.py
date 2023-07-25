class Command:
  def __init__(self, function, args, kwargs, results, protocol):
    """
    """
    self._function = function
    self._args = args
    self._kwargs = kwargs
    self._results = results
    self._protocol = protocol

  def module_name():
    mod_name = self._function.__module__ # TODO : fix __main__ module name
    if mod_name == "__main__" :
      import inspect
      file_name = os.path.basename(inspect.getfile(self._function))
      mod_name = file_name[0:-3] # remove .py
    return mod_name

  def function_name():
    return self._function.__name__

  def function():
    return self._function

  def args():
    return self._args

  def kwargs():
    return self._kwargs

  def results():
    return self._results

  def protocol_name():
    return self._protocol.name()
  
  def protocol_config():
    return self._protocol.config()
