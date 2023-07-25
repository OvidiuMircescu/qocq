import typing
import inspect
import sys

def number_of_results(f):
  """
  Get the number of values returned by a function.
  """
  if sys.version_info >= (3,8) :
    return_annotation = inspect.signature(f).return_annotation
    if return_annotation != inspect.Signature.empty :
      if typing.get_origin(ret) == tuple :
        return len(typing.get_args(ret))
      else:
        return 1
  if hasattr(f, "qocq_nb_return"):
    return f.qocq_nb_return
  co = f.__code__
  props = _old_function_properties(co.co_filename, co.co_name)
  if props.outputs is None :
    f.qocq_nb_return = 0
    return 0
  else:
    f.qocq_nb_return = len(props.outputs)
    return f.qocq_nb_return

def _old_function_properties(file_path, function_name):
  """
  Old implementation based on YACS for older versions of python.
  Also used when study function is not annotated.
  """
  with open(file_path, 'r') as f:
    text_file = f.read()
  import py2yacs
  functions,errors = py2yacs.get_properties(text_file)
  result = [fn for fn in functions if fn.name == function_name]
  if len(result) < 1:
    raise Exception("File {} - function not found {}.".format(
                    file_path, function_name))
  result = result[0]
  error_string = ""
  if len(result.errors) > 0:
    error_string += "File {} - errors when parsing function {}\n".format(
                    file_path, function_name)
    error_string += '\n'.join(result.errors)
    raise Exception(error_string)
  return result
