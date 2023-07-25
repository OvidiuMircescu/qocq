import json

class ContainerProperties():
  def __init__(self, name, nb_cores, use_cache):
    self.name = name
    self.nb_cores = nb_cores
    self.use_cache = use_cache

def jsonContainerEncoder(obj):
  if isinstance(obj, ContainerProperties) :
    return {
            "name": obj.name,
            "nb_cores": obj.nb_cores,
            "use_cache": obj.use_cache }
  else:
    raise TypeError("Cannot serialize object "+str(obj))

def jsonContainerDecoder(dct):
  if "name" in dct and "nb_cores" in dct and "use_cache" in dct :
    return ContainerProperties(dct["name"], dct["nb_cores"], dct["use_cache"])
  if "name" in dct and "nb_cores" in dct :
    return ContainerProperties(dct["name"], dct["nb_cores"], False)
  return dct

class ContainerManager():
  defaultContainerName = "default_container"
  noContainerName = "nocontainer"
  def __init__(self):
    self._containers = []
    self._defaultContainer = ContainerProperties(
                                ContainerManager.defaultContainerName, 1, False)
    self._containers.append(self._defaultContainer)

  def setDefaultContainer(self, nb_cores, use_cache):
    self._defaultContainer.nb_cores = nb_cores
    self._defaultContainer.use_cache = use_cache

  def loadFile(self, file_path):
    with open(file_path, 'r') as json_file:
      self._containers = json.load(json_file, object_hook=jsonContainerDecoder)
    try:
      self._defaultContainer = next(cont for cont in self._containers
                          if cont.name == ContainerManager.defaultContainerName)
    except StopIteration:
      self._defaultContainer = ContainerProperties(
                                ContainerManager.defaultContainerName, 1, False)
      self._containers.append(self._defaultContainer)

  def saveFile(self, file_path):
    with open(file_path, 'w') as json_file:
      json.dump(self._containers, json_file,
                indent=2, default=jsonContainerEncoder)

  def addContainer(self, name, nb_cores, use_cache):
    try:
      # if the name already exists
      obj = next(cont for cont in self._containers if cont.name == name)
      obj.nb_cores = nb_cores
      obj.use_cache = use_cache
    except StopIteration:
      # new container
      self._containers.append(ContainerProperties(name, nb_cores, use_cache))

  def getContainer(self, name):
    ret = self._defaultContainer
    try:
      ret = next(cont for cont in self._containers if cont.name == name)
    except StopIteration:
      # not found
      pass
    return ret
