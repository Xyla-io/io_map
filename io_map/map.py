from __future__ import annotations
import json
import inspect
import importlib

from enum import Enum
from typing import Optional, List, Dict, Type, Tuple, Union
from contextlib import contextmanager
from .context import io_transform_leaves, io_merged_structure, IOCounter

class IOMapEncoder(json.JSONEncoder):
  def default(self, obj):
    if isinstance(obj, IOMap):
      return obj._get_map_identifier()
    try:
      return json.JSONEncoder.default(self, obj)
    except TypeError:
      return str(obj)

class IOMapKey(Enum):
  map = 'map'
  key = 'key'
  options = 'options'
  construct = 'construct'
  calculate = 'calculate'
  input = 'input'
  output = 'output'
  run = 'run'
  maps = 'maps'
  iomap = 'iomap'
  iokeymap = 'iokeymap'
  iocontext = 'iocontext'

class IOMapOption(Enum):
  enabled = 'enabled'
  factory = 'factory'
  expand_at_run = 'expand_at_run'

class IOMapValueType(Enum):
  str = 'str'
  int = 'int'
  bool = 'bool'
  float = 'float'
  date = 'date'
  datetime = 'datetime'
  json = 'json'

  @classmethod
  def format_value(cls, value: any) -> str:
    use_case = cls.json
    for case in cls:
      if case.value_type and isinstance(value, case.value_type):
        use_case = case
        break
    return f'{use_case.value}.{use_case.format(value)}'
  
  @classmethod
  def parse_value(cls, formatted: str) -> any:
    parts = formatted.split('.', maxsplit=1)
    if len(parts) > 1:
      for case in cls:
        if case.value == parts[0]:
          return case.parse(parts[1])
    raise ValueError('Cannot parse value', formatted)

  @property
  def value_type(self) -> Optional[Type]:
    if self is IOMapValueType.date:
      raise NotImplementedError()
    elif self is IOMapValueType.datetime:
      raise NotImplementedError()
    elif self is IOMapValueType.str:
      return str
    elif self is IOMapValueType.int:
      return int
    elif self is IOMapValueType.bool:
      return bool
    elif self is IOMapValueType.float:
      return float
    else:
      return None

  def format(self, value: any) -> str:
    if self is IOMapValueType.date:
      raise NotImplementedError()
    elif self is IOMapValueType.datetime:
      raise NotImplementedError()
    elif self is IOMapValueType.json:
      return json.dumps(value, cls=IOMapEncoder, sort_keys=True, indent=2)
    return str(value) if value is None else str(self.value_type(value))
  
  def parse(self, formatted: str) -> Optional[any]:
    if formatted.lower() == 'none':
      return None
    elif self is IOMapValueType.date:
      raise NotImplementedError()
    elif self is IOMapValueType.datetime:
      raise NotImplementedError()
    elif self is IOMapValueType.bool:
      falsy_values = [
        '',
        '0',
        'false',
        'none',
        'null',
      ]
      return formatted.lower() not in falsy_values
    elif self is IOMapValueType.json:
      return json.loads(formatted)
    return self.value_type(formatted)

  def __getitem__(self, key: str) -> any:
    return self.parse(key)

class IOMapPath(Enum):
  index = 'index'
  join = 'join'

  @classmethod
  def forward_key(cls, key: str, target_map: IOMap, key_stack: List[str]=[]) -> Tuple[any, Union[str, List[any]]]:
    parts = key.split('.', maxsplit=1)
    if len(parts) > 1:
      for case in cls:
        if case.value == parts[0]:
          return case.forward(
            path=parts[1],
            map=map,
            key_stack=key_stack
          )
    raise ValueError('Cannot forward key', key)

  def forward(self, path: str, target_map: IOMap, key_stack: List[str]=[]):
    if self is IOMapPath.index:
      components = IOMapValueType.json.parse(path)
      assert isinstance(components, list), f'{components} index path is not a list'
      collection = target_map
      for component in components[:-1]:
        collection = collection[component]
      return (collection, components[-1:])
    elif self is IOMapPath.join:
      components = IOMapValueType.json.parse(path)
      assert isinstance(components, list) and {type(c) for c in components} == {str}, f'{components} join path is not a list of strings'
      key = ''.join(
        str(target_map[c])
        for c in components
      )
      return target_map._forward_key(
        key=key,
        key_stack=key_stack
      )

class IOMapMetaClass(type):
  def __init__(cls, name, bases, dct):
    super(IOMapMetaClass, cls).__init__(name, bases, dct)
    if cls.map_auto_register and name != 'IOMap':
      cls.register_map_type(map_type=cls, replace=True)

class IOMap(metaclass=IOMapMetaClass):
  map_auto_register: bool=False
  map_registry: Dict[str, Type]={}
  map_key: Optional[str]=None
  map_run_keys: Optional[List[str]]=None
  maps: Optional[List[Dict[str, any]]]=None
  _run_maps: Optional[List[Dict[str, any]]]=None
  _context_registry: Dict[str, any]={}
  _key_map_registry: Dict[str, Type]={}
  _map_registries: List[Dict[str, Type]]=[]
  _key_map_registries: List[Dict[str, Type]]=[]
  _context_registries: List[Dict[str, any]]=[]
  _concurrent_counter: IOCounter=IOCounter()
  _max_concurrency: int=256
  _clear_without_force: bool=False

  @classmethod
  def _push_registries(cls, clear: bool=True):
    cls._map_registries.append({**cls.map_registry})
    cls._key_map_registries.append({**cls._key_map_registry})
    cls._context_registries.append({**cls._context_registry})
    if clear:
      cls.map_registry.clear()
      cls._key_map_registry.clear()
      cls._context_registry.clear()

  @classmethod
  def _pop_registries(cls):
    cls.map_registry.clear()
    cls._key_map_registry.clear()
    cls._context_registry.clear()
    cls.map_registry.update(cls._map_registries.pop())
    cls._key_map_registry.update(cls._key_map_registries.pop())
    cls._context_registry.update(cls._context_registries.pop())

  @classmethod
  @contextmanager
  def _local_registries(cls, clear: bool=True):
    cls._push_registries(clear=clear)
    try:
      yield
    finally:
      cls._pop_registries()

  @classmethod
  def _register_map_identifiers(cls, identifiers: Union[List[str], Dict[str, str]]):
    for identifier in identifiers:
      if identifier not in cls.map_registry:
        if isinstance(identifiers, dict):
          cls.register_map_type(
            map_type=identifiers[identifier],
            identifier=identifier,
            include_dependencies=True
          )
        else:
          cls.register_map_type(
            map_type=identifier,
            include_dependencies=True
          )

  @classmethod
  def import_map_type(cls, identifier: str) -> Type:
    map_location_components = identifier.split('/')
    map_module_name = map_location_components[0]
    map_module = importlib.import_module(map_module_name)
    map_class = map_module
    for component in map_location_components[1:]:
      map_class = getattr(map_class, component)
    return map_class

  @classmethod
  def register_map_type(cls, map_type: Union[Type, str], identifier: Optional[str]=None, replace=False, include_dependencies=False):
    if isinstance(map_type, str):
      if identifier is None:
        identifier = map_type
      map_type = cls.import_map_type(identifier=map_type)
    elif identifier is None:
      identifier = map_type._get_map_identifier()
    assert issubclass(map_type, IOMap), f'Cannot register map type {cls} since it does not inherit from IOMap'
    if identifier is None:
      identifier = map_type._get_map_identifier()
    if not replace:
      assert identifier not in cls.map_registry, f'Cannot register map type {map_type} since {cls.map_registry[identifier]} is already registered for identifier {identifier}'
    cls.map_registry[identifier] = map_type
    if include_dependencies:
      for dependency in map_type.get_map_dependencies():
        if dependency not in cls.map_registry:
          cls.register_map_type(map_type=dependency, include_dependencies=True)

  @classmethod
  def unregister_map_type(cls, identifier: str, force: bool=False):
    if not force:
      assert identifier in cls.map_registry, f'No map type registered for identifier {identifier}'
    if identifier in cls.map_registry:
      del cls.map_registry[identifier]

  @classmethod
  def _register_key_maps(cls, key_maps: Dict[str, Dict[str, any]]):
    for identifier, key_map in key_maps.items():
      cls._register_key_map(
        identifier=identifier,
        key_map=key_map,
        replace=True
      )

  @classmethod
  def _register_key_map(cls, identifier: str, key_map: Dict[str, any], replace=False):
    if not replace:
      assert identifier not in cls._key_map_registry, f'Cannot register key map {key_map} since {cls._key_map_registry[identifier]} is already registered for identifier {identifier}'
    cls._key_map_registry[identifier] = key_map

  @classmethod
  def _unregister_key_map_type(cls, identifier: str, force: bool=False):
    if not force:
      assert identifier in cls._key_map_registry, f'No key map registered for identifier {identifier}'
    if identifier in cls._key_map_registry:
      del cls._key_map_registry[identifier]

  @classmethod
  def _register_context(cls, context: Dict[str, any]):
    for identifier, value in context.items():
      cls._register_context_value(
        identifier=identifier,
        value=value,
        replace=True
      )

  @classmethod
  def _register_context_value(cls, identifier: str, value: [any], replace=False):
    if not replace:
      assert identifier not in cls._context_registry, f'Cannot register context value {value} since {cls._context_registry[identifier]} is already registered for identifier {identifier}'
    cls._context_registry[identifier] = value

  @classmethod
  def _unregister_context_value(cls, identifier: str, force: bool=False):
    if not force:
      assert identifier in cls._context_registry, f'No context value registered for identifier {identifier}'
    if identifier in cls._context_registry:
      del cls._context_registry[identifier]

  @classmethod
  def _get_map_identifier(cls) -> str:
    return f'{cls.__module__}/{cls.__name__}'

  @classmethod
  def get_map_dependencies(cls) -> List[str]:
    dependencies = []
    for key_map in cls.get_key_maps():
      if IOMapKey.map.value not in key_map or not isinstance(key_map[IOMapKey.map.value], str):
        continue
      components = key_map[IOMapKey.map.value].split('.', maxsplit=1)
      if len(components) < 2 or components[0] != 'iomap':
        continue
      dependencies.append(components[1])
    return dependencies

  @classmethod
  def get_construct_keys(cls) -> List[str]:
    signature = inspect.signature(cls.__init__)
    return list(signature.parameters.keys())[1:]

  @classmethod
  def get_calcuate_keys(cls) -> List[str]:
    return []

  @classmethod
  def get_input_keys(cls) -> List[str]:
    signature = inspect.signature(cls.run)
    return list(signature.parameters.keys())[1:]

  @classmethod
  def get_run_keys(cls) -> List[str]:
    return []

  @classmethod
  def get_output_keys(cls) -> List[str]:
    return []

  @classmethod
  def get_key_maps(cls) -> List[Dict[str, any]]:
    return []

  @classmethod
  def get_key_map(cls) -> Dict[str, any]:
    key_map = {
      IOMapKey.map.value: cls._get_map_identifier(),
      IOMapKey.construct.value: cls.get_construct_keys(),
      IOMapKey.calculate.value: cls.get_calcuate_keys(),
      IOMapKey.input.value: cls.get_input_keys(),
      IOMapKey.run.value: cls.get_run_keys(),
      IOMapKey.output.value: cls.get_output_keys(),
      IOMapKey.maps.value: list(map(lambda m: {k: v._get_map_identifier() if k == IOMapKey.map.value and not isinstance(v, str) else v for k, v in m.items()}, cls.get_key_maps())),
    }
    key_map = {
      k: v
      for k, v in key_map.items() if v
    }
    return key_map

  @classmethod
  def _generate_full_key_map(cls, key_map: Dict[str, any]) -> Dict[str, any]:
    return {
      IOMapKey.key.value: None,
      IOMapKey.options.value: {},
      IOMapKey.construct.value: {},
      IOMapKey.input.value: {},
      IOMapKey.output.value: {},
      **key_map,
    }

  @classmethod
  def _generate_instantiated_key_map(cls, *args, **kwargs) -> Dict[str, any]:
    key_map = args[0]
    expander = cls(*args[1:], **kwargs)
    expanded_map = expander.expand_key_map(key_map=key_map)
    instantiated_map = {
      k: v
      for k, v in expanded_map.items()
      if k not in [IOMapKey.construct.value, IOMapKey.options.value]
    }
    return instantiated_map

  @classmethod
  def _merged_key_map(cls, *args) -> Dict[str, any]:
    return io_merged_structure(
      *args,
      recurse={
        IOMapKey.construct.value: {},
        IOMapKey.input.value: {},
        IOMapKey.output.value: {},
        IOMapKey.options.value: {},
      }
    )

  @contextmanager
  def _limit_concurrency(self):
    if self._concurrent_counter.increment() > self._max_concurrency and self._max_concurrency:
      raise RecursionError(f'Maximum IO map concurrency {self._max_concurrency} exceeded by IO map {self._identifier}')
    try:
      yield
    finally:
      self._concurrent_counter.decrement()

  @property
  def _identifier(self) -> str:
    return self.__class__._get_map_identifier()

  @property
  def _map_dependencies(self) -> List[str]:
    return self.__class__.get_map_dependencies()

  @property
  def _construct_keys(self) -> List[str]:
    return self.__class__.get_construct_keys()

  @property
  def _calculate_keys(self) -> List[str]:
    return self.__class__.get_calcuate_keys()

  @property
  def _input_keys(self) -> List[str]:
    return self.__class__.get_input_keys()

  @property
  def _run_keys(self) -> List[str]:
    return self.__class__.get_run_keys()

  @property
  def _output_keys(self) -> List[str]:
    return self.__class__.get_output_keys()

  @property
  def _key_maps(self) -> List[Dict[str, any]]:
    return self.__class__.get_key_maps()

  @property
  def _key_map(self) -> Dict[str, any]:
    return self.__class__.get_key_map()

  @property
  def json(self) -> str:
    return self.get_json()

  def key_is_private(self, key: str) -> bool:
    return key.startswith('_')

  def get_json(self, depth=-1):
    populated_map = self.get_populated_map(depth=depth)
    return json.dumps(populated_map, cls=IOMapEncoder, sort_keys=True, indent=2)

  @property
  def populated_construct(self) -> Dict[str, any]:
    return {
      k: self[f'construct.{k}']
      for k in self._construct_keys
    }

  @property
  def populated_calcuate(self) -> Dict[str, any]:
    return {
      k: self[f'calculate.{k}']
      for k in self._calculate_keys
    }

  @property
  def populated_input(self) -> Dict[str, any]:
    return {
      k: self[f'input.{k}']
      for k in self._input_keys
    }

  @property
  def populated_run(self) -> Dict[str, any]:
    run_keys = self._run_keys
    existing_keys = {
      *run_keys,
      *self._input_keys,
      *self._output_keys,
    }
    if self.map_run_keys is not None:
      run_keys.extend(filter(lambda k: k not in existing_keys, self.map_run_keys))
    return {
      k: self[f'run.{k}']
      for k in run_keys
    }

  @property
  def populated_output(self) -> Dict[str, any]:
    return {
      k: self[f'output.{k}']
      for k in self._output_keys
    }

  @property
  def populated_maps(self) -> List[Dict[str, any]]:
    return self.get_populated_maps()

  def get_populated_maps(self, depth: int=-1) -> List[Dict[str, any]]:
    populate_maps = []
    if self._run_maps:
      populate_maps.extend(self._run_maps)
    if self.maps:
      populate_maps.extend(self.maps[len(populate_maps):])
    return list(map(lambda m: {k: v.get_populated_map(depth=depth - 1) if k == IOMapKey.map.value and isinstance(v, IOMap) and depth != 0 else v for k, v in m.items()}, populate_maps))

  @property
  def populated_map(self) -> Dict[str, any]:
    return self.get_populated_map()

  def get_populated_map(self, depth: int=-1) -> Dict[str, any]:
    populated_map = {
      **self._key_map,
      IOMapKey.key.value: self.map_key,
      IOMapKey.construct.value: self.populated_construct,
      IOMapKey.calculate.value: self.populated_calcuate,
      IOMapKey.input.value: self.populated_input,
      IOMapKey.run.value: self.populated_run,
      IOMapKey.output.value: self.populated_output,
      IOMapKey.maps.value: self.get_populated_maps(depth=depth),
    }
    populated_map = {
      k: v
      for k, v in populated_map.items() if v
    }
    return populated_map

  @property
  def _construct_key_map(self) -> Dict[str, any]:
    return {
      IOMapKey.map.value: f'iomap.{self._identifier}',
      IOMapKey.construct.value: io_transform_leaves(lambda v: IOMapValueType.format_value(v), self.populated_construct),
    }

  def _full_key_map(self, key_map: Dict[str, any]) -> Dict[str, any]:
    return self.__class__._generate_full_key_map(key_map=key_map)

  def _key_map_option(self, key_map: Dict[str, any], option: str) -> Optional[any]:
    if IOMapKey.options.value not in key_map:
      return None
    options = key_map[IOMapKey.options.value]
    if not isinstance(options, dict) or option not in options:
      return None
    return options[option]

  def run(self, **kwargs) -> Optional[any]:
    with self._limit_concurrency():
      self.prepare_run(**kwargs)
      self.prepare_maps()
      self.run_maps()
      output = self.populated_output if self._output_keys else self[f'{IOMapKey.run.value}.{IOMapKey.output.value}']
      self.clear_run()
      return output

  def prepare_run(self, **kwargs):
    self.clear_run(force=True)
    self._run_maps = []
    self.map_run_keys = []
    for key, value in kwargs.items():
      self[f'run.{key}'] = value

  def clear_run(self, force: bool=False):
    if not force and not self._clear_without_force:
      return
    if self.map_run_keys is None:
      self.map_run_keys = []
    clear_keys = {
      *self._input_keys,
      *self._run_keys,
      *self._output_keys,
      *(self.map_run_keys if self.map_run_keys is not None else []),
    }
    for key in clear_keys:
      del self[f'run.{key}']
    self.map_run_keys = None
    self._run_maps = None

  def prepare_maps(self):
    if self.maps is not None:
      return
    maps = []
    for key_map in self._key_maps:
      if self._key_map_option(key_map=key_map, option=IOMapOption.expand_at_run.value) is not None:
        expanded_map = key_map
      else:
        expanded_map = self.expand_key_map(key_map=key_map)
      if expanded_map is None:
        continue
      maps.append(expanded_map)
    self.maps = maps

  def expand_key_map(self, key_map: Dict[str, any]) -> Optional[Dict[str, any]]:
    key_map_stack = []
    expand = {**key_map}
    while IOMapKey.iokeymap.value in expand:
      key_map_value = expand[IOMapKey.iokeymap.value]
      del expand[IOMapKey.iokeymap.value]
      if isinstance(key_map_value, str):
        if key_map_value in key_map_stack:
          raise RecursionError(f'{key_map_value} already in key map stack')
        key_map_value = self[key_map_value]
      expand = type(self)._merged_key_map(
        key_map_value,
        expand
      )
    expanded = {
      **expand,
      IOMapKey.construct.value: self.expand(
        structure=[expand[IOMapKey.construct.value]] if isinstance(expand[IOMapKey.construct.value], str) else expand[IOMapKey.construct.value]
      ) if IOMapKey.construct.value in expand else {},
      IOMapKey.options.value: self.expand(structure=expand[IOMapKey.options.value]) if IOMapKey.options.value in expand else {},
    }
    if IOMapOption.factory.value in expanded[IOMapKey.options.value]:
      return self[expanded[IOMapKey.options.value][IOMapOption.factory.value]](expanded)
    if IOMapOption.enabled.value in expanded[IOMapKey.options.value] and not expanded[IOMapKey.options.value][IOMapOption.enabled.value]:
      return None
    if isinstance(expanded[IOMapKey.map.value], str):
      expanded[IOMapKey.map.value] = self[expanded[IOMapKey.map.value]]
    if inspect.isclass(expanded[IOMapKey.map.value]):
      expanded[IOMapKey.map.value] = expanded[IOMapKey.map.value](*expanded[IOMapKey.construct.value]) if isinstance(expanded[IOMapKey.construct.value], list) else expanded[IOMapKey.map.value](**expanded[IOMapKey.construct.value])
    return expanded

  def run_maps(self):
    map_index = 0
    while map_index < len(self.maps):
      map_to_run = self.maps[map_index]
      self._run_maps.append(self.run_map(expanded_map=map_to_run))
      map_index += 1

  def run_map(self, expanded_map: Dict[str, any]) -> Dict[str, any]:
    expand_at_run = self._key_map_option(key_map=expanded_map, option=IOMapOption.expand_at_run.value)
    if expand_at_run is not None:
      if expand_at_run:
        run_key_map = self[expand_at_run]
        if run_key_map:
          expanded_map = {
            **expanded_map,
            **run_key_map,
          }
      expanded_map = self.expand_key_map(key_map=expanded_map)
    if expanded_map is None:
      return {}
    enabled = self._key_map_option(key_map=expanded_map, option=IOMapOption.enabled.value)
    if enabled is not None:
      enabled = self.expand(structure=enabled)
      if not enabled:
        return {}
    run = {
      **expanded_map,
      IOMapKey.input.value: self.expand(
        structure=[expanded_map[IOMapKey.input.value]] if isinstance(expanded_map[IOMapKey.input.value], str) else expanded_map[IOMapKey.input.value]
      ) if IOMapKey.input.value in expanded_map else {},
    }
    try:
      output = run[IOMapKey.map.value].run(*run[IOMapKey.input.value]) if isinstance(run[IOMapKey.input.value], list) else run[IOMapKey.map.value].run(**run[IOMapKey.input.value])
    except (KeyboardInterrupt, SystemExit):
      raise
    except Exception as e:
      output = self.handle_map_run_error(
        run=run,
        error=e
      )
    if IOMapKey.output.value in run:
      self.absorb_map(
        key_structure=run[IOMapKey.output.value],
        value_structure=output
      )
    return run

  def handle_map_run_error(self, run: Dict[str, any], error: Exception) -> any:
    raise error

  def absorb_map(self, key_structure: Union[str, Dict[str, str]], value_structure: Dict[str, any]):
    if isinstance(key_structure, str):
      self[key_structure] = value_structure
      return
    for key, value in key_structure.items():
      structure = value_structure
      components = key.split('.')
      for component in components[:-1]:
        structure = structure[int(component)] if isinstance(structure, list) else structure[component]
      self[value] = structure[components[-1]]
  
  def expand(self, structure: any):
    if isinstance(structure, str):
      return self[structure]
    elif isinstance(structure, list):
      return list(map(lambda s: self.expand(s), structure))
    elif isinstance(structure, dict):
      nested = {}
      for key, value in structure.items():
        components = key.split('.', maxsplit=1)
        if len(components) > 1:
          if components[0] not in nested:
            nested[components[0]] = {}
          nested[components[0]][components[1]] = value
        else:
          nested[key] = value
      nested = {
        k: self.expand(v)
        for k, v in nested.items()
      }
      return nested
    else:
      return structure

  def _forward_key(self, key: str, key_stack: List[str]=[]) -> Tuple[any, Union[str, List[any]]]:
    def collection_path(collection: any, collection_components: List[str]) -> Tuple[any, Union[str, List[any]]]:
      for collection_key in collection_components[:-1]:
        if isinstance(collection, IOMap):
          break
        collection = collection[int(collection_key) if isinstance(collection, list) else collection_key]
        collection_components = collection_components[1:]
      return (collection, collection_components)

    if key in key_stack:
      raise RecursionError(f'{key} already in key stack')
    components = key.split('.', maxsplit=1)
    realm = components[0]
    realm_path = components[1] if len(components) > 1 else None
    if realm in [t.value for t in IOMapValueType]:
      if realm_path is None:
        raise KeyError(key)
      value_type = IOMapValueType(realm)
      return (value_type, [realm_path])
    elif realm in [p.value for p in IOMapPath]:
      if realm_path is None:
        raise KeyError(key)
      path = IOMapPath(realm)
      return path.forward(
        path=realm_path,
        target_map=self,
        key_stack=key_stack
      )
    elif realm == IOMapKey.iomap.value:
      if realm_path not in self.map_registry:
        if self.map_auto_register:
          self.register_map_type(map_type=realm_path, include_dependencies=True)
        else:
          raise KeyError(key)
      return (self.map_registry, [realm_path])
    elif realm == IOMapKey.iokeymap.value:
      if realm_path not in self._key_map_registry:
          raise KeyError(key)
      return (self._key_map_registry, [realm_path])
    elif realm == IOMapKey.iocontext.value:
      context_components = realm_path.split('.')
      return collection_path(self._context_registry, context_components)
    elif realm == IOMapKey.key.value and realm_path is None:
      return (self, 'map_key')
    elif realm == IOMapKey.maps:
      if realm_path is None:
        return (self, 'maps')
      maps_path_components = realm_path.split('.')
      map_key = maps_path_components[0]
      map_path_components = maps_path_components[1:]
      for index, keyed_map in enumerate(self.maps):
        if keyed_map[IOMapKey.key] == map_key:
          if map_path_components:
            return (keyed_map, map_path_components)
          else:
            return (self.maps, index)
      raise KeyError(key)
    elif realm == IOMapKey.construct.value:
      keys = self._construct_keys
    elif realm == IOMapKey.calculate.value:
      keys = self._calculate_keys
    elif realm == IOMapKey.input.value:
      keys = self._input_keys
    elif realm == IOMapKey.run.value:
      keys = self._run_keys + self.map_run_keys if self.map_run_keys is not None else []
    elif realm == IOMapKey.output.value:
      keys = self._output_keys
    else:
      raise KeyError(key)
    if realm_path is None:
      raise KeyError(key)
    self_path_components = realm_path.split('.')
    self_key = self_path_components[0]
    if self.key_is_private(self_key):
      raise KeyError(key)
    collection_components = self_path_components[1:]
    if not collection_components:
      if self_key not in keys and realm not in [
        IOMapKey.calculate.value,
        IOMapKey.input.value,
        IOMapKey.run.value,
        IOMapKey.output.value,
      ]:
        raise KeyError(key)
      return (self, self_key)
    else:
      collection = getattr(self, self_key)
      return collection_path(collection, collection_components)

  def __getitem__(self, key: str) -> any:
    collection, collection_key = self._forward_key(key=key)
    if isinstance(collection_key, str):
      if collection is self and not hasattr(self, collection_key):
        value = None
      else:
        value = getattr(collection, collection_key)
    else:
      combined_key = '.'.join(collection_key)
      value = collection[int(combined_key) if isinstance(collection, list) else combined_key]
    return value   

  def __setitem__(self, key: str, value: any):
    collection, collection_key = self._forward_key(key=key)
    if isinstance(collection_key, str):
      if collection is self and key.split('.', maxsplit=1)[0] == IOMapKey.run.value:
        run_keys = [
          *self._run_keys,
          *(self.map_run_keys if self.map_run_keys is not None else []),
        ]
        if collection_key not in run_keys:
          self.map_run_keys.append(collection_key)
      setattr(collection, collection_key, value)
    else:
      combined_key = '.'.join(collection_key)
      collection[int(combined_key) if isinstance(collection, list) else combined_key] = value

  def __delitem__(self, key):
    collection, collection_key = self._forward_key(key=key)
    if isinstance(collection_key, str):
      if collection is self and key.split('.', maxsplit=1)[0] == IOMapKey.run.value:
        if collection_key in self.map_run_keys:
          delattr(self, collection_key)
          self.map_run_keys = list(filter(lambda k: k != collection_key, self.map_run_keys))
        else:
          method_and_property_keys = [
            t[0] 
            for t in inspect.getmembers(
              self.__class__,
              lambda m: inspect.isfunction(m) or isinstance(m, property)
            )
          ]
          if collection_key in method_and_property_keys:
            return
      setattr(collection, collection_key, None)
    else:
      combined_key = '.'.join(collection_key)
      del collection[int(combined_key) if isinstance(collection, list) else combined_key]

  def __repr__(self):
    return f'{type(self)} "{self._identifier}" <{hex(id(self))}>'

  def __str__(self):
    try:
      return f'{self.get_populated_map(depth=0)} {self.__repr__()}'
    except (SystemExit, KeyboardInterrupt):
      raise
    except Exception:
      return self.__repr__()
