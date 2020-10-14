import inspect

from .map import IOMap, IOMapKey
from typing import Optional, List, Dict, Callable, Union
from .context import io_structure_leaves, io_flattened_structure

class IOMapGraph(IOMap):
  key_maps: List[Dict[str, any]]
  construct_keys: List[str]
  input_keys: List[str]
  output_keys: List[str]
  private_keys: List[str]

  def __init__(self, key_maps: Union[List[any], Dict[str, any], str], input_keys: List[str]=[], output_keys: List[str]=[], private_keys: List[str]=[], **kwargs):
    self.key_maps = self._flatten_key_maps(key_maps)
    self.construct_keys = list(kwargs.keys())
    self.input_keys = [*input_keys]
    self.output_keys = [*output_keys]
    for key_map in self.key_maps:
      if IOMapKey.input.value in key_map:
        for input_leaf in io_structure_leaves(key_map[IOMapKey.input.value]):
          input_key = input_leaf.split('.')[1] if isinstance(input_leaf, str) and input_leaf.startswith('input.') else None
          if input_key and input_key not in self.input_keys:
            self.input_keys.append(input_key)
            setattr(self, input_key, None)
      if IOMapKey.output.value in key_map:
        for output_leaf in io_structure_leaves(key_map[IOMapKey.output.value]):
          output_key = output_leaf.split('.')[1] if isinstance(output_leaf, str) and output_leaf.startswith('output.') else None
          if output_key and output_key not in self.output_keys:
            self.output_keys.append(output_key)
            setattr(self, output_key, None)
    self.private_keys = [*private_keys]
    for key in self.construct_keys:
      self[f'construct.{key}'] = kwargs[key]

  def key_is_private(self, key: str) -> bool:
    return key in self.private_keys or super().key_is_private(key)

  @property
  def _key_maps(self) -> List[Dict[str, any]]:
    return self.key_maps

  @property
  def _construct_keys(self) -> List[str]:
    return self.construct_keys

  @property
  def _input_keys(self) -> List[str]:
    return self.input_keys

  @property
  def _output_keys(self) -> List[str]:
    return self.output_keys

  def _flatten_key_maps(self, key_maps: Union[List[any], Dict[str, any], str]) -> List[Dict[str, any]]:
    if not isinstance(key_maps, list):
      key_maps = [key_maps]
    def flatten_string(structure: Optional[any], depth: int,) -> Optional[any]:
      for _ in range(type(self)._max_concurrency):
        if not isinstance(structure, str):
          return structure
        structure = self[structure]
      return structure
      
    flattened_key_maps = io_flattened_structure(
      structure=key_maps,
      depth=type(self)._max_concurrency,
      transformer=flatten_string
    )
    return flattened_key_maps

class IOMapEach(IOMap):
  key_map: Dict[str, any]
  unmapped: Optional[any]=None
  mapped: Optional[any]=None

  def __init__(self, key_map: Optional[Dict[str, any]]=None):
    self.key_map = self._prepared_key_map(key_map=key_map)

  @classmethod
  def get_run_keys(cls) -> List[str]:
    return ['item', 'mapped_item']

  @property
  def _key_maps(self) -> List[Dict[str, any]]:
    return [self.key_map]

  def _prepared_key_map(self, key_map: Optional[Dict[str, any]]) -> Optional[Dict[str, any]]:
    return {
      IOMapKey.input.value: 'run.unmapped',
      IOMapKey.output.value: 'run.mapped',
      **key_map,
    } if key_map else key_map

  def run(self, items: List[Optional[any]], key_map: Optional[Dict[str, any]]=None, **kwargs) -> List[Optional[any]]:
    run_key_map = self._prepared_key_map(key_map=key_map) if key_map else self.key_map
    self.prepare_run(**kwargs)
    output = []
    for item in items:
      self.unmapped = item
      self.mapped = None
      expanded_map = self.expand_key_map(key_map={**run_key_map})
      self.run_map(expanded_map=expanded_map)
      output.append(self.mapped)
    self.clear_run()
    return output

class IOMapZip(IOMap):
  def run(self, keys: List[any], values: List[any]) -> Dict[any, any]:
    return dict(zip(keys, values))

class IOMapCall(IOMap):
  callback: Callable[..., any]

  def __init__(self, callback: Callable[..., any]):
    self.callback = callback

  @property
  def _input_keys(self) -> List[str]:
    signature = inspect.signature(self.callback)
    return list(signature.parameters.keys())

  def run(self, *args, **kwargs):
    self.prepare_run(**kwargs)
    result = self.callback(*args, **kwargs)
    self.clear_run()
    return result

class IOMapPassthrough(IOMap):
  def run(self, *args, **kwargs):
    run_args = {
      **{
        str(i): v
        for i, v in enumerate(args)
      },
      **kwargs,
    }
    return run_args

class IOMapConstantKey(IOMap):
  key_constant: str
  fallback_keys: List[str]

  def __init__(self, key_constant: str, fallback_keys: List[str]=[]):
    self.key_constant = key_constant
    self.fallback_keys = [*fallback_keys]

  def run(self, *args, **kwargs) -> Optional[any]:
    run_args = {
      **{
        str(i): v
        for i, v in enumerate(args)
      },
      **kwargs,
    }
    self.prepare_run(**run_args)
    keys = [self.key_constant, *self.fallback_keys]
    for index, key in enumerate(keys):
      try:
        output = self[key]
        break
      except KeyError:
        if index == len(keys) - 1:
          raise
    self.clear_run()
    return output

class AllMap(IOMap):
  results: Optional[List[Dict[str, any]]]=None

  @classmethod
  def _get_map_identifier(cls) -> str:
    return 'all'

  def __init__(self, maps):
    self.maps = [
      {
        IOMapKey.map.value: m,
        IOMapKey.input.value: 'input.each_input',
      }
      for m in maps
    ]

  def run(self, each_input: Dict[str, any]) -> Dict[str, any]:
    super().run(
      each_input=each_input
    )
    self.results = []
    for each_map in self.maps:
      self.results.append(each_map.run(**each_input))
