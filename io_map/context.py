from __future__ import annotations

from threading import Lock
from enum import Enum
from functools import total_ordering
from typing import Optional, Dict, List, Callable

def io_flattened_structure(structure: Optional[any], depth: int=-1, transformer: Optional[Callable[[Optional[any], int], Optional[any]]]=None) -> Optional[any]:
  if transformer is not None:
    structure = transformer(structure, depth)
  if depth == 0:
    return structure
  if isinstance(structure, list):
    flattened = []
    for nested_structure in structure:
      flattened_nested_structure = io_flattened_structure(
        structure=nested_structure,
        depth=depth - 1,
        transformer=transformer
      )
      if isinstance(flattened_nested_structure, list):
        flattened.extend(flattened_nested_structure)
      else:
        flattened.append(flattened_nested_structure)
  else:
    flattened = structure
  return flattened

def io_merged_structure(*args, recurse: Optional[Dict[any, any]]=None):
  if recurse is None:
    recurse = {}
  if not args:
    return None    
  if len(args) == 1:
    return args[0]
  if len(args) > 2:
    merged_structure = args[0]
    for structure in args[1:]:
      merged_structure = io_merged_structure(merged_structure, structure, recurse=recurse)
    return merged_structure
  merged_structure, structure = args
  if isinstance(merged_structure, dict) and isinstance(structure, dict):
    merged_structure = {**merged_structure}
    recursive_keys = set(merged_structure).intersection(set(structure)).intersection(set(recurse))
    recursive_structure = {
      k: io_merged_structure(merged_structure[k], structure[k], recurse=recurse[k])
      for k in recursive_keys
    }
    merged_structure.update(structure)
    merged_structure.update(recursive_structure)
  elif isinstance(merged_structure, list) and isinstance(structure, list):
    merged_structure = [*merged_structure]
    recursive_indices = set(range(len(merged_structure))).intersection(set(range(len(structure)))).intersection(set(recurse))
    recursive_structure = {
      k: io_merged_structure(merged_structure[k], structure[k], recurse=recurse[k])
      for k in recursive_indices
    }
    for index in range(len(structure)):
      if index < len(merged_structure):
        merged_structure[index] = structure[index]
      else:
        merged_structure.append(index)
    for index, value in recursive_structure.items():
      merged_structure[index] = value
  else:
    merged_structure = structure
  return merged_structure

def io_pruned_structure(structure: Optional[any]) -> Optional[any]:
  if isinstance(structure, list):
    pruned = list(filter(lambda v: v is not None, map(io_pruned_structure, structure)))
    return pruned if pruned else None
  elif isinstance(structure, dict):
    pruned = {
      k: io_pruned_structure(v)
      for k, v in structure.items()
    }
    pruned = {
      k: v
      for k, v in pruned.items()
      if k is not None and v is not None
    }
    return pruned if pruned else None
  else:
    return structure

def io_structure_leaves(structure: Optional[any]) -> List[any]:
  children = None
  if isinstance(structure, list):
    children = structure
  elif isinstance(structure, dict):
    children = list(structure.values())
  if children is not None:
    leaves = [
      l
      for c in children
      for l in io_structure_leaves(c)
    ]
  else:
    leaves = [structure]
  return leaves

def io_transform_leaves(transformer: Callable[[any], any], structure: Optional[any]) -> Optional[any]:
  if isinstance(structure, list):
    return list(map(lambda v: io_transform_leaves(transformer, v), structure))
  elif isinstance(structure, dict):
    return {
      k: io_transform_leaves(transformer, v)
      for k, v in structure.items()
    }
  else:
    return transformer(structure)

def io_structure_keys(structure: Optional[any]) -> List[any]:
  structure_keys = []
  children = []
  if isinstance(structure, list):
    children.extend(structure)
  elif isinstance(structure, dict):
    structure_keys.extend(structure.keys())
    children.extend(structure.values())
  keys = [
    *structure_keys,
    *[
      k
      for c in children
      for k in io_structure_keys(c)
    ],
  ]
  return keys

@total_ordering
class IOOrderedEnum(Enum):
  @classmethod
  def get_values(cls) -> List[str]:
    return [e.value for e in cls]

  def __lt__(self, other):
    if self.__class__ is other.__class__:
      cases = list(self.__class__)
      return cases.index(self) < cases.index(other)
    if isinstance(other, IOOrderedEnum):
      return False
    return NotImplemented

  @property
  def higher(self) -> List[IOOrderedEnum]:
    return [
      e
      for e in self.__class__
      if e > self
    ]

  @property
  def lower(self) -> List[IOOrderedEnum]:
    return reversed([
      e
      for e in self.__class__
      if e < self
    ])

class IOCounter(object):
  def __init__(self):
    self.value = 0
    self._lock = Lock()
      
  def increment(self, increment: int=1) -> int:
    with self._lock:
      self.value += increment
      value = self.value
    return value

  def decrement(self, decrement: int=1):
    return self.increment(increment=-decrement)
