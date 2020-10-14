import pandas as pd
import urllib
import json

from typing import Dict, List, Tuple, Optional, Type
from datetime import datetime
from .map import IOMap, IOMapKey, IOMapOption
from .context import io_pruned_structure

class IOSourceReporter(IOMap):
  columns: List[str]
  filters: Dict[str, any]
  options: Dict[str, any]

  def __init__(self, columns: List[str], filters: Dict[str, any]={}, options: Dict[str, any]={}):
    self.columns = columns
    self.filters = filters
    self.options = options

  @classmethod
  def append_metadata(cls, metadata_entry: Dict[str, any], metadata: Optional[str]=None):
    if IOMapKey.map.value not in metadata_entry:
      metadata_entry = {
        **metadata_entry,
        IOMapKey.map.value: cls._get_map_identifier(),
      }
    if not pd.isna(metadata):
      metadata_list = json.loads(metadata)
      assert isinstance(metadata_list, list), 'Source report metadata must be stored as a JSON array.'
    else:
      metadata_list = []
    metadata_list.append(metadata_entry)
    return json.dumps(metadata_list, sort_keys=True)

  def finalize_report_columns(self, report: pd.DataFrame, columns: List[str]) -> pd.DataFrame:
    if not columns:
      return pd.DataFrame()
    existing_columns = sorted(set(columns).intersection(report.columns))
    missing_columns = sorted(set(columns) - set(existing_columns))
    extra_columns = sorted(set(report.columns) - set(columns))
    finalized_report = report
    finalized_report.drop(columns=extra_columns, inplace=True)
    for missing_column in missing_columns:
      finalized_report[missing_column] = None
    finalized_report = finalized_report[columns]
    return finalized_report

  def finalize_report_rows(self, report: pd.DataFrame, sort: Optional[any]=None) -> pd.DataFrame:
    finalized_report = report
    if finalized_report.empty:
      return finalized_report
    if sort is not None:
      finalized_report.sort_values(by=sort, inplace=True)
    finalized_report.reset_index(drop=True, inplace=True)
    return finalized_report

class IOMultiSourceReporter(IOSourceReporter):
  credentials: Optional[Dict[str, Dict[str, any]]]
  source_reports: Optional[Dict[str, pd.DataFrame]]
  source_credentials: Optional[Dict[str, Dict[str, any]]]

  @property
  def sources(self) -> List[str]:
    return sorted(self.credentials.keys()) if self.credentials is not None else None

  @property
  def source_columns(self) -> List[str]:
    source_columns = []
    for column in self.columns:
      source_column = self.parse_target(column)[-1]
      if source_column not in source_columns:
        source_columns.append(source_column)
    return source_columns

  def handle_map_run_error(self, run: Dict[str, any], error: Exception) -> any:
    return pd.DataFrame([{
      '': self.append_metadata({
        'credential': run[IOMapKey.key.value],
        'error': repr(error),
      }),
    }])

  def get_source_class(self, source: str) -> Type:
    _, source_location = self.parse_source(source)
    source_class = self[f'iomap.{source_location}']
    assert issubclass(source_class, IOSourceReporter)
    return source_class

  def generate_source_maps(self) -> List[Dict[str, any]]:
    return [
      {
        IOMapKey.map.value: self.get_source_class(s)(
          columns=self.columns_for_source(s),
          filters=self.filters_for_source(s),
          options=self.options_for_source(s)
        ),
        IOMapKey.key.value: s,
        IOMapKey.input.value: f'run.source_credentials.source_{i}',
        IOMapKey.output.value: f'run.source_reports.source_{i}',
      }
      for i, s in enumerate(self.sources)
    ] 

  def parse_source(self, url: str) -> Tuple[str]:
    parts = urllib.parse.urlparse(url)
    if not parts.scheme:
      parts = urllib.parse.urlparse(f'io://{url}')
    return (
      parts.username,
      parts.hostname + parts.path,
    )

  def parse_target(self, url: str) -> Tuple[str]:
    parts = urllib.parse.urlparse(url)
    if not parts.scheme:
      parts = urllib.parse.urlparse(f'io://{url}')
    path_components = (parts.hostname + parts.path).split('/')
    return (
      parts.username,
      '/'.join(path_components[:-1]),
      path_components[-1],
    )

  def source_match(self, target: str, source: str) -> Optional[str]:
    source_user, source_location = self.parse_source(source)
    target_user, target_location, target_key = self.parse_target(target)
    if target_user and target_user != source_user:
      return None
    if target_location and target_location != source_location:
      return None
    return target_key

  def properties_for_source(self, source: str, properties: Dict[str, any]) -> Dict[str, any]:
    source_properties = {
      self.source_match(k, source): v
      for k, v in properties.items()
      if '@' not in k and '/' not in k
    }
    source_properties.update({
      self.source_match(k, source): v
      for k, v in properties.items()
      if '@' not in k and '/' in k
    })
    source_properties.update({
      self.source_match(k, source): v
      for k, v in properties.items()
      if '@' in k
    })
    return io_pruned_structure(source_properties) or {}

  def columns_for_source(self, source: str) -> List[str]:
    return io_pruned_structure([
      self.source_match(v, source)
      for v in self.columns
    ]) or []

  def filters_for_source(self, source: str) -> List[str]:
    return self.properties_for_source(
      source=source,
      properties=self.filters
    )

  def options_for_source(self, source: str) -> Dict[str, any]:
    return self.properties_for_source(
      source=source,
      properties=self.options
    )

  def combine_source_reports(self) -> pd.DataFrame:
    report = pd.DataFrame()
    for index, source in enumerate(self.sources):
      source_report = self.source_reports[f'source_{index}']
      if '' not in source_report:
        source_report[''] = None
      metadata = {'credential': source}
      source_report[''] = source_report[''].apply(lambda m: self.append_metadata(metadata, m))
      report = report.append(source_report)
    return report

  def run(self, credentials: Dict[str, Dict[str, any]]) -> Dict[str, any]:
    self.prepare_run(
      credentials=credentials,
      source_reports={},
      source_credentials={}
    )
    self.source_credentials = {
      f'source_{i}': credentials[s]
      for i, s in enumerate(self.sources)
    }
    self.maps = self.generate_source_maps()
    self.run_maps()
    report = self.combine_source_reports()
    report = self.finalize_report_columns(
      report=report,
      columns=['', *self.source_columns] if '' not in self.source_columns else self.source_columns
    )
    report = self.finalize_report_rows(
      report=report
    )
    self.clear_run()
    return report

class IOSingleSourceReporter(IOSourceReporter):
  pass  