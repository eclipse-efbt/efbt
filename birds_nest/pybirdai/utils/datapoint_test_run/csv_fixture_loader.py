"""
CSV Fixture Loader for BIRD data model test data.

Loads test data from CSV files (one per table) into the database.
Replaces the legacy SQL INSERT-based fixture loading.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Dict, List, Type, Any, Optional, Tuple

from django.db import connection, models, transaction
from django.utils.dateparse import parse_date, parse_datetime

from pybirdai.utils.datapoint_test_run.test_data_template_utils import (
    get_bird_model_classes,
    get_model_by_table_name,
)

logger = logging.getLogger(__name__)


class CSVFixtureLoader:
    """
    Loads test data from CSV files into BIRD data model tables.

    CSV files should:
    - Have one file per table (e.g., prty.csv, instrmnt.csv)
    - Have header row with field names matching Django model fields
    - Use ISO 8601 date format (YYYY-MM-DD or YYYY-MM-DD HH:MM:SS)
    - Use empty strings for NULL values
    - Store foreign key values as the referenced unique ID
    """

    def __init__(self):
        self._model_cache: Optional[Dict[str, Type[models.Model]]] = None

    def _get_model_cache(self) -> Dict[str, Type[models.Model]]:
        """Get cached model classes."""
        if self._model_cache is None:
            self._model_cache = get_bird_model_classes()
        return self._model_cache

    def _parse_csv_value(self, field: models.Field, raw_value: str) -> Any:
        """
        Parse a CSV string value into the appropriate Python type for the field.

        Args:
            field: Django model field
            raw_value: Raw string value from CSV

        Returns:
            Parsed value appropriate for the field type

        Raises:
            ValueError: If value cannot be parsed
        """
        # Handle empty strings as NULL
        if raw_value == '' or raw_value is None:
            return None

        # Strip whitespace
        raw_value = str(raw_value).strip()
        if raw_value == '':
            return None
        if raw_value.upper() == 'NULL':
            return None

        if getattr(field, 'choices', None):
            choice_value = self._normalize_choice_value(field, raw_value)
            if choice_value is not None:
                raw_value = choice_value

        # DateTimeField
        if isinstance(field, models.DateTimeField):
            parsed_datetime = parse_datetime(raw_value)
            if parsed_datetime is not None:
                return parsed_datetime

            parsed_date = parse_date(raw_value)
            if parsed_date is not None:
                return datetime.combine(parsed_date, datetime.min.time())

            raise ValueError(f"Invalid datetime format for {field.name}: {raw_value}")

        # DateField
        if isinstance(field, models.DateField):
            parsed_date = parse_date(raw_value)
            if parsed_date is not None:
                return parsed_date

            parsed_datetime = parse_datetime(raw_value)
            if parsed_datetime is not None:
                return parsed_datetime.date()

            raise ValueError(f"Invalid date format for {field.name}: {raw_value}")

        # Integer fields
        if isinstance(field, (models.BigIntegerField, models.IntegerField, models.SmallIntegerField)):
            try:
                # Handle float strings like "200000000.0"
                return int(float(raw_value))
            except ValueError:
                raise ValueError(f"Invalid integer for {field.name}: {raw_value}")

        # Float fields
        if isinstance(field, (models.FloatField, models.DecimalField)):
            try:
                return float(raw_value)
            except ValueError:
                raise ValueError(f"Invalid float for {field.name}: {raw_value}")

        # Boolean fields
        if isinstance(field, models.BooleanField):
            return raw_value.lower() in ('true', '1', 'yes', 't')

        # CharField and others - return as string
        return raw_value

    def _normalize_choice_value(self, field: models.Field, raw_value: str) -> Optional[str]:
        """Normalize CSV enum display values back to the stored choice key."""
        choices = getattr(field, 'choices', None)
        if not choices:
            return None

        try:
            choice_map = dict(choices)
        except (TypeError, ValueError):
            return None

        if raw_value in choice_map:
            return raw_value

        if ':' in raw_value:
            code_candidate = raw_value.split(':', 1)[0].strip()
            if code_candidate in choice_map:
                return code_candidate

        normalized_raw = raw_value.replace(' ', '_')
        for code, description in choice_map.items():
            if raw_value == description or normalized_raw == description:
                return str(code)

        return None

    def _get_field_by_name(self, model_class: Type[models.Model], field_name: str) -> Optional[models.Field]:
        """
        Get a field from a model by name, handling FK _id suffix.

        Args:
            model_class: Django model class
            field_name: Field name (may include _id suffix for FKs)

        Returns:
            Django field or None if not found
        """
        try:
            return model_class._meta.get_field(field_name)
        except Exception:
            pass

        # Try without _id suffix (for FK fields specified with _id)
        if field_name.endswith('_id'):
            base_name = field_name[:-3]
            try:
                return model_class._meta.get_field(base_name)
            except Exception:
                pass

        return None

    def _get_insertable_field_map(
        self,
        model_class: Type[models.Model],
    ) -> Dict[str, Tuple[models.Field, str]]:
        """
        Build CSV-header to local database-column mappings for direct inserts.

        Multi-table inherited BIRD models cannot be bulk-created through the ORM.
        For fixture loading we insert into each concrete table from its own CSV,
        so only local fields belong in the INSERT column list.
        """
        field_map = {}

        for field in model_class._meta.local_fields:
            if not getattr(field, 'column', None):
                continue

            for key in {field.name, getattr(field, 'attname', field.name), field.column}:
                field_map[key] = (field, field.column)

        return field_map

    def _parse_insert_value(self, field: models.Field, raw_value: str) -> Any:
        """Parse a CSV value for direct database insertion."""
        if raw_value is None:
            return None

        raw_value = str(raw_value).strip()
        if raw_value == '' or raw_value.upper() == 'NULL':
            return None

        if isinstance(field, models.ForeignKey):
            return raw_value

        return self._parse_csv_value(field, raw_value)

    def _insert_csv_file_direct(
        self,
        csv_path: str,
        model_class: Type[models.Model],
        encoding: str = 'utf-8-sig',
    ) -> int:
        """
        Insert one CSV fixture directly into its model table.

        Unknown columns are ignored with a warning so fixture files can carry
        harmless metadata without blocking the load.
        """
        insertable_fields = self._get_insertable_field_map(model_class)
        if not insertable_fields:
            logger.warning("No insertable fields found for model %s", model_class.__name__)
            return 0

        table_name = model_class._meta.db_table
        quote_name = connection.ops.quote_name
        inserted_count = 0

        with open(csv_path, 'r', encoding=encoding, newline='') as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError(f"CSV file has no headers: {csv_path}")

            header_map = {}
            ignored_columns = []
            for raw_header in reader.fieldnames:
                field_name = (raw_header or '').lstrip('\ufeff').strip()
                if not field_name or field_name == 'rowid':
                    continue

                if field_name in insertable_fields:
                    header_map[raw_header] = (field_name, *insertable_fields[field_name])
                else:
                    ignored_columns.append(field_name)

            if ignored_columns:
                logger.warning(
                    "Ignoring %s unknown CSV column(s) for %s: %s",
                    len(ignored_columns),
                    model_class.__name__,
                    ', '.join(ignored_columns),
                )

            if not header_map:
                logger.warning("No usable columns in CSV file: %s", csv_path)
                return 0

            with connection.cursor() as cursor:
                for row_num, row in enumerate(reader, start=2):
                    values_by_column = {}
                    try:
                        for raw_header, (_mapped_name, field, column_name) in header_map.items():
                            values_by_column[column_name] = self._parse_insert_value(
                                field,
                                row.get(raw_header),
                            )
                    except Exception as e:
                        logger.error(f"Error parsing row {row_num} in {csv_path}: {e}")
                        raise ValueError(f"Error parsing row {row_num} in {csv_path}: {e}")

                    columns = list(values_by_column.keys())
                    values = [values_by_column[column] for column in columns]
                    placeholders = ', '.join(['%s'] * len(columns))
                    quoted_columns = ', '.join(quote_name(column) for column in columns)
                    sql = (
                        f"INSERT INTO {quote_name(table_name)} "
                        f"({quoted_columns}) VALUES ({placeholders})"
                    )
                    cursor.execute(sql, values)
                    inserted_count += 1

        logger.info("Inserted %s records into %s from %s", inserted_count, model_class.__name__, csv_path)
        return inserted_count

    def _sort_files_by_model_dependencies(
        self,
        file_to_model: Dict[str, Tuple[str, Type[models.Model]]],
    ) -> List[Tuple[str, Tuple[str, Type[models.Model]]]]:
        """Sort fixture files so inherited parent tables are loaded first."""
        items = list(file_to_model.items())
        model_to_files = {}
        for csv_file, (_table_name, model_class) in items:
            model_to_files.setdefault(model_class, []).append(csv_file)

        def dependencies_for(model_class: Type[models.Model]):
            dependencies = set()
            for field in model_class._meta.local_fields:
                if isinstance(field, models.ForeignKey):
                    related_model = field.remote_field.model
                    if related_model in model_to_files:
                        dependencies.add(related_model)
            return dependencies

        ordered_models = []
        temporary = set()
        permanent = set()

        def visit(model_class: Type[models.Model]):
            if model_class in permanent:
                return
            if model_class in temporary:
                return
            temporary.add(model_class)
            for dependency in dependencies_for(model_class):
                visit(dependency)
            temporary.remove(model_class)
            permanent.add(model_class)
            ordered_models.append(model_class)

        for _csv_file, (_table_name, model_class) in items:
            visit(model_class)

        model_rank = {model_class: idx for idx, model_class in enumerate(ordered_models)}
        return sorted(
            items,
            key=lambda item: (model_rank.get(item[1][1], 999), item[0]),
        )

    def load_csv_file(
        self,
        csv_path: str,
        model_class: Type[models.Model],
        encoding: str = 'utf-8-sig'
    ) -> List[models.Model]:
        """
        Load a single CSV file and return list of model instances.

        Args:
            csv_path: Path to CSV file
            model_class: Django model class to instantiate
            encoding: File encoding (default: utf-8)

        Returns:
            List of model instances (not yet saved to database)

        Raises:
            FileNotFoundError: If CSV file doesn't exist
            ValueError: If CSV format is invalid
        """
        instances = []

        with open(csv_path, 'r', encoding=encoding, newline='') as f:
            reader = csv.DictReader(f)

            if reader.fieldnames is None:
                raise ValueError(f"CSV file has no headers: {csv_path}")

            for row_num, row in enumerate(reader, start=2):  # Start at 2 (header is row 1)
                try:
                    instance_data = {}

                    for field_name, raw_value in row.items():
                        if field_name is None:
                            continue

                        field_name = field_name.lstrip('\ufeff').strip()
                        if not field_name:
                            continue

                        field = self._get_field_by_name(model_class, field_name)

                        if isinstance(field, models.ForeignKey):
                            if raw_value and raw_value.strip():
                                fk_value = raw_value.strip()
                                instance_data[field.attname] = None if fk_value.upper() == 'NULL' else fk_value
                            else:
                                instance_data[field.attname] = None
                        else:
                            # Regular field - parse according to type
                            if field is not None:
                                parsed_value = self._parse_csv_value(field, raw_value)
                                instance_data[field_name] = parsed_value
                            else:
                                if raw_value and raw_value.strip():
                                    logger.debug(
                                        "Ignoring unknown CSV column '%s' for model %s",
                                        field_name,
                                        model_class.__name__,
                                    )

                    instances.append(model_class(**instance_data))

                except Exception as e:
                    logger.error(f"Error parsing row {row_num} in {csv_path}: {e}")
                    raise ValueError(f"Error parsing row {row_num} in {csv_path}: {e}")

        logger.info(f"Loaded {len(instances)} records from {csv_path}")
        return instances

    def load_scenario_fixtures(
        self,
        scenario_path: str,
        use_transaction: bool = True
    ) -> Dict[str, int]:
        """
        Load all CSV files from a scenario directory.

        CSV files are loaded in order to respect foreign key constraints.
        Files are named after the table (e.g., prty.csv -> PRTY model).

        Args:
            scenario_path: Path to directory containing CSV files
            use_transaction: Whether to wrap in atomic transaction (default: True)

        Returns:
            Dict mapping table name to number of records loaded

        Raises:
            FileNotFoundError: If scenario_path doesn't exist
        """
        if not os.path.isdir(scenario_path):
            raise FileNotFoundError(f"Scenario directory not found: {scenario_path}")

        # Discover CSV files
        csv_files = [f for f in os.listdir(scenario_path) if f.lower().endswith('.csv')]

        if not csv_files:
            logger.warning(f"No CSV files found in {scenario_path}")
            return {}

        # Map CSV filenames to model classes
        file_to_model = {}
        for csv_file in csv_files:
            table_name = os.path.splitext(csv_file)[0]  # Remove .csv extension
            model_class = get_model_by_table_name(table_name)
            if model_class:
                file_to_model[csv_file] = (table_name, model_class)
            else:
                logger.warning(f"No model found for CSV file: {csv_file}")

        # Sort by model dependencies so inherited parent tables load first.
        sorted_files = self._sort_files_by_model_dependencies(file_to_model)

        # Load files
        results = {}

        def do_load():
            for csv_file, (table_name, model_class) in sorted_files:
                csv_path = os.path.join(scenario_path, csv_file)
                inserted_count = self._insert_csv_file_direct(csv_path, model_class)

                if inserted_count:
                    results[model_class.__name__] = results.get(model_class.__name__, 0) + inserted_count

        if use_transaction:
            with transaction.atomic():
                with connection.constraint_checks_disabled():
                    do_load()
        else:
            with connection.constraint_checks_disabled():
                do_load()

        return results

    def has_csv_fixtures(self, scenario_path: str) -> bool:
        """
        Check if a scenario directory contains CSV fixtures.

        Args:
            scenario_path: Path to scenario directory

        Returns:
            True if CSV files exist in the directory
        """
        if not os.path.isdir(scenario_path):
            return False

        csv_files = [f for f in os.listdir(scenario_path) if f.lower().endswith('.csv')]
        return len(csv_files) > 0


def load_csv_fixtures(scenario_path: str) -> Dict[str, int]:
    """
    Convenience function to load CSV fixtures from a scenario directory.

    Args:
        scenario_path: Path to directory containing CSV files

    Returns:
        Dict mapping table name to number of records loaded
    """
    loader = CSVFixtureLoader()
    return loader.load_scenario_fixtures(scenario_path)
