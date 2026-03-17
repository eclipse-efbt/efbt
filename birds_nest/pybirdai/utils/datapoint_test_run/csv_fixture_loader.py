"""
CSV Fixture Loader for BIRD data model test data.

Loads test data from CSV files (one per table) into the database.
Replaces the legacy SQL INSERT-based fixture loading while maintaining
backwards compatibility.
"""

import csv
import logging
import os
from datetime import datetime
from typing import Dict, List, Type, Any, Optional

from django.db import models, transaction
from django.utils.dateparse import parse_date, parse_datetime

from pybirdai.utils.datapoint_test_run.test_data_template_utils import (
    get_bird_model_classes,
    get_model_by_table_name,
    get_table_load_order,
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

        # Sort by load order
        load_order = get_table_load_order()

        def sort_key(item):
            table_name = item[1][0].upper()
            try:
                return load_order.index(table_name)
            except ValueError:
                return 999  # Unknown tables last

        sorted_files = sorted(file_to_model.items(), key=sort_key)

        # Load files
        results = {}

        def do_load():
            for csv_file, (table_name, model_class) in sorted_files:
                csv_path = os.path.join(scenario_path, csv_file)
                instances = self.load_csv_file(csv_path, model_class)

                if instances:
                    model_class.objects.bulk_create(instances, ignore_conflicts=False)
                    results[table_name] = len(instances)
                    logger.info(f"Inserted {len(instances)} records into {model_class.__name__}")

        if use_transaction:
            with transaction.atomic():
                do_load()
        else:
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
