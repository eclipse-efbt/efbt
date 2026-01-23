"""
Utilities for extracting model metadata and domain dictionaries from BIRD data models.

Used by the Excel template generator and CSV fixture loader to understand
the structure of BIRD data model tables.
"""

import logging
import os
import sys
from typing import Dict, List, Type, Any, Optional

from django.db import models

logger = logging.getLogger(__name__)


def _ensure_django_setup():
    """Ensure Django is properly configured."""
    if '.' not in sys.path:
        sys.path.insert(0, '.')
    if 'DJANGO_SETTINGS_MODULE' not in os.environ:
        os.environ['DJANGO_SETTINGS_MODULE'] = 'birds_nest.settings'

    import django
    from django.conf import settings
    if not settings.configured:
        django.setup()


def get_bird_model_classes() -> Dict[str, Type[models.Model]]:
    """
    Get all BIRD data model classes from bird_data_model.py.

    Returns:
        Dict mapping model name (e.g., 'PRTY') to Django model class.
    """
    _ensure_django_setup()

    try:
        from pybirdai.models import bird_data_model

        result = {}
        for name in dir(bird_data_model):
            obj = getattr(bird_data_model, name)
            if (isinstance(obj, type) and
                issubclass(obj, models.Model) and
                obj is not models.Model and
                hasattr(obj, '_meta') and
                hasattr(obj._meta, 'db_table')):
                result[name] = obj

        logger.debug(f"Discovered {len(result)} BIRD model classes")
        return result

    except Exception as e:
        logger.error(f"Failed to get BIRD model classes: {e}")
        return {}


def get_model_by_table_name(table_short_name: str) -> Optional[Type[models.Model]]:
    """
    Get a model class by its short table name (e.g., 'prty' -> PRTY model).

    Args:
        table_short_name: Lowercase table name without 'pybirdai_' prefix

    Returns:
        Django model class or None if not found
    """
    all_models = get_bird_model_classes()

    # Try exact match (uppercase)
    upper_name = table_short_name.upper()
    if upper_name in all_models:
        return all_models[upper_name]

    # Try matching by db_table name
    for model_name, model_class in all_models.items():
        db_table = model_class._meta.db_table
        # db_table is like 'pybirdai_prty'
        if db_table.lower() == f'pybirdai_{table_short_name.lower()}':
            return model_class
        if db_table.lower() == table_short_name.lower():
            return model_class

    return None


def extract_domain_dictionaries(model_class: Type[models.Model]) -> Dict[str, Dict[str, str]]:
    """
    Extract domain dictionaries from a BIRD model class.

    Domain dictionaries are defined as class attributes ending with '_domain'
    and are referenced by CharField fields via their 'choices' parameter.

    Example in model:
        DFLT_STTS_domain = {'14': 'Not_in_default', '18': 'Default_because_both...'}
        DFLT_STTS = models.CharField(choices=DFLT_STTS_domain, ...)

    Args:
        model_class: Django model class from bird_data_model

    Returns:
        Dict mapping field name to its domain dictionary {code: description}
    """
    domains = {}

    # Collect all domain dictionaries from class attributes
    domain_dicts = {}
    for attr_name in dir(model_class):
        if attr_name.endswith('_domain') and not attr_name.startswith('_'):
            domain_dict = getattr(model_class, attr_name, None)
            if isinstance(domain_dict, dict):
                domain_dicts[attr_name] = domain_dict

    # Match fields to their domain dictionaries
    for field in model_class._meta.get_fields():
        if not hasattr(field, 'choices') or not field.choices:
            continue

        # Get the field's choices as a dict
        if isinstance(field.choices, dict):
            field_choices = field.choices
        else:
            # Convert list of tuples to dict
            try:
                field_choices = dict(field.choices)
            except (TypeError, ValueError):
                continue

        # Find matching domain dict
        for domain_name, domain_dict in domain_dicts.items():
            if field_choices == domain_dict:
                domains[field.name] = domain_dict
                break
        else:
            # If no exact match found but field has choices, use them
            if field_choices:
                domains[field.name] = field_choices

    return domains


def get_model_fields_metadata(model_class: Type[models.Model]) -> List[Dict[str, Any]]:
    """
    Get metadata for all database fields in a model.

    Args:
        model_class: Django model class

    Returns:
        List of dicts with field metadata:
        - name: Field name
        - type: Field type class name (CharField, DateTimeField, etc.)
        - python_type: Python type for values (str, datetime, int, float)
        - required: Whether the field is required (not null/blank)
        - is_foreign_key: Whether this is a foreign key field
        - related_model: Name of related model (for FKs)
        - has_choices: Whether field has enumerated choices
        - max_length: Max length for CharFields
    """
    fields_meta = []

    for field in model_class._meta.get_fields():
        # Skip reverse relations and many-to-many
        if not hasattr(field, 'column') or field.column is None:
            continue

        meta = {
            'name': field.name,
            'type': field.__class__.__name__,
            'python_type': 'str',  # Default
            'required': not (getattr(field, 'null', True) and getattr(field, 'blank', True)),
            'is_foreign_key': isinstance(field, models.ForeignKey),
            'related_model': None,
            'has_choices': bool(getattr(field, 'choices', None)),
            'max_length': getattr(field, 'max_length', None),
        }

        # Determine Python type
        if isinstance(field, models.DateTimeField):
            meta['python_type'] = 'datetime'
        elif isinstance(field, models.DateField):
            meta['python_type'] = 'date'
        elif isinstance(field, (models.BigIntegerField, models.IntegerField, models.SmallIntegerField)):
            meta['python_type'] = 'int'
        elif isinstance(field, (models.FloatField, models.DecimalField)):
            meta['python_type'] = 'float'
        elif isinstance(field, models.BooleanField):
            meta['python_type'] = 'bool'

        # Get related model name for FKs
        if isinstance(field, models.ForeignKey):
            meta['related_model'] = field.related_model.__name__

        fields_meta.append(meta)

    return fields_meta


def get_field_names(model_class: Type[models.Model]) -> List[str]:
    """
    Get list of database field names for a model.

    Args:
        model_class: Django model class

    Returns:
        List of field names in definition order
    """
    return [
        field.name
        for field in model_class._meta.get_fields()
        if hasattr(field, 'column') and field.column is not None
    ]


# Default tables commonly used in test fixtures
DEFAULT_TEST_TABLES = [
    'PRTY',
    'FNNCL_CNTRCT',
    'CRDT_FCLTY',
    'INSTRMNT',
    'ENTTY_RL',
    'INSTRMNT_RL',
    'INSTRMNT_ENTTY_RL_ASSGNMNT',
    'CLLTRL',
    'CLLTRL_RL',
    'PRTCTN_ARRNGMNT',
    'PRTCTN_RCVD',
]


def get_table_load_order() -> List[str]:
    """
    Get the order in which tables should be loaded to respect foreign key constraints.

    Parent tables (referenced by FKs) should be loaded before child tables.

    Returns:
        List of table names in load order
    """
    # This order respects the FK relationships in the BIRD data model
    return [
        # Level 1: Base entities (no FK dependencies on other BIRD tables)
        'GRP',
        'PRTY',
        'SNDCTD_CNTRCT',
        'FNNCL_CNTRCT',
        'CRDT_FCLTY',
        'BLNC_SHT_NTTNG',
        'PRTCTN_ARRNGMNT',

        # Level 2: Entities with FK to Level 1
        'INSTRMNT',
        'PRTCTN_RCVD',
        'CLLTRL',
        'ENTTY_RL',

        # Level 3: Relationship tables with FK to Level 2
        'INSTRMNT_RL',
        'CLLTRL_RL',
        'PRTCTN_RCVD_INSTRMNT',

        # Level 4: Assignment tables (FK to multiple tables)
        'INSTRMNT_ENTTY_RL_ASSGNMNT',
        'PRTCTN_RCVD_RL',
    ]


def format_domain_value_for_dropdown(code: str, description: str, max_length: int = 50) -> str:
    """
    Format a domain code and description for Excel dropdown display.

    Args:
        code: The domain code (e.g., '14')
        description: The human-readable description
        max_length: Maximum total length (to avoid Excel issues)

    Returns:
        Formatted string like '14: Not_in_default'
    """
    # Replace underscores with spaces for readability
    clean_desc = description.replace('_', ' ')

    # Truncate if too long
    prefix = f"{code}: "
    available_length = max_length - len(prefix)

    if len(clean_desc) > available_length:
        clean_desc = clean_desc[:available_length - 3] + '...'

    return f"{prefix}{clean_desc}"
