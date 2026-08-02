# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation
#


from pybirdai.model_blueprint import AnnotationDirective, ModelPackage, PrimitiveTypes
import os
from contextlib import contextmanager
from contextvars import ContextVar

_DEBUG_LINEAGE = os.environ.get('PYBIRDAI_DEBUG_LINEAGE', '').lower() in {'1', 'true', 'yes', 'on'}
_lineage_setting_override = ContextVar('pybirdai_lineage_setting_override', default=None)

def _context_debug(message):
    if _DEBUG_LINEAGE:
        print(message)


#: The annotation kinds SQL Developer facts are recorded under while importing.
ANNOTATION_DIRECTIVE_NAMES = (
    'key',
    'dep',
    'entity_hierarchy',
    'relationship_type',
    'long_name',
    'il_mapping',
    'code',
)


@contextmanager
def lineage_tracking_override(enabled):
    """Temporarily override lineage tracking for the current execution context."""
    token = _lineage_setting_override.set(enabled)
    try:
        yield
    finally:
        _lineage_setting_override.reset(token)

class Context:
    '''
    Documentation for Context
    '''
    # variables to configure the behaviour

    # enable_lineage_tracking will be set dynamically from configuration
    enable_lineage_tracking = False
    _lineage_setting_cache = None

    # When False, cube_link derivation rules are not created or shown anywhere
    cube_link_derivations_allowed = False

    check_domain_members_during_join_meta_data_creation = False

    enrich_ldm_relationships = False
    use_codes = True

    reference_data_class_list = []

    # Framework tracking - identifies which framework (FINREP, COREP, ANCRDT) is being processed
    current_framework = None

    # the directory where we get our input files
    file_directory = ""
    # the directory where we save our outputs.
    output_directory = ""

    types = PrimitiveTypes()

    # the model blueprint packages built during stage one of the import
    types_package = ModelPackage(name='types')
    ldm_domains_package = ModelPackage(
        name='ldm_domains',
        ns_uri='http://www.eclipse.org/bird/ldm_domains',
        ns_prefix='ldm_domains')

    ldm_entities_package = ModelPackage(
        name='ldm_entities',
        ns_uri='http://www.eclipse.org/bird/ldm_entities',
        ns_prefix='ldm_entities')

    il_domains_package = ModelPackage(
        name='il_domains',
        ns_uri='http://www.eclipse.org/bird/il_domains',
        ns_prefix='ldm_domains')

    il_tables_package = ModelPackage(
        name='il_entities',
        ns_uri='http://www.eclipse.org/bird/il_entities',
        ns_prefix='il_entities')


    skip_reference_data_in_ldm = True
    reports_dictionary = {}

    classification_types = {}

    enum_literals_map = {}

    # classesMap keeps a reference between ldm ID's for classes and
    # the class instance
    classes_map = {}

    table_map = {}

    fk_to_mandatory_map = {}

    fk_to_column_map = {}

    # A map between the ELDM names for primitive types types, and
    # our standard primitive types such as EString
    datatype_map = {}

    main_category_to_name_map_finrep = {}
    main_category_to_name_map_ae = {}
    main_category_to_name_map_corep = {}

    join_for_products_to_main_category_map_finrep = {}
    join_for_products_to_main_category_map_ae = {}
    join_for_products_to_main_category_map_corep = {}

    tables_for_main_category_map_finrep = {}
    tables_for_main_category_map_ae = {}
    tables_for_main_category_map_corep = {}

    join_for_products_to_linked_tables_map_finrep = {}
    join_for_products_to_linked_tables_map_ae = {}
    join_for_products_to_linked_tables_map_corep = {}

    join_for_products_to_to_filter_map_finrep = {}
    join_for_products_to_to_filter_map_ae = {}
    join_for_products_to_to_filter_map_corep = {}

    table_and_part_tuple_map_finrep = {}
    table_and_part_tuple_map_ae = {}
    table_and_part_tuple_map_corep = {}

    ldm_entity_to_linked_tables_map = {}
    report_to_main_category_map = {}
    enum_map = {}

    arc_to_source_map = {}
    arc_name_to_arc_class_map = {}

    arc_target_to_arc_map = {}

    enums_used = []

    main_categories_in_scope_finrep = []
    main_categories_in_scope_ae = []
    main_categories_in_scope_corep = []

    load_sdd_from_website =False

    save_derived_sdd_items = True

    input_layer_name = "Input Layer 6.7"

    generate_etl = True

    def _get_configured_lineage_tracking(self):
        """Get the configured lineage tracking setting from temporary file."""
        # Use the static method for consistency
        return Context.get_current_lineage_setting()

    def _get_configured_data_model_type(self):
        """Get the configured data model type from temporary file or AutomodeConfiguration."""
        # First try to read from temporary configuration file
        try:
            import os
            import json
            # Use the same path as in views.py
            temp_config_path = os.path.join('.', 'automode_config.json')
            if os.path.exists(temp_config_path):
                with open(temp_config_path, 'r') as f:
                    config_data = json.load(f)
                data_model_type = config_data.get('data_model_type', 'ELDM')
                return 'ldm' if data_model_type == 'ELDM' else 'il'
        except Exception:
            # If temp file fails, try database
            pass

        # Fallback to database configuration
        try:
            # Import here to avoid circular imports
            from pybirdai.models.workflow_model import AutomodeConfiguration

            config = AutomodeConfiguration.get_active_configuration()
            if config:
                return 'ldm' if config.data_model_type == 'ELDM' else 'il'
        except:
            # If no configuration exists or there's an error, default to 'ldm'
            pass
        return 'ldm'

    @property
    def ldm_or_il(self):
        """Get the current data model type (ldm or il)."""
        return self._ldm_or_il

    @ldm_or_il.setter
    def ldm_or_il(self, value):
        """Set the data model type (ldm or il)."""
        self._ldm_or_il = value

    def __init__(self):
        # Initialize ldm_or_il based on AutomodeConfiguration if available
        self._ldm_or_il = self._get_configured_data_model_type()
        # Initialize enable_lineage_tracking from configuration
        self.enable_lineage_tracking = self._get_configured_lineage_tracking()
        # Also update the class attribute so the orchestration factory can see it
        Context.enable_lineage_tracking = self.enable_lineage_tracking

        for directive_name in ANNOTATION_DIRECTIVE_NAMES:
            self.ldm_entities_package.annotation_directives.append(
                AnnotationDirective(name=directive_name, source_uri=directive_name))
            self.il_tables_package.annotation_directives.append(
                AnnotationDirective(name=directive_name, source_uri=directive_name))

        types = PrimitiveTypes()
        self.types_package.add_classifier(types.e_string)
        self.types_package.add_classifier(types.e_double)
        self.types_package.add_classifier(types.e_int)

    def refresh_lineage_setting(self):
        """Refresh the lineage tracking setting from configuration files"""
        Context._lineage_setting_cache = None
        self.enable_lineage_tracking = self._get_configured_lineage_tracking()
        Context.enable_lineage_tracking = self.enable_lineage_tracking
        _context_debug(f"Context: Refreshed lineage setting to: {self.enable_lineage_tracking}")
        return self.enable_lineage_tracking

    @staticmethod
    def get_current_lineage_setting():
        """Static method to get the current lineage setting without creating instance"""
        override = _lineage_setting_override.get()
        if override is not None:
            return override

        try:
            import json

            # Try multiple possible paths for the config file
            possible_paths = [
                os.path.join('.', 'automode_config.json'),  # Current directory
                os.path.join(os.path.dirname(__file__), '..', '..', 'automode_config.json'),  # Relative to this file
            ]

            # Try to get Django BASE_DIR if available
            try:
                from django.conf import settings
                if hasattr(settings, 'BASE_DIR'):
                    possible_paths.append(os.path.join(settings.BASE_DIR, 'automode_config.json'))
            except:
                pass  # Django not configured or available

            current_dir = os.getcwd()
            _context_debug(f"Context: Current working directory: {current_dir}")

            for temp_config_path in possible_paths:
                _context_debug(f"Context: Trying config path: {temp_config_path}")
                if os.path.exists(temp_config_path):
                    mtime = os.path.getmtime(temp_config_path)
                    cached = Context._lineage_setting_cache
                    if cached and cached[0] == temp_config_path and cached[1] == mtime:
                        return cached[2]

                    with open(temp_config_path, 'r') as f:
                        config_data = json.load(f)
                    lineage_setting = config_data.get('enable_lineage_tracking', False)
                    Context._lineage_setting_cache = (temp_config_path, mtime, lineage_setting)
                    _context_debug(f"Context: Found config at {temp_config_path}, lineage_tracking = {lineage_setting}")
                    return lineage_setting

        except Exception as e:
            _context_debug(f"Context: Static read error: {e}")
            pass
        Context._lineage_setting_cache = None
        _context_debug("Context: No config file found, using default: False")
        return False
