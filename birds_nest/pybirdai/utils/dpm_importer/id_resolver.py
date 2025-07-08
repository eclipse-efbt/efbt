"""
EBA ID Resolver Module

This module provides functions to resolve numeric IDs from the DPM database into
meaningful EBA-prefixed identifiers following EBA naming conventions.

Author: Claude Code
Date: 2025-07-04
"""

import re
import logging
from functools import lru_cache
import csv

logger = logging.getLogger(__name__)

class EBAIDResolver:
    """
    Resolves numeric DPM database IDs into meaningful EBA-prefixed identifiers.
    """

    def __init__(self, path:str = "target"):
        self.path = path
        """Initialize the resolver with lookup dictionaries."""
        self.framework_lookup = {}  # framework_id -> framework_code
        self.domain_lookup = {}     # domain_id -> domain_code
        self.domain_code_to_eba = {}  # domain_code -> eba_prefix
        self.member_domain_lookup = {}  # member_id -> domain_code
        self.template_group_lookup = {}  # template_id -> template_group_id
        self.template_group_framework_lookup = {}  # template_group_id -> framework_id
        self.table_template_lookup = {}  # table_id -> template_id
        self.table_version_lookup = {}  # table_id -> version_info (direct lookup)
        self.taxonomy_lookup = {}  # taxonomy_id -> dmp_package_code (version)
        self.template_taxonomy_lookup = {}  # template_id -> taxonomy_id
        self.resolution_stats = {
            'templates_resolved': 0,
            'tables_resolved': 0,
            'variables_resolved': 0,
            'members_resolved': 0,
            'domains_resolved': 0,
            'failed_resolutions': 0
        }
        # Memoization caches for performance
        self._code_clean_cache = {}
        self._framework_determination_cache = {}

    def load_reference_data_v2(self):
        self.load_frameworks()
        self.load_domains()
        self.load_members()
        self.load_variables()
        self.load_templates()
        self.load_tables_and_versions()
        self.load_axes()

    def define_order_of_import(self):
        self._order_of_import = [
            self.load_frameworks,
            self.load_domains,
            self.load_members,
            self.load_variables,
            self.load_templates,
            self.load_tables_and_versions,
            self.load_axes
        ]

    def load_frameworks(self, framework_data):
        """
        Load framework data for ID resolution.

        Args:
            framework_data (list): List of framework records from ReportingFramework.csv
        """
        with open(self.path + '/ReportingFramework.csv', 'r') as file:
            reader = csv.DictReader(file)
            self._frameworks = {row['id']: row for row in reader}

    def load_reference_data(self, framework_data, domain_data, member_data=None, template_group_data=None, template_group_template_data=None, taxonomy_data=None, taxonomy_table_version_data=None, table_data=None, table_version_data=None):
        """
        Load reference data for ID resolution.

        Args:
            framework_data (list): List of framework records from ReportingFramework.csv
            domain_data (list): List of domain records from Domain.csv
            member_data (list): Optional list of member records from Member.csv
            template_group_data (list): Optional list of template group records from TemplateGroup.csv
            template_group_template_data (list): Optional list of template-group mappings from TemplateGroupTemplate.csv
            taxonomy_data (list): Optional list of taxonomy records from Taxonomy.csv
            taxonomy_table_version_data (list): Optional list of taxonomy-template mappings from TaxonomyTableVersion.csv
            table_data (list): Optional list of table records from Table.csv
            table_version_data (list): Optional list of table version records from TableVersion.csv
        """
        # Load framework lookup
        for row in framework_data:
            framework_id = row.get('FrameworkID', '')
            framework_code = row.get('FrameworkCode', '')
            if framework_id and framework_code:
                self.framework_lookup[framework_id] = framework_code

        logger.info(f"Loaded {len(self.framework_lookup)} framework mappings")

        # Load domain lookup
        for row in domain_data:
            domain_id = row.get('DomainID', '')
            domain_code = row.get('DomainCode', '')
            domain_xbrl = row.get('DomainXbrlCode', '')
            if domain_id and domain_code:
                self.domain_lookup[domain_id] = domain_code
                if domain_xbrl:
                    self.domain_code_to_eba[domain_code] = domain_xbrl

        logger.info(f"Loaded {len(self.domain_lookup)} domain mappings")

        # Load member-to-domain mapping if provided
        if member_data:
            for row in member_data:
                member_id = row.get('MemberID', '')
                domain_id = row.get('DomainID', '')
                if member_id and domain_id and domain_id in self.domain_lookup:
                    self.member_domain_lookup[member_id] = self.domain_lookup[domain_id]

            logger.info(f"Loaded {len(self.member_domain_lookup)} member-domain mappings")

        # Load template group to framework mapping if provided
        if template_group_data:
            for row in template_group_data:
                template_group_id = row.get('TemplateGroupID', '')
                framework_id = row.get('FrameworkID', '')
                if template_group_id and framework_id:
                    self.template_group_framework_lookup[template_group_id] = framework_id

            logger.info(f"Loaded {len(self.template_group_framework_lookup)} template group-framework mappings")

        # Load template to template group mapping if provided
        if template_group_template_data:
            for row in template_group_template_data:
                template_id = row.get('TemplateID', '')
                template_group_id = row.get('TemplateGroupID', '')
                if template_id and template_group_id:
                    self.template_group_lookup[template_id] = template_group_id

            logger.info(f"Loaded {len(self.template_group_lookup)} template-group mappings")

        # Load table to template mapping if provided
        if table_data:
            for row in table_data:
                table_id = row.get('TableID', '')
                template_id = row.get('TemplateID', '')
                if table_id and template_id:
                    self.table_template_lookup[table_id] = template_id

            logger.info(f"Loaded {len(self.table_template_lookup)} table-template mappings")

        # Load taxonomy to version mapping if provided
        if taxonomy_data:
            for row in taxonomy_data:
                taxonomy_id = row.get('TaxonomyID', '')
                dpm_package_code = row.get('DpmPackageCode', '')
                if taxonomy_id and dpm_package_code:
                    self.taxonomy_lookup[taxonomy_id] = dpm_package_code

            logger.info(f"Loaded {len(self.taxonomy_lookup)} taxonomy-version mappings")

        # Load template to taxonomy mapping if provided
        if taxonomy_table_version_data:
            for row in taxonomy_table_version_data:
                template_id = row.get('TemplateID', '')
                taxonomy_id = row.get('TaxonomyID', '')
                if template_id and taxonomy_id:
                    # Use the most recent taxonomy if multiple exist for same template
                    if template_id not in self.template_taxonomy_lookup:
                        self.template_taxonomy_lookup[template_id] = taxonomy_id

            logger.info(f"Loaded {len(self.template_taxonomy_lookup)} template-taxonomy mappings")

        # Load table version data for direct table version lookup if provided
        if table_version_data:
            for row in table_version_data:
                table_id = row.get('TableID', '')
                table_vid = row.get('TableVID', '')
                version_code = row.get('TableVersionCode', '')
                version_label = row.get('TableVersionLabel', '')
                if table_id and version_code:
                    # Store version information directly by table ID
                    self.table_version_lookup[table_id] = {
                        'table_vid': table_vid,
                        'version_code': version_code,
                        'version_label': version_label,
                        'from_date': row.get('FromDate', ''),
                        'to_date': row.get('ToDate', ''),
                    }

            logger.info(f"Loaded {len(self.table_version_lookup)} table version mappings")

    def _clean_code_for_eba(self, code):
        """
        Clean a code string to make it suitable for EBA identifier.
        Uses memoization for performance improvement.

        Args:
            code (str): Raw code string

        Returns:
            str: Cleaned code suitable for EBA identifier
        """
        if not code:
            return ""

        # Check cache first
        code_str = str(code)
        if code_str in self._code_clean_cache:
            return self._code_clean_cache[code_str]

        # Remove spaces, dots, and special characters, keep alphanumeric and underscores
        cleaned = re.sub(r'[^A-Za-z0-9_]', '_', code_str)

        # Remove multiple consecutive underscores
        cleaned = re.sub(r'_+', '_', cleaned)

        # Remove leading/trailing underscores
        cleaned = cleaned.strip('_')

        # Cache the result
        self._code_clean_cache[code_str] = cleaned

        return cleaned

    def _get_framework_from_template_id(self, template_id):
        """
        Get framework code from template ID using definitive lookup.

        Args:
            template_id (str): Original template ID

        Returns:
            str: Framework code or None if not found
        """
        if not template_id:
            return None

        # Look up template -> template group -> framework
        template_group_id = self.template_group_lookup.get(template_id)
        if template_group_id:
            framework_id = self.template_group_framework_lookup.get(template_group_id)
            if framework_id:
                framework_code = self.framework_lookup.get(framework_id)
                return framework_code

        return None

    def _get_version_from_template_id(self, template_id):
        """
        Get DPM package version from template ID using definitive lookup.

        Args:
            template_id (str): Original template ID

        Returns:
            str: DPM package code (version) or None if not found
        """
        if not template_id:
            return None

        # Look up template -> taxonomy -> version
        taxonomy_id = self.template_taxonomy_lookup.get(template_id)
        if taxonomy_id:
            version = self.taxonomy_lookup.get(taxonomy_id)
            return version

        return None

    def _get_version_from_table_id(self, table_id):
        """
        Get version information directly from table ID using TableVersion data.

        Args:
            table_id (str): Original table ID

        Returns:
            str: Version code or None if not found
        """
        if not table_id:
            return None

        # Look up table -> version directly from TableVersion data
        version_info = self.table_version_lookup.get(table_id)
        if version_info:
            return version_info.get('version_code')

        return None

    @lru_cache(maxsize=5000)
    def resolve_template_id(self, template_id, template_code, concept_id=None):
        """
        Resolve template ID to EBA format: EBA_<framework>_EBA_<template_code>_<framework>_<version>

        Args:
            template_id (str): Original template ID
            template_code (str): Template code (e.g., "F 08.01")
            concept_id (str): Optional concept ID for framework lookup

        Returns:
            dict: Resolved template information
        """
        try:
            # Primary: Use relationship chain lookup Template → TemplateGroup → Framework
            framework = None
            try:
                framework = self._resolve_template_framework_via_relationship(template_id)
                # logger.debug(f"Successfully resolved framework via relationship chain for template {template_id}: {framework}")
            except ValueError as e:
                logger.warning(f"Relationship chain lookup failed for template {template_id}: {str(e)}")

            # Secondary: Try definitive lookup using template ID (existing method)
            if not framework:
                framework = self._get_framework_from_template_id(template_id)
                if framework:
                    # logger.debug(f"Resolved framework via template ID lookup for template {template_id}: {framework}")
                    pass

            # Tertiary: Fall back to pattern-based detection
            if not framework:
                framework = self._determine_framework_from_code(template_code)
                if framework:
                    # logger.debug(f"Resolved framework via pattern matching for template {template_id}: {framework}")
                    pass

            # Failure: Throw error instead of defaulting to UNKNOWN
            if not framework:
                self.resolution_stats['failed_resolutions'] += 1
                error_msg = f"Could not determine framework for template {template_id} with code '{template_code}' using any resolution method"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Get version from template ID
            version = self._get_version_from_template_id(template_id)
            if not version:
                error_msg = f"Could not determine version for template {template_id}"
                logger.error(error_msg)
                raise ValueError(error_msg)

            cleaned_code = self._clean_code_for_eba(template_code)
            eba_id = f"EBA_{framework}_EBA_{cleaned_code}_{framework}_{version}"

            self.resolution_stats['templates_resolved'] += 1

            return {
                'original_id': template_id,
                'eba_id': eba_id,
                'framework': framework,
                'code': template_code,
                'version': version,
                'resolution_success': True  # If we reach here, resolution was successful
            }

        except Exception as e:
            logger.error(f"Framework resolution failed for template ID {template_id}: {str(e)}")
            self.resolution_stats['failed_resolutions'] += 1
            # Return error structure instead of fallback to maintain data integrity
            return {
                'original_id': template_id,
                'eba_id': None,  # No fallback EBA ID
                'framework': None,  # No fallback framework
                'code': template_code,
                'version': None,  # No fallback version
                'resolution_success': False,
                'error': str(e)
            }

    @lru_cache(maxsize=8000)
    def resolve_table_id(self, table_id, template_id, original_code, template_code=None):
        """
        Resolve table ID to EBA format: EBA_<framework>_EBA_<table_code>_<framework>_<version>_T<table_id>

        Args:
            table_id (str): Original table ID
            template_id (str): Template ID this table belongs to
            original_code (str): Original table code
            template_code (str): Optional template code for framework determination

        Returns:
            dict: Resolved table information
        """
        try:
            # Primary: Use relationship chain lookup Table → Template → TemplateGroup → Framework
            framework = None
            try:
                framework = self._resolve_table_framework_via_relationship(table_id)
                # logger.debug(f"Successfully resolved framework via relationship chain for table {table_id}: {framework}")
            except ValueError as e:
                logger.warning(f"Relationship chain lookup failed for table {table_id}: {str(e)}")

            # Secondary: Try to get framework from template ID (definitive)
            if not framework:
                framework = self._get_framework_from_template_id(template_id)
                if framework:
                    # logger.debug(f"Resolved framework via template ID lookup for table {table_id}: {framework}")
                    pass

            # Tertiary: Fall back to code-based framework detection
            if not framework:
                framework = self._determine_framework_from_code(original_code or template_code or "")
                if framework:
                    # logger.debug(f"Resolved framework via pattern matching for table {table_id}: {framework}")
                    pass

            # Failure: Throw error instead of defaulting to UNKNOWN
            if not framework:
                self.resolution_stats['failed_resolutions'] += 1
                error_msg = f"Could not determine framework for table {table_id} with code '{original_code}' using any resolution method"
                logger.error(error_msg)
                raise ValueError(error_msg)

            # Primary: Get version directly from table ID using TableVersion data
            version = self._get_version_from_table_id(table_id)
            if version:
                # logger.debug(f"Resolved version via direct table lookup for table {table_id}: {version}")
                pass

            # Secondary: Fall back to getting version from template ID if direct lookup fails
            if not version:
                version = self._get_version_from_template_id(template_id)
                if version:
                    # logger.debug(f"Resolved version via template ID lookup for table {table_id}: {version}")
                    pass

            # Failure: Throw error if both methods fail
            if not version:
                error_msg = f"Could not determine version for table {table_id} via direct table lookup or template {template_id}. PLease fix this by looking at the tableVersion table instead of going through the template. The Framework is to be found via the template but not the version"
                logger.error(error_msg)
                raise ValueError(error_msg)

            cleaned_code = self._clean_code_for_eba(original_code)
            eba_id = f"EBA_{framework}_EBA_{cleaned_code}_{framework}_{version}_T{table_id}"

            self.resolution_stats['tables_resolved'] += 1

            return {
                'original_id': table_id,
                'eba_id': eba_id,
                'template_id': template_id,
                'framework': framework,
                'original_code': original_code,
                'version': version,
                'resolution_success': True  # If we reach here, resolution was successful
            }

        except Exception as e:
            logger.error(f"Framework resolution failed for table ID {table_id}: {str(e)}")
            self.resolution_stats['failed_resolutions'] += 1
            # Return error structure instead of fallback to maintain data integrity
            return {
                'original_id': table_id,
                'eba_id': None,  # No fallback EBA ID
                'template_id': template_id,
                'framework': None,  # No fallback framework
                'original_code': original_code,
                'version': None,  # No fallback version
                'resolution_success': False,
                'error': str(e)
            }

    @lru_cache(maxsize=10000)
    def resolve_variable_id(self, concept_id, concept_code=None, data_type=None):
        """
        Resolve concept/variable ID to EBA format: EBA_<concept_code>

        Args:
            concept_id (str): Original concept ID
            concept_code (str): Concept code if available
            data_type (str): Data type if available

        Returns:
            dict: Resolved variable information
        """
        try:
            if concept_code:
                cleaned_code = self._clean_code_for_eba(concept_code)
                eba_id = f"EBA_{cleaned_code}"
            else:
                eba_id = f"EBA_CONCEPT_{concept_id}"

            self.resolution_stats['variables_resolved'] += 1

            return {
                'original_id': concept_id,
                'eba_id': eba_id,
                'code': concept_code,
                'data_type': data_type,
                'resolution_success': bool(concept_code)
            }

        except Exception as e:
            logger.error(f"Error resolving variable ID {concept_id}: {str(e)}")
            self.resolution_stats['failed_resolutions'] += 1
            return {
                'original_id': concept_id,
                'eba_id': f"EBA_VARIABLE_{concept_id}",
                'code': concept_code,
                'resolution_success': False,
                'error': str(e)
            }

    @lru_cache(maxsize=15000)
    def resolve_member_id(self, member_id, domain_id, member_code, member_xbrl_code=None):
        """
        Resolve member ID to EBA format: EBA_<domain_code>_<member_code>

        Args:
            member_id (str): Original member ID
            domain_id (str): Domain ID the member belongs to
            member_code (str): Member code
            member_xbrl_code (str): Optional XBRL code (e.g., "eba_AP:x1")

        Returns:
            dict: Resolved member information
        """
        try:
            # Get domain code
            domain_code = self.domain_lookup.get(domain_id, f"DOMAIN_{domain_id}")

            if member_xbrl_code and ":" in member_xbrl_code:
                # Extract from XBRL code (e.g., "eba_AP:x1" -> "AP", "x1")
                eba_part, code_part = member_xbrl_code.split(":", 1)
                domain_from_eba = eba_part.replace("eba_", "")
                eba_id = f"EBA_{domain_from_eba}_{code_part}"
            elif domain_code and member_code:
                cleaned_member_code = self._clean_code_for_eba(member_code)
                eba_id = f"EBA_{domain_code}_{cleaned_member_code}"
            else:
                eba_id = f"EBA_MEMBER_{member_id}"

            self.resolution_stats['members_resolved'] += 1

            return {
                'original_id': member_id,
                'eba_id': eba_id,
                'domain_id': domain_id,
                'domain_code': domain_code,
                'member_code': member_code,
                'full_eba_code': member_xbrl_code,
                'resolution_success': bool(domain_code and member_code)
            }

        except Exception as e:
            logger.error(f"Error resolving member ID {member_id}: {str(e)}")
            self.resolution_stats['failed_resolutions'] += 1
            return {
                'original_id': member_id,
                'eba_id': f"EBA_MEMBER_{member_id}",
                'domain_id': domain_id,
                'member_code': member_code,
                'resolution_success': False,
                'error': str(e)
            }

    @lru_cache(maxsize=3000)
    def resolve_domain_id(self, domain_id, domain_code):
        """
        Resolve domain ID to EBA format: EBA_<domain_code>

        Args:
            domain_id (str): Original domain ID
            domain_code (str): Domain code

        Returns:
            dict: Resolved domain information
        """
        try:
            eba_id = f"EBA_{domain_code}"

            self.resolution_stats['domains_resolved'] += 1

            return {
                'original_id': domain_id,
                'eba_id': eba_id,
                'domain_code': domain_code,
                'eba_xbrl_code': self.domain_code_to_eba.get(domain_code),
                'resolution_success': True
            }

        except Exception as e:
            logger.error(f"Error resolving domain ID {domain_id}: {str(e)}")
            self.resolution_stats['failed_resolutions'] += 1
            return {
                'original_id': domain_id,
                'eba_id': f"EBA_DOMAIN_{domain_id}",
                'domain_code': domain_code,
                'resolution_success': False,
                'error': str(e)
            }

    @lru_cache(maxsize=2000)
    def _determine_framework_from_code(self, code):
        """
        Determine framework from template/table code pattern.
        Enhanced to support all EBA frameworks with memoization.

        Args:
            code (str): Template or table code

        Returns:
            str: Framework name or None
        """
        if not code:
            return None

        # Check cache first
        code_str = str(code)
        if code_str in self._framework_determination_cache:
            return self._framework_determination_cache[code_str]

        code_upper = code_str.upper().strip()
        result = None

        # Framework determination patterns - enhanced for all EBA frameworks
        if code_upper.startswith('F '):
            result = 'FINREP'
        elif code_upper.startswith('C '):
            result = 'COREP'
        elif code_upper.startswith('A ') or code_upper.startswith('AE '):
            result = 'AE'
        elif code_upper.startswith('P '):
            result = 'FP'
        elif code_upper.startswith('S '):
            result = 'SBP'
        elif code_upper.startswith('Z '):
            result = 'RES'
        elif code_upper.startswith('FP '):
            result = 'FP'
        elif code_upper.startswith('SBP '):
            result = 'SBP'
        elif code_upper.startswith('REM '):
            result = 'REM'
        elif code_upper.startswith('RES '):
            result = 'RES'
        elif code_upper.startswith('PAY '):
            result = 'PAY'
        elif code_upper.startswith('IF '):
            result = 'IF'
        elif code_upper.startswith('GSII '):
            result = 'GSII'
        elif code_upper.startswith('MREL '):
            result = 'MREL'
        elif code_upper.startswith('ESG '):
            result = 'ESG'
        elif code_upper.startswith('IPU '):
            result = 'IPU'
        elif code_upper.startswith('PILLAR3 '):
            result = 'PILLAR3'
        elif code_upper.startswith('IRRBB '):
            result = 'IRRBB'
        elif code_upper.startswith('DORA '):
            result = 'DORA'
        elif code_upper.startswith('FC '):
            result = 'FC'
        elif code_upper.startswith('MICA '):
            result = 'MICA'
        elif 'FINREP' in code_upper:
            result = 'FINREP'
        elif 'COREP' in code_upper:
            result = 'COREP'
        elif 'ASSET_ENCUMBRANCE' in code_upper or 'AE' in code_upper:
            result = 'AE'

        # Cache the result
        self._framework_determination_cache[code_str] = result

        return result

    def _resolve_template_framework_via_relationship(self, template_id):
        """
        Resolve template framework using relationship chain: Template → TemplateGroup → Framework

        Args:
            template_id (str): Template ID

        Returns:
            str: Framework code or None if not found

        Raises:
            ValueError: If framework cannot be determined through relationship chain
        """
        if not template_id:
            raise ValueError("Template ID is required for framework resolution")

        # Step 1: Template → TemplateGroup
        template_group_id = self.template_group_lookup.get(str(template_id))
        if not template_group_id:
            raise ValueError(f"No template group found for template {template_id}")

        # Step 2: TemplateGroup → Framework
        framework_id = self.template_group_framework_lookup.get(str(template_group_id))
        if not framework_id:
            raise ValueError(f"No framework found for template group {template_group_id} (template {template_id})")

        # Step 3: Framework ID → Framework Code
        framework_code = self.framework_lookup.get(str(framework_id))
        if not framework_code:
            raise ValueError(f"No framework code found for framework ID {framework_id} (template {template_id})")

        # logger.debug(f"Resolved template {template_id} → group {template_group_id} → framework {framework_id} ({framework_code})")
        return framework_code

    def _resolve_table_framework_via_relationship(self, table_id):
        """
        Resolve table framework using relationship chain: Table → Template → TemplateGroup → Framework

        Args:
            table_id (str): Table ID

        Returns:
            str: Framework code or None if not found

        Raises:
            ValueError: If framework cannot be determined through relationship chain
        """
        if not table_id:
            raise ValueError("Table ID is required for framework resolution")

        # Step 1: Table → Template
        template_id = self.table_template_lookup.get(str(table_id))
        if not template_id:
            raise ValueError(f"No template found for table {table_id}")

        # Step 2: Template → Framework (reuse existing method)
        try:
            framework_code = self._resolve_template_framework_via_relationship(template_id)
            # logger.debug(f"Resolved table {table_id} → template {template_id} → framework {framework_code}")
            return framework_code
        except ValueError as e:
            raise ValueError(f"Failed to resolve framework for table {table_id} via template {template_id}: {str(e)}")

    def resolve_axis_id(self, axis_id, table_code, framework=None, version=None, orientation=None):
        """
        Resolve axis ID to EBA format: EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}

        Args:
            axis_id (str): Original axis ID
            table_code (str): Table code (e.g., "F_08.01")
            framework (str): Framework name
            version (str): Framework version
            orientation (str): Axis orientation (X, Y, Z)

        Returns:
            dict: Resolved axis information
        """
        try:
            # Determine framework if not provided
            if not framework:
                framework = self._determine_framework_from_code(table_code)
                if not framework:
                    raise ValueError(f"Could not determine framework for axis {axis_id} with table code '{table_code}'")

            # Clean table code
            cleaned_table_code = self._clean_code_for_eba(table_code)

            # Determine axis number from orientation
            axis_number = '1'  # Default
            if orientation:
                orientation_upper = str(orientation).upper()
                if orientation_upper == 'X':
                    axis_number = '1'
                elif orientation_upper == 'Y':
                    axis_number = '2'
                elif orientation_upper == 'Z':
                    axis_number = '3'

            # Build EBA axis ID
            if version:
                eba_id = f"EBA_{framework}_EBA_{cleaned_table_code}_{framework}_{version}_{axis_number}"
            else:
                eba_id = f"EBA_{framework}_EBA_{cleaned_table_code}_{framework}_{axis_number}"

            return {
                'original_id': axis_id,
                'eba_id': eba_id,
                'framework': framework,
                'table_code': table_code,
                'version': version,
                'axis_number': axis_number,
                'orientation': orientation,
                'resolution_success': True
            }

        except Exception as e:
            logger.error(f"Error resolving axis ID {axis_id}: {str(e)}")
            return {
                'original_id': axis_id,
                'eba_id': None,
                'framework': None,
                'resolution_success': False,
                'error': str(e)
            }

    def resolve_axis_ordinate_id(self, ordinate_id, axis_info, ordinate_code=None):
        """
        Resolve axis ordinate ID to EBA format: EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}_{ordinate_code}

        Args:
            ordinate_id (str): Original ordinate ID
            axis_info (dict): Resolved axis information from resolve_axis_id
            ordinate_code (str): Ordinate code (can be empty for open axes)

        Returns:
            dict: Resolved axis ordinate information
        """
        try:
            # Build on axis information
            framework = axis_info.get('framework')
            if not framework:
                raise ValueError(f"No framework information in axis_info for ordinate {ordinate_id}")
            table_code = axis_info.get('table_code', '')
            version = axis_info.get('version', '')
            axis_number = axis_info.get('axis_number', '1')

            # Clean ordinate code
            ordinate_suffix = f"_{ordinate_code}" if ordinate_code and ordinate_code.strip() else "_"

            # Build EBA axis ordinate ID
            if version:
                eba_id = f"EBA_{framework}_EBA_{self._clean_code_for_eba(table_code)}_{framework}_{version}_{axis_number}{ordinate_suffix}"
            else:
                eba_id = f"EBA_{framework}_EBA_{self._clean_code_for_eba(table_code)}_{framework}_{axis_number}{ordinate_suffix}"

            return {
                'original_id': ordinate_id,
                'eba_id': eba_id,
                'axis_id': axis_info.get('eba_id'),
                'framework': framework,
                'table_code': table_code,
                'ordinate_code': ordinate_code,
                'resolution_success': True
            }

        except Exception as e:
            logger.error(f"Error resolving ordinate ID {ordinate_id}: {str(e)}")
            return {
                'original_id': ordinate_id,
                'eba_id': f"EBA_ORDINATE_{ordinate_id}",
                'resolution_success': False,
                'error': str(e)
            }

    def resolve_cell_id(self, cell_id, context_data=None, framework=None, version=None, table_code=None):
        """
        Resolve cell ID to EBA format: EBA_<framework>_EBA_<table_code>_<framework>_<version>_C<cell_id>

        Args:
            cell_id (str): Original cell ID
            context_data (dict): Optional context data for fallback generation
            framework (str): Framework for the parent table
            version (str): Version for the parent table
            table_code (str): Table code for the parent table

        Returns:
            dict: Resolved cell information
        """
        try:
            # If we have framework, version, and table_code, use the new pattern
            if framework and version and table_code and cell_id:
                cleaned_table_code = self._clean_code_for_eba(table_code)
                eba_id = f"EBA_{framework}_EBA_{cleaned_table_code}_{framework}_{version}_C{cell_id}"
                resolution_success = True
            elif cell_id and str(cell_id).strip():
                # Fallback to simple cell ID format
                eba_id = f"EBA_C{cell_id}"
                resolution_success = True
            else:
                # Generate cell ID from context
                if context_data:
                    data_point_vid = context_data.get('DataPointVID', '')
                    table_vid = context_data.get('TableVID', '')

                    if data_point_vid:
                        eba_id = f"EBA_{data_point_vid}"
                        resolution_success = True
                    elif table_vid:
                        # Generate from table context
                        import hashlib
                        hash_obj = hashlib.md5(f"CELL_{table_vid}_{cell_id}".encode())
                        numeric_id = int(hash_obj.hexdigest()[:6], 16)
                        eba_id = f"EBA_{numeric_id}"
                        resolution_success = True
                    else:
                        raise ValueError(f"No context data available for cell {cell_id}")
                else:
                    raise ValueError(f"No context data available for cell {cell_id}")

            return {
                'original_id': cell_id,
                'eba_id': eba_id,
                'framework': framework,
                'version': version,
                'table_code': table_code,
                'context_data': context_data,
                'resolution_success': resolution_success
            }

        except Exception as e:
            logger.error(f"Error resolving cell ID {cell_id}: {str(e)}")
            import random
            fallback_id = random.randint(100000, 999999)
            return {
                'original_id': cell_id,
                'eba_id': f"EBA_{fallback_id}",
                'framework': framework,
                'version': version,
                'table_code': table_code,
                'resolution_success': False,
                'error': str(e)
            }

    def resolve_cube_structure_id(self, template_id, template_code, framework=None, version=None):
        """
        Resolve cube structure ID to EBA format for templates->cubes mapping.

        Args:
            template_id (str): Original template ID
            template_code (str): Template code
            framework (str): Framework name
            version (str): Framework version

        Returns:
            dict: Resolved cube structure information
        """
        try:
            # Get framework from template ID if not provided
            if not framework:
                framework = self._get_framework_from_template_id(template_id)

            # Fall back to code-based detection
            if not framework:
                framework = self._determine_framework_from_code(template_code)
                if not framework:
                    raise ValueError(f"Could not determine framework for cube structure template {template_id} with code '{template_code}'")

            # Get version from template ID if not provided
            if not version:
                version = self._get_version_from_template_id(template_id)

            # Clean template code for table code
            cleaned_code = self._clean_code_for_eba(template_code)

            # Build EBA cube structure ID following the new pattern
            if version:
                eba_id = f"EBA_{framework}_EBA_{cleaned_code}_{framework}_{version}"
            else:
                eba_id = f"EBA_{framework}_EBA_{cleaned_code}_{framework}"

            return {
                'original_id': template_id,
                'eba_id': eba_id,
                'framework': framework,
                'template_code': template_code,
                'version': version,
                'resolution_success': True and version != "UNKNOWN"
            }

        except Exception as e:
            logger.error(f"Error resolving cube structure ID for template {template_id}: {str(e)}")
            return {
                'original_id': template_id,
                'eba_id': f"EBA_CUBE_{template_id}",
                'framework': "UNKNOWN",
                'version': "UNKNOWN",
                'resolution_success': False,
                'error': str(e)
            }

    def get_resolution_stats(self):
        """
        Get statistics about ID resolution success/failure rates.

        Returns:
            dict: Resolution statistics
        """
        total_attempted = sum(self.resolution_stats.values())
        success_rate = 0
        if total_attempted > 0:
            successful = total_attempted - self.resolution_stats['failed_resolutions']
            success_rate = (successful / total_attempted) * 100

        return {
            **self.resolution_stats,
            'total_attempted': total_attempted,
            'success_rate': round(success_rate, 2)
        }

    def print_resolution_summary(self):
        """Print a summary of resolution statistics."""
        stats = self.get_resolution_stats()

        print("\nEBA ID Resolution Summary:")
        print("=" * 40)
        print(f"Templates resolved: {stats['templates_resolved']}")
        print(f"Tables resolved: {stats['tables_resolved']}")
        print(f"Variables resolved: {stats['variables_resolved']}")
        print(f"Members resolved: {stats['members_resolved']}")
        print(f"Domains resolved: {stats['domains_resolved']}")
        print(f"Failed resolutions: {stats['failed_resolutions']}")
        print(f"Total attempted: {stats['total_attempted']}")
        print(f"Success rate: {stats['success_rate']}%")
        print("=" * 40)

    def clear_cache(self):
        """Clear memoization caches to free memory."""
        self._code_clean_cache.clear()
        self._framework_determination_cache.clear()
