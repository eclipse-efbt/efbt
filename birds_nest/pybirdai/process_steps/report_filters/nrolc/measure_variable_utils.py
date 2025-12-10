# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#

import os
import csv
from pybirdai.models.bird_meta_data_model import VARIABLE, MEMBER, DOMAIN, SUBDOMAIN

class MeasureVariableConverter:
    """
    Utility class for converting meta-variables with measures (members)
    to actual measure-specific variables based on configuration.
    """

    def __init__(self):
        self.measure_domain_config = None
        self.measure_variables_cache = {}

    def load_measure_domain_config(self):
        """
        Load configuration from resources/dpm_metrics_configuration/configuration_dpm_measure_domain.csv.
        Returns: dict {variable_code: {'domain_id': ..., 'subdomain_id': ..., 'variable_id': ..., 'variable_name': ...}}
        """
        if self.measure_domain_config is not None:
            return self.measure_domain_config

        self.measure_domain_config = {}
        config_path = os.path.join("resources", "dpm_metrics_configuration", "configuration_dpm_measure_domain.csv")

        # Check if config file exists
        if not os.path.exists(config_path):
            return self.measure_domain_config

        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    domain_id = row.get('domain_id', '').strip()
                    subdomain_id = row.get('subdomain_id', '').strip()
                    variable_id = row.get('variable_id', '').strip()
                    variable_code = row.get('variable_code', '').strip()
                    variable_name = row.get('variable_name', '').strip()

                    if domain_id and subdomain_id and variable_id and variable_code:
                        self.measure_domain_config[variable_code] = {
                            'domain_id': domain_id,
                            'subdomain_id': subdomain_id,
                            'variable_id': variable_id,
                            'variable_name': variable_name
                        }
        except Exception as e:
            print(f"Warning: Could not load measure domain configuration: {e}")

        return self.measure_domain_config

    def extract_member_code(self, member_id):
        """
        Extract member code from member_id.
        E.g., "EBA_AT_EBA_mi7" → "mi7"
        Assumes pattern: DOMAIN_EBA_CODE where CODE is what we want.
        """
        if not member_id:
            return None

        # Split by underscore and take the last part
        parts = member_id.split('_')
        if len(parts) >= 3:
            # Pattern is typically: DOMAIN_EBA_CODE (e.g., EBA_AT_EBA_mi7)
            # We want the last part after the last occurrence of "_EBA_"
            if '_EBA_' in member_id:
                # Find the last "_EBA_" and take everything after it
                last_eba_idx = member_id.rfind('_EBA_')
                if last_eba_idx != -1:
                    return member_id[last_eba_idx + 5:]  # 5 = len('_EBA_')

        # Fallback: return last part
        return parts[-1] if parts else member_id

    def get_or_create_measure_variable(self, member):
        """
        Create/fetch measure variable from member using configuration.
        1. Extract member code from member_id
        2. Look up member code in configuration
        3. Use configured variable_id, domain, and metadata
        4. Create/fetch VARIABLE from database
        5. Return the variable instance

        Args:
            member: MEMBER instance

        Returns:
            VARIABLE instance or None if not applicable
        """
        if not member or not member.member_id:
            return None

        # Load configuration if not already loaded
        config = self.load_measure_domain_config()
        if not config:
            return None

        # Extract member code
        member_code = self.extract_member_code(member.member_id)
        if not member_code:
            return None

        # Check if this member code is in configuration
        if member_code not in config:
            return None

        # Get configuration for this member
        config_entry = config[member_code]
        variable_id = config_entry['variable_id']
        domain_id = config_entry['domain_id']
        subdomain_id = config_entry['subdomain_id']
        variable_name = config_entry['variable_name']

        # Check cache first
        if variable_id in self.measure_variables_cache:
            return self.measure_variables_cache[variable_id]

        # Try to get existing variable from database
        try:
            variable = VARIABLE.objects.get(variable_id=variable_id)
            self.measure_variables_cache[variable_id] = variable
            return variable
        except VARIABLE.DoesNotExist:
            pass

        # Create new variable
        variable = VARIABLE()
        variable.variable_id = variable_id
        variable.code = member_code
        variable.name = variable_name or member.name or f"Measure field {member_code}"
        variable.description = f"Generated measure variable from member {member.member_id}"

        # Set domain/subdomain
        try:
            subdomain = SUBDOMAIN.objects.get(subdomain_id=subdomain_id)
            # Get domain from subdomain
            if subdomain.domain_id:
                variable.domain_id = subdomain.domain_id
        except SUBDOMAIN.DoesNotExist:
            # Try to get domain directly
            try:
                domain = DOMAIN.objects.get(domain_id=domain_id)
                variable.domain_id = domain
            except DOMAIN.DoesNotExist:
                pass

        # Set maintenance agency if available
        if member.maintenance_agency_id:
            variable.maintenance_agency_id = member.maintenance_agency_id

        # Save the variable
        variable.save()

        # Cache it
        self.measure_variables_cache[variable_id] = variable

        return variable

    def should_convert_to_measure_variable(self, member):
        """
        Check if a member should be converted to a measure variable
        based on the configuration.

        Args:
            member: MEMBER instance

        Returns:
            bool: True if conversion should happen, False otherwise
        """
        if not member or not member.member_id:
            return False

        config = self.load_measure_domain_config()
        if not config:
            return False

        # Extract member code
        member_code = self.extract_member_code(member.member_id)
        if not member_code:
            return False

        # Check if this member code is in configuration
        return member_code in config
