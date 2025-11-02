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

"""
Subdomain management for NROLC (Non-Reference Output Layer Creator).
Handles subdomain creation, caching, and enumeration generation.
"""

from pybirdai.models.bird_meta_data_model import SUBDOMAIN, SUBDOMAIN_ENUMERATION
from pybirdai.process_steps.report_filters.nrolc import nrolc_utils


class SubdomainManager:
    """Manages subdomain creation and caching for output layers."""

    def __init__(self, subdomains_to_create, subdomain_enumerations_to_create, existing_subdomains_cache):
        """
        Initialize subdomain manager.

        Args:
            subdomains_to_create: List to collect SUBDOMAIN objects for bulk creation
            subdomain_enumerations_to_create: List to collect SUBDOMAIN_ENUMERATION objects
            existing_subdomains_cache: Dict of existing subdomains (pre-fetched)
        """
        self.subdomains_to_create = subdomains_to_create
        self.subdomain_enumerations_to_create = subdomain_enumerations_to_create
        self.existing_subdomains = existing_subdomains_cache

    def get_or_create_subdomain(self, variable, members, cube_structure):
        """
        Get or create a subdomain for the given variable and members.

        Args:
            variable: VARIABLE instance
            members: Set of MEMBER instances
            cube_structure: CUBE_STRUCTURE instance

        Returns:
            SUBDOMAIN instance
        """
        # Create a unique subdomain ID
        subdomain_id = nrolc_utils.generate_subdomain_id(
            variable.variable_id,
            cube_structure.cube_structure_id
        )

        # Check if subdomain already exists in our creation list
        existing_subdomain = next(
            (sd for sd in self.subdomains_to_create if sd.subdomain_id == subdomain_id),
            None
        )

        if existing_subdomain:
            return existing_subdomain

        # Check if subdomain exists in database (using pre-fetched cache - no DB query!)
        if subdomain_id in self.existing_subdomains:
            return self.existing_subdomains[subdomain_id]

        # Create new subdomain
        subdomain = SUBDOMAIN()
        subdomain.subdomain_id = subdomain_id
        subdomain.name = f"Output subdomain for {variable.name or variable.variable_id}"
        subdomain.code = subdomain_id
        subdomain.is_listed = True
        subdomain.description = f"Generated subdomain for output layer"

        # Get the variable's domain if it exists
        if hasattr(variable, 'domain_id') and variable.domain_id:
            subdomain.domain_id = variable.domain_id

        self.subdomains_to_create.append(subdomain)

        # Create subdomain enumeration entries for each member
        order = 0
        for member in members:
            order += 1
            enum_entry = SUBDOMAIN_ENUMERATION()
            enum_entry.subdomain_id = subdomain
            enum_entry.member_id = member
            enum_entry.order = order
            self.subdomain_enumerations_to_create.append(enum_entry)

        return subdomain
