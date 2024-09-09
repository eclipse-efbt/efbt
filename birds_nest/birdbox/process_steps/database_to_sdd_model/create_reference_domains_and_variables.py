# coding=UTF-8#
# Copyright (c) 2020 Bird Software Solutions Ltd
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

from birdbox.sdd_models import *
from django.apps import apps


class CreateRefDomainsAndVariables(object):
    """
    A class for creating reference domains, variables, and cubes in the SDD model.
    """

    def create_ref_domains_and_variables_and_cubes(sdd_context, context):
        """
        Create reference domains, variables, and cubes in the SDD model.

        Args:
            sdd_context: The SDD context object containing dictionaries for
                         storing created elements.
            context: The context object containing configuration settings.
        """
        CreateRefDomainsAndVariables._create_maintenance_agency(sdd_context)
        CreateRefDomainsAndVariables._create_string_domain(sdd_context)
        CreateRefDomainsAndVariables._process_models(sdd_context, context)

    def _create_maintenance_agency( sdd_context):
        """
        Create a maintenance agency named 'REF' and add it to the SDD context.

        Args:
            sdd_context: The SDD context object to store the created
                         maintenance agency.
        """
        maintenance_agency = MAINTENANCE_AGENCY(name="REF")
        maintenance_agency.code = "REF"
        maintenance_agency.description = "REFERENCE"
        maintenance_agency.maintenance_agency_id = "REF"
        maintenance_agency.save()
        sdd_context.agency_dictionary["REF"] = maintenance_agency

    def _create_string_domain(sdd_context):
        """
        Create a 'String' domain and add it to the SDD context.

        Args:
            sdd_context: The SDD context object to store the created string
                         domain.
        """
        sdd_context.ref_domain_dictionary['String'] = DOMAIN()
        sdd_context.ref_domain_dictionary['String'].domain_id = 'String'
        sdd_context.ref_domain_dictionary['String'].name = 'String'
        sdd_context.ref_domain_dictionary['String'].description = 'String'
        sdd_context.ref_domain_dictionary['String'].type = 'String'
        sdd_context.ref_domain_dictionary['String'].save()

    def _process_models(sdd_context, context):
        """
        Process all models in the 'birdbox' app, creating cubes, structures,
        and processing fields.

        Args:
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        for model in apps.get_models():
            if model._meta.app_label == 'birdbox':
                print(f"{model._meta.app_label}  -> {model.__name__}")
                CreateRefDomainsAndVariables._create_cube_and_structure(model, sdd_context, context)
                CreateRefDomainsAndVariables._process_fields(model, sdd_context, context)

    def _create_cube_and_structure(model, sdd_context, context):
        """
        Create a cube and cube structure for the given model and add them to
        the SDD context.

        Args:
            model: The Django model to create a cube and structure for.
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        bird_cube = CUBE()
        bird_cube_cube_structure = CUBE_STRUCTURE()
        bird_cube.cube_id = model.__name__
        bird_cube.name = model.__name__
        bird_cube_cube_structure.cube_structure_id = model.__name__
        bird_cube_cube_structure.name = model.__name__
        bird_cube.cube_structure_id = bird_cube_cube_structure

        sdd_context.bird_cube_structure_dictionary[
            bird_cube_cube_structure.name] = bird_cube_cube_structure
        sdd_context.bird_cube_dictionary[bird_cube.name] = bird_cube
        if context.save_derived_sdd_items:
            bird_cube_cube_structure.save()
            bird_cube.save()

    def _process_fields(model, sdd_context, context):
        """
        Process all fields of the given model.

        Args:
            model: The Django model whose fields are to be processed.
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        for field in model._meta.get_fields():
            CreateRefDomainsAndVariables._process_field(field, sdd_context, context)

    def _process_field(field, sdd_context, context):
        """
        Process a single field, creating domain, variable, and cube structure
        item as needed.

        Args:
            field: The Django model field to process.
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        variable_id = field.name
        domain = CreateRefDomainsAndVariables._create_domain_if_needed(field, sdd_context)
        CreateRefDomainsAndVariables._create_variable_if_needed(field, variable_id, domain, sdd_context)
        CreateRefDomainsAndVariables._create_cube_structure_item(field, variable_id, sdd_context, context)

    def _create_domain_if_needed(field, sdd_context):
        """
        Create a domain for the field if it doesn't exist and add it to the
        SDD context.

        Args:
            field: The Django model field to create a domain for.
            sdd_context: The SDD context object to store created elements.

        Returns:
            The created or existing domain, or None if no domain is needed.
        """
        domain = None
        try:
            domain_id = field.db_comment
            if domain_id and domain_id not in sdd_context.ref_domain_dictionary:
                domain = DOMAIN()
                domain.domain_id = domain_id
                domain.name = domain_id
                sdd_context.ref_domain_dictionary[domain_id] = domain
                if sdd_context.save_sdd_to_db:
                    domain.save()
                print(f"Adding domain: {domain_id}")
                
                choices = field.choices
                if choices:
                    for choice in choices:
                        member = MEMBER()
                        member.member_id = f"{domain_id}_{choice[0]}"
                        member.name = choice[1]
                        member.domain_id = domain
                        sdd_context.ref_member_dictionary[member.member_id] = member
                        if sdd_context.save_sdd_to_db:
                            member.save()
                        print(f"Adding member {member.member_id}: {member.name}")
        except AttributeError:
            pass
        return domain

    def _create_variable_if_needed(field, variable_id, domain, sdd_context):
        """
        Create a variable for the field if it doesn't exist and add it to the
        SDD context.

        Args:
            field: The Django model field to create a variable for.
            variable_id: The ID for the variable.
            domain: The domain associated with the variable.
            sdd_context: The SDD context object to store created elements.
        """
        if variable_id not in sdd_context.ref_variable_dictionary:
            variable = VARIABLE()
            variable.maintenance_agency_id = sdd_context.agency_dictionary["REF"]
            variable.variable_id = variable_id
            try:
                variable.name = field.verbose_name
            except AttributeError:
                pass
            variable.domain_id = domain if domain else sdd_context.ref_domain_dictionary['String']
            sdd_context.ref_variable_dictionary[variable_id] = variable
            if sdd_context.save_sdd_to_db:
                variable.save()
            print(f"Adding variable {variable_id}")

    def _create_cube_structure_item(field, variable_id, sdd_context, context):
        """
        Create a cube structure item for the field and add it to the SDD context.

        Args:
            field: The Django model field to create a cube structure item for.
            variable_id: The ID of the variable associated with the field.
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        csi = CUBE_STRUCTURE_ITEM()
        csi.cube_structure_id = sdd_context.bird_cube_structure_dictionary[
            field.model.__name__]
        csi.variable_id = sdd_context.ref_variable_dictionary[variable_id]
        key = f"{csi.cube_structure_id.cube_structure_id}:{csi.variable_id.variable_id}"
        sdd_context.bird_cube_structure_item_dictionary[key] = csi
        if context.save_derived_sdd_items:
            csi.save()

