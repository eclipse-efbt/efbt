# coding=UTF-8#
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

from pybirdai.sdd_models import *
from django.apps import apps

from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField


class ImportInputModel(object):
    """
    A class for creating reference domains, variables, and cubes in the SDD model.
    """

    def import_input_model(sdd_context, context):
        """
        Create reference domains, variables, and cubes in the SDD model.

        Args:
            sdd_context: The SDD context object containing dictionaries for
                         storing created elements.
            context: The context object containing configuration settings.
        """
        ImportInputModel._create_maintenance_agency(sdd_context)
        ImportInputModel._create_primitive_domains(sdd_context)
        ImportInputModel._process_models(sdd_context, context)

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

        maintenance_agency = MAINTENANCE_AGENCY(name="NODE")
        maintenance_agency.code = "NODE"
        maintenance_agency.description = "Member Hierarchy Node"
        maintenance_agency.maintenance_agency_id = "NODE"
        maintenance_agency.save()

        sdd_context.agency_dictionary["REF"] = maintenance_agency

    def _create_primitive_domains(sdd_context):
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
        sdd_context.ref_domain_dictionary['Integer'] = DOMAIN()
        sdd_context.ref_domain_dictionary['Integer'].domain_id = 'Integer'
        sdd_context.ref_domain_dictionary['Integer'].name = 'Integer'
        sdd_context.ref_domain_dictionary['Integer'].description = 'Integer'
        sdd_context.ref_domain_dictionary['Integer'].type = 'Integer'
        sdd_context.ref_domain_dictionary['Integer'].save()
        sdd_context.ref_domain_dictionary['Date'] = DOMAIN()
        sdd_context.ref_domain_dictionary['Date'].domain_id = 'Date'
        sdd_context.ref_domain_dictionary['Date'].name = 'Date'
        sdd_context.ref_domain_dictionary['Date'].description = 'Date'
        sdd_context.ref_domain_dictionary['Date'].type = 'Date'
        sdd_context.ref_domain_dictionary['Date'].save()
        sdd_context.ref_domain_dictionary['Float'] = DOMAIN()
        sdd_context.ref_domain_dictionary['Float'].domain_id = 'Float'
        sdd_context.ref_domain_dictionary['Float'].name = 'Float'
        sdd_context.ref_domain_dictionary['Float'].description = 'Float'
        sdd_context.ref_domain_dictionary['Float'].type = 'Float'
        sdd_context.ref_domain_dictionary['Float'].save()
        sdd_context.ref_domain_dictionary['Boolean'] = DOMAIN()
        sdd_context.ref_domain_dictionary['Boolean'].domain_id = 'Boolean'
        sdd_context.ref_domain_dictionary['Boolean'].name = 'Boolean'
        sdd_context.ref_domain_dictionary['Boolean'].description = 'Boolean'
        sdd_context.ref_domain_dictionary['Boolean'].type = 'Boolean'
        sdd_context.ref_domain_dictionary['Boolean'].save()


    def _process_models(sdd_context, context):
        """
        Process all models in the 'pybirdai' app, creating cubes, structures,
        and processing fields.

        Args:
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        for model in apps.get_models():
            if model._meta.app_label == 'pybirdai':
                print(f"{model._meta.app_label}  -> {model.__name__}")
                ImportInputModel._create_cube_and_structure(model, sdd_context, context)
                ImportInputModel._process_fields(model, sdd_context, context)

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
            if isinstance(field, (CharField, DateTimeField, BigIntegerField, BooleanField, FloatField, CharField)):
                ImportInputModel._process_field(field, sdd_context, context)

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
        domain = ImportInputModel._create_domain_if_needed(field, sdd_context)
        ImportInputModel._create_variable_if_needed(field, variable_id, domain, sdd_context)
        ImportInputModel._create_cube_structure_item(field, variable_id, sdd_context, context)

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
                        member.member_id = f"{domain_id[0:len(domain_id)-7]}_{choice[0]}"
                        member.name = choice[1]
                        member.code = choice[0]
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
            variable.code = variable_id
            try:
                variable.name = field.verbose_name
            except AttributeError:
                pass
            if domain:
                variable.domain_id = domain
            else:
                if isinstance(field, (CharField)):
                    variable.domain_id = sdd_context.ref_domain_dictionary['String']
                if isinstance(field, (DateTimeField)):
                    variable.domain_id = sdd_context.ref_domain_dictionary['Date']
                if isinstance(field, (BigIntegerField)):
                    variable.domain_id = sdd_context.ref_domain_dictionary['Integer']
                if isinstance(field, ( BooleanField)):
                    variable.domain_id = sdd_context.ref_domain_dictionary['Boolean']
                if isinstance(field, (FloatField)):
                    variable.domain_id = sdd_context.ref_domain_dictionary['Float']

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

