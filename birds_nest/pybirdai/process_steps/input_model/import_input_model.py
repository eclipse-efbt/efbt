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

from pybirdai.bird_meta_data_model import *
from django.apps import apps
from django.db import models
import os
import csv
from pybirdai.context.csv_column_index_context import ColumnIndexes
from django.db.models.fields import CharField,DateTimeField,BooleanField,FloatField,BigIntegerField
from django.db import transaction
from uuid import uuid4
from pybirdai.views import load_variables_from_csv_file
import logging
from django.conf import settings
import copy

logging.basicConfig(level=logging.INFO)

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
        sdd_context.csi_counter = dict()
        context.fields = dict()
        context.derived_properties = dict()
        ImportInputModel._fetch_derived_fields_from_model(context)
        ImportInputModel._create_maintenance_agency(sdd_context)
        ImportInputModel._create_primitive_domains(sdd_context)
        ImportInputModel._create_subdomain_to_domain_map(sdd_context)
        ImportInputModel._process_models(sdd_context, context)
         # Load extra variables from CSV file
        from django.conf import settings
        base_dir = settings.BASE_DIR
        extra_variables_path = os.path.join(base_dir, 'resources', 'extra_variables', 'extra_variables.csv')
        variables_loaded = load_variables_from_csv_file(extra_variables_path)
        if variables_loaded > 0:
            print(f"Loaded {variables_loaded} extra variables from CSV file.")
        else:
            print("No extra variables loaded (file not found or empty).")


    def _create_maintenance_agency(sdd_context):
        """
        Create a maintenance agency named 'REF' and add it to the SDD context.

        Args:
            sdd_context: The SDD context object to store the created
                         maintenance agency.
        """
        # Prepare all agencies at once
        agencies = [
            MAINTENANCE_AGENCY(
                name="REF",
                code="REF",
                maintenance_agency_id="REF"
            ),
            MAINTENANCE_AGENCY(
                name="NODE",
                code="NODE",
                maintenance_agency_id="NODE"
            ),
            MAINTENANCE_AGENCY(
                name="SDD_DOMAIN",
                code="SDD_DOMAIN",
                maintenance_agency_id="SDD_DOMAIN"
            )
        ]

        # Bulk create all agencies
        created_agencies = MAINTENANCE_AGENCY.objects.bulk_create(agencies, ignore_conflicts=True)

        # Update dictionary with created instances
        sdd_context.agency_dictionary.update({
            agency.code: agency for agency in created_agencies
        })


    def _create_primitive_domains(sdd_context):
        """
        Create a 'String' domain and add it to the SDD context.

        Args:
            sdd_context: The SDD context object to store the created string
                         domain.
        """
        domains = []
        for domain_type in ['String', 'Integer', 'Date', 'Float', 'Boolean']:
            domain = DOMAIN(
                domain_id=domain_type,
                name=domain_type,
                description=domain_type,
                data_type=domain_type
            )
            domains.append(domain)
            sdd_context.domain_dictionary[domain_type] = domain

        # Bulk create all domains
        DOMAIN.objects.bulk_create(domains, ignore_conflicts=True)

    def _create_subdomain_to_domain_map(sdd_context):
        file_location = sdd_context.file_directory + os.sep + "technical_export" + os.sep + "subdomain.csv"
        header_skipped = False

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if not header_skipped:
                    header_skipped = True
                else:
                    domain_id = row[ColumnIndexes().subdomain_domain_id_index]
                    subdomain_id = row[ColumnIndexes().subdomain_subdomain_id_index]
                    sdd_context.subdomain_to_domain_map[subdomain_id] = domain_id

    def _process_models(sdd_context, context):
        """
        Process all models in the 'pybirdai' app, creating cubes, structures,
        and processing fields.

        Args:
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        logging.info(f"Creating models for pybirdai")
        for model in set([_ for _ in apps.get_models() if _._meta.app_label == 'pybirdai']):
            logging.info(f"Creating cube and structure for {model.__name__}")
            ImportInputModel._create_cube_and_structure(model, sdd_context, context)
            ImportInputModel._process_fields(model, sdd_context, context)
            continue

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

    def _fetch_derived_fields_from_model(context):
        import ast

        # Read bird_data_model.py and find functions with lineage decorator
        bird_data_model_path = os.path.join(settings.BASE_DIR, 'pybirdai/bird_data_model.py')


        with open(bird_data_model_path, 'r', encoding='utf-8') as file:
            tree = ast.parse(file.read())


            for node in [_node for _node in tree.body if isinstance(_node, ast.ClassDef)]:
                property_nodes = []

                for function_node in [function_node for function_node in node.body if isinstance(function_node, ast.FunctionDef)]:
                    for decorator in function_node.decorator_list:
                        if isinstance(decorator, ast.Call) and isinstance(decorator.func, ast.Name) and decorator.func.id == 'lineage':
                            property_nodes.append(function_node.name)

                if not property_nodes:
                    continue

                for meta_node in [meta_node for meta_node in node.body if isinstance(meta_node, ast.ClassDef) and meta_node.name == 'Meta']:
                    for field in meta_node.body:
                        if isinstance(field, ast.Assign) and field.targets[0].id == "verbose_name":
                            verbose_name = field.value.s
                            break

                if verbose_name not in context.derived_properties:
                    context.derived_properties[verbose_name] = []

                context.derived_properties[verbose_name] = property_nodes
                logging.info(f"Processed derived properties for {verbose_name} : {property_nodes}")
                property_nodes = []

    def _provide_fields(context,verbose_name,model_name ):
        """
        Provide type information for a list of variable IDs.

        Args:
            list_of_variable_ids: A list of variable IDs to provide type information for.
        """

        fields = []
        for variable_id in context.derived_properties[verbose_name]:
            # Implementation details here
            if variable_id in context.fields:
                field = copy.copy(context.fields[variable_id])
                field.model.__name__ = model_name
                fields.append(field)
                continue
            fields.append(models.CharField(variable_id,max_length=1000))
        return fields

    def _process_fields(model, sdd_context, context):
        """
        Process all fields of the given model.

        Args:
            model: The Django model whose fields are to be processed.
            sdd_context: The SDD context object to store created elements.
            context: The context object containing configuration settings.
        """
        relevant_field_types = (CharField, DateTimeField, BigIntegerField, BooleanField, FloatField)

        fields = [
            field for field in model._meta.get_fields()
            if isinstance(field, relevant_field_types)
        ]

        context.fields.update({field.name: field for field in fields})

        if model._meta.verbose_name in context.derived_properties:
            fields += ImportInputModel._provide_fields(context,model._meta.verbose_name,model.__name__)
            # logging.info(f"Processing derived properties for model {model._meta.verbose_name} :: fields : {fields}")

        variables_to_create = []
        cube_structure_items_to_create = []

        for field in fields:
            variable_id = field.name
            domain, subdomain = ImportInputModel._create_domain_and_subdomain_if_needed(field, sdd_context)

            # Create variable if needed
            if variable_id not in sdd_context.variable_dictionary:
                variable = VARIABLE(
                    maintenance_agency_id=sdd_context.agency_dictionary["REF"],
                    variable_id=variable_id,
                    code=variable_id,
                    name=getattr(field, 'verbose_name', variable_id),
                    domain_id=domain or ImportInputModel._get_default_domain(field, sdd_context)
                )
                variables_to_create.append(variable)
                sdd_context.variable_dictionary[variable_id] = variable

            # Create cube structure item

            if context.save_derived_sdd_items:
                csid = sdd_context.bird_cube_structure_dictionary[model.__name__]
                if (csid,variable_id) not in sdd_context.csi_counter:
                    sdd_context.csi_counter[(csid,variable_id)] = 0

                variable_id_ = sdd_context.variable_dictionary[variable_id]

                csi = CUBE_STRUCTURE_ITEM(
                    cube_structure_id=csid,
                    variable_id=variable_id_,
                    subdomain_id=subdomain,
                    cube_variable_code = "__".join([csid.cube_structure_id,variable_id_.variable_id,str(sdd_context.csi_counter[(csid,variable_id)])])
                )

                cube_structure_items_to_create.append(csi)
                #key = f"{csi.cube_structure_id.cube_structure_id}:{csi.variable_id.variable_id}"
                if csi.cube_structure_id.cube_structure_id not in sdd_context.bird_cube_structure_item_dictionary.keys():
                    sdd_context.bird_cube_structure_item_dictionary[csi.cube_structure_id.cube_structure_id] = []
                sdd_context.bird_cube_structure_item_dictionary[csi.cube_structure_id.cube_structure_id].append(csi)
                sdd_context.csi_counter[(csid,variable_id)] += 1

        # Bulk create all objects
        if variables_to_create and sdd_context.save_sdd_to_db:
            VARIABLE.objects.bulk_create(variables_to_create, ignore_conflicts=True)

        if cube_structure_items_to_create and context.save_derived_sdd_items:
            if model._meta.verbose_name == "Party_role":
                logging.info("Party_role cube structure items to be created :: %s", len(cube_structure_items_to_create))
            for item in cube_structure_items_to_create:
                item.save()

    @staticmethod
    def _get_default_domain(field, sdd_context):
        if isinstance(field, CharField):
            return sdd_context.domain_dictionary['String']
        elif isinstance(field, DateTimeField):
            return sdd_context.domain_dictionary['Date']
        elif isinstance(field, BigIntegerField):
            return sdd_context.domain_dictionary['Integer']
        elif isinstance(field, BooleanField):
            return sdd_context.domain_dictionary['Boolean']
        elif isinstance(field, FloatField):
            return sdd_context.domain_dictionary['Float']
        return None


    def _create_domain_and_subdomain_if_needed(field, sdd_context):
        """
        Create a domain for the field if it doesn't exist and add it to the
        SDD context.

        Args:
            field: The Django model field to create a domain for.
            sdd_context: The SDD context object to store created elements.

        Returns:
            The created or existing domain, or None if no domain is needed.
        """
        try:
            subdomain_id = field.db_comment
            if not subdomain_id:
                return None, None

            domain_id = None
            try:
                domain_id = sdd_context.subdomain_to_domain_map[subdomain_id[0:len(subdomain_id)-7]]
            except KeyError:
                pass

            domains_to_create = []
            subdomains_to_create = []
            members_to_create = []
            subdomain_enums_to_create = []

            # Create domain if needed
            if domain_id and domain_id not in sdd_context.domain_dictionary:
                domain = DOMAIN(
                    domain_id=domain_id,
                    name=domain_id
                )
                domains_to_create.append(domain)
                sdd_context.domain_dictionary[domain_id] = domain
                print(f"Adding domain: {domain_id}")

            # Create subdomain if needed
            subdomain = None
            if subdomain_id and subdomain_id not in sdd_context.subdomain_dictionary:
                subdomain = SUBDOMAIN(
                    subdomain_id=subdomain_id,
                    domain_id=sdd_context.domain_dictionary.get(domain_id)
                )
                subdomains_to_create.append(subdomain)
                sdd_context.subdomain_dictionary[subdomain_id] = subdomain
                print(f"Adding subdomain: {subdomain_id}")

            # Handle choices and create members
            if field.choices:
                for choice in field.choices:
                    member_id = f"{domain_id}_{choice[0]}"
                    if member_id not in sdd_context.member_dictionary:
                        member = MEMBER(
                            member_id=member_id,
                            name=choice[1],
                            code=choice[0],
                            domain_id=sdd_context.domain_dictionary.get(domain_id)
                        )
                        members_to_create.append(member)
                        sdd_context.member_dictionary[member_id] = member

                        if subdomain:
                            subdomain_enum = SUBDOMAIN_ENUMERATION(
                                subdomain_id=subdomain,
                                member_id=member
                            )
                            subdomain_enums_to_create.append(subdomain_enum)
                            enum_key = f"{subdomain_id}:{member_id}"
                            sdd_context.subdomain_enumeration_dictionary[enum_key] = subdomain_enum

            # Bulk create all objects
            if domains_to_create and sdd_context.save_sdd_to_db:
                DOMAIN.objects.bulk_create(domains_to_create, ignore_conflicts=True)

            if subdomains_to_create and sdd_context.save_sdd_to_db:
                SUBDOMAIN.objects.bulk_create(subdomains_to_create, ignore_conflicts=True)

            if members_to_create and sdd_context.save_sdd_to_db:
                MEMBER.objects.bulk_create(members_to_create, ignore_conflicts=True)

            if subdomain_enums_to_create and sdd_context.save_sdd_to_db:
                SUBDOMAIN_ENUMERATION.objects.bulk_create(subdomain_enums_to_create, ignore_conflicts=True)

            return sdd_context.domain_dictionary.get(domain_id), subdomain

        except AttributeError:
            return None, None
