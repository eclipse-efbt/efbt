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
import os
from pprint import pformat

from pybirdai.model_blueprint import Enumeration, Field, Relationship
from pybirdai.process_steps.forward_engineering import ldm_annotations


class BlueprintToDjango:
    '''
    Stage two of the SQL Developer import: write the finished model blueprint
    out as Django source. Classes are emitted after their superclasses so the
    generated module is valid Python, and enumerations become choices dicts.
    '''
    def convert(self,context):
        '''
        Documentation for the method.
        '''
        #ensure the existing files are properly removed and recreated
        models_path = context.output_directory + os.sep + 'database_configuration_files' + os.sep + 'models.py'
        admin_path = context.output_directory + os.sep + 'database_configuration_files' + os.sep + 'admin.py'

        # Force deletion and recreation to avoid append-related duplicates
        try:
            os.remove(models_path)
        except (FileNotFoundError, PermissionError):
            pass

        try:
            os.remove(admin_path)
        except (FileNotFoundError, PermissionError):
            pass

        # Use write mode instead of append to ensure clean files
        models_file = open(models_path, "w",  encoding='utf-8')
        admin_file = open(admin_path, "w",  encoding='utf-8')
        if context.ldm_or_il == 'ldm':
            BlueprintToDjango.createDjangoForPackage(self,context.ldm_entities_package,models_file,context)
            BlueprintToDjango.createDjangoAdminForPackage(self,context.ldm_entities_package,admin_file,context)
        else:
            BlueprintToDjango.createDjangoForPackage(self,context.il_tables_package,models_file,context)
            BlueprintToDjango.createDjangoAdminForPackage(self,context.il_tables_package,admin_file,context)

    def djangoChoices(self, theEnum):

        returnString =  theEnum.name + " = {"

        for value in theEnum.values:
            returnString  = returnString  + '\t\t' +"\""+ value.code + "\":\""+value.label + "\",\n"

        returnString  = returnString  + "}"
        return returnString

    def createDjangoForPackage(self, elpackage, output_file, context):
        '''
        Documentation for the method.
        '''
        output_file.write('from django.db import models\r\n')

        class_names_written = []
        for model_class in elpackage.model_classes:
            BlueprintToDjango.write_class_and_superclasses_in_correct_order(self, model_class, output_file, class_names_written)

        output_file.close()

    def write_class_and_superclasses_in_correct_order(self, elclass, output_file, classes_written):
        # Skip None classes - can happen with orphaned arc references
        if elclass is None:
            print("Warning: Skipping None class reference")
            return
        print(elclass.name)
        if elclass.name in classes_written:
            return
        else:
            if elclass.superclasses:
                supertype = elclass.superclass
                # Skip if supertype is None - can happen when arc source was reference data
                if supertype is None:
                    print(f"Warning: Skipping None supertype for class {elclass.name}")
                    output_file.write('class ' + elclass.name + '(models.Model):\r\n')
                    output_file.write('\ttest_id = models.CharField("test_id",max_length=255,default=None, blank=True, null=True)\r\n')
                else:
                    try:
                        print(supertype.name)
                    except:
                        print("no superclass name")
                    if supertype.name not in classes_written:
                        BlueprintToDjango.write_class_and_superclasses_in_correct_order(self, supertype, output_file, classes_written)
                    output_file.write('class ' + elclass.name + '(' + supertype.name + '):\r\n')
            else:
                output_file.write('class ' + elclass.name + '(models.Model):\r\n')
                output_file.write('\ttest_id = models.CharField("test_id",max_length=255,default=None, blank=True, null=True)\r\n')
            annotations = BlueprintToDjango.django_annotations(self, elclass)
            if annotations:
                formatted_annotations = pformat(annotations, width=120, sort_dicts=False)
                output_file.write('\t__bird_annotations__ = ' + formatted_annotations.replace('\n', '\r\n\t') + '\r\n')
            for elmember in elclass.members:
                if  isinstance(elmember , Field):
                    data_type = elmember.data_type
                    if isinstance(data_type, Enumeration):
                        output_file.write('\t' + BlueprintToDjango.djangoChoices(self,data_type) + '\r\n')
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255, choices=' + data_type.name +',default=None, blank=True, null=True, db_comment="' + data_type.name +'")\r\n')
                    elif (data_type.name == "String") and elmember.is_identifier:
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255, primary_key=True)\r\n')
                    elif data_type.name == "String":
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255,default=None, blank=True, null=True)\r\n')
                    elif data_type.name == "double":
                        output_file.write('\t' + elmember.name + ' = models.FloatField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif data_type.name == "int":
                        output_file.write('\t' + elmember.name + ' = models.BigIntegerField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif data_type.name == "Date":
                        output_file.write('\t' + elmember.name + ' = models.DateTimeField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif data_type.name == "boolean":
                        output_file.write('\t' + elmember.name + ' = models.BooleanField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                if isinstance(elmember, Relationship):
                    # only create a foreign key if the upper bound is 1, not that n to 1 relationships have
                    # a refernce on both sides of the relationship, we only show the one with cardiantlity of 1.
                    if elmember.upper_bound == 1:
                        # Sanitize field name - remove double underscores and leading underscores
                        field_name = elmember.name
                        # Replace double underscores with single
                        while '__' in field_name:
                            field_name = field_name.replace('__', '_')
                        if field_name.startswith('_'):
                            field_name = field_name[1:]
                        # Build related_name without double underscores
                        related_name = elclass.name + '_to_' + field_name + 's'
                        # Truncate related_name if too long (Django limit)
                        if len(related_name) > 200:
                            related_name = related_name[:200]
                        # Replace any double underscores in related_name
                        while '__' in related_name:
                            related_name = related_name.replace('__', '_')
                        output_file.write('\t' + field_name + ' = models.ForeignKey("' + elmember.target.name + '", models.SET_NULL,blank=True,null=True,related_name="' + related_name + '")\r\n')
                    else:
                        if elmember.opposite is not None:
                            pass
                        else:
                            print("asssociation with cardinality of N does not have an opposite relationship:" + elmember.name)

            long_name_exists = False
            for annotion in elclass.annotations:
                if annotion.source is not None:
                    if annotion.source.name == "long_name":
                        output_file.write('\t' + 'class Meta:\r\n')
                        output_file.write('\t\t' + 'verbose_name = \'' + annotion.details[0].value + '\'\r\n')
                        output_file.write('\t\t' + 'verbose_name_plural = \'' + annotion.details[0].value + 's\'\r\n')
                        long_name_exists = True
                else:
                    print("no source for annotation" + elclass.name)


            if not long_name_exists:
                output_file.write('\t' + 'class Meta:\r\n')
                output_file.write('\t\t' + 'verbose_name = \'' + elclass.name + '\'\r\n')
                output_file.write('\t\t' + 'verbose_name_plural = \'' + elclass.name + 's\'\r\n')

            classes_written.append(elclass.name)

    def django_annotations(self, elclass):
        '''
        Emit the LDM annotation contract of specs/BIRD_LDM_ANNOTATIONS_SPEC.md.
        The SQLDeveloper metadata collected during import is only one authoring
        source, so it is reduced to the same canonical shape that hand edits and
        other tools write.
        '''
        source_metadata = {}
        source_metadata.update(elclass.entity_metadata)
        source_metadata.update(elclass.key_metadata)
        if not source_metadata:
            return None
        return ldm_annotations.canonical_annotations({ldm_annotations.LDM_NAMESPACE: source_metadata}) or None

    def createDjangoAdminForPackage(self, elpackage, output_file, context):
        '''
        Documentation for the method.
        '''
        output_file.write('from django.contrib import admin\r\n')
        for model_class in elpackage.model_classes:
            output_file.write('from .models.bird_data_model import ' + model_class.name + '\n')
            output_file.write('admin.site.register(' + model_class.name + ')\n')
        output_file.close()
