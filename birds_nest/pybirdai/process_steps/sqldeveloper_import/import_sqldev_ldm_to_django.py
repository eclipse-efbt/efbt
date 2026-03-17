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

from pybirdai.regdna import ELAttribute, ELClass, ELEnum
from pybirdai.regdna import ELReference

class RegDNAToDJango:
    '''
    Documentation for SQLDevLDMImport
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
            RegDNAToDJango.createDjangoForPackage(self,context.ldm_entities_package,models_file,context)
            RegDNAToDJango.createDjangoAdminForPackage(self,context.ldm_entities_package,admin_file,context)
        else:
            RegDNAToDJango.createDjangoForPackage(self,context.il_tables_package,models_file,context)
            RegDNAToDJango.createDjangoAdminForPackage(self,context.il_tables_package,admin_file,context)

    def djangoChoices(self, theEnum):

        returnString =  theEnum.name + " = {"

        for literal in  theEnum.eLiterals:
            returnString  = returnString  + '\t\t' +"\""+ literal.literal + "\":\""+literal.name + "\",\n"

        returnString  = returnString  + "}"
        return returnString

    def createDjangoForPackage(self, elpackage, output_file, context):
        '''
        Documentation for the method.
        '''
        output_file.write('from django.db import models\r\n')

        for theImport in elpackage.imports:
            if not(theImport.importedNamespace.trim == "types.*"):
                output_file.write('from ' + theImport.importedNamespace + ' import *\r\n')
        class_names_written = []
        for elclass in elpackage.eClassifiers:
            if  isinstance(elclass ,ELClass):
                RegDNAToDJango.write_class_and_superclasses_in_correct_order(self, elclass, output_file, class_names_written)

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
            if len(elclass.eSuperTypes) > 0:
                supertype = elclass.eSuperTypes[0]
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
                        RegDNAToDJango.write_class_and_superclasses_in_correct_order(self, supertype, output_file, classes_written)
                    output_file.write('class ' + elclass.name + '(' + supertype.name + '):\r\n')
            else:
                output_file.write('class ' + elclass.name + '(models.Model):\r\n')
                output_file.write('\ttest_id = models.CharField("test_id",max_length=255,default=None, blank=True, null=True)\r\n')
            for elmember in elclass.eStructuralFeatures:
                if  isinstance(elmember ,ELAttribute):
                    if isinstance(elmember.eAttributeType, ELEnum):
                        output_file.write('\t' + RegDNAToDJango.djangoChoices(self,elmember.eAttributeType) + '\r\n')
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255, choices=' + elmember.eAttributeType.name +',default=None, blank=True, null=True, db_comment="' + elmember.eAttributeType.name +'")\r\n')
                    elif (elmember.eAttributeType.name == "String") and elmember.iD:
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255, primary_key=True)\r\n')
                    elif elmember.eAttributeType.name == "String":
                        output_file.write('\t' + elmember.name + ' = models.CharField("' + elmember.name + '",max_length=255,default=None, blank=True, null=True)\r\n')
                    elif elmember.eAttributeType.name == "double":
                        output_file.write('\t' + elmember.name + ' = models.FloatField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif elmember.eAttributeType.name == "int":
                        output_file.write('\t' + elmember.name + ' = models.BigIntegerField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif elmember.eAttributeType.name == "Date":
                        output_file.write('\t' + elmember.name + ' = models.DateTimeField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                    elif elmember.eAttributeType.name == "boolean":
                        output_file.write('\t' + elmember.name + ' = models.BooleanField("' + elmember.name + '",default=None, blank=True, null=True)\r\n')
                if isinstance(elmember, ELReference):
                    # only create a foreign key if the upper bound is 1, not that n to 1 relationships have
                    # a refernce on both sides of the relationship, we only show the one with cardiantlity of 1.
                    if elmember.upperBound == 1:
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
                        output_file.write('\t' + field_name + ' = models.ForeignKey("' + elmember.eType.name + '", models.SET_NULL,blank=True,null=True,related_name="' + related_name + '")\r\n')
                    else:
                        if elmember.eOpposite is not None:
                            pass
                        else:
                            print("asssociation with cardinality of N does not have an opposite relationship:" + elmember.name)

            long_name_exists = False
            for annotion in elclass.eAnnotations:
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

    def createDjangoAdminForPackage(self, elpackage, output_file, context):
        '''
        Documentation for the method.
        '''
        output_file.write('from django.contrib import admin\r\n')
        for elclass in elpackage.eClassifiers:
            if  isinstance(elclass ,ELClass):
                output_file.write('from .models.bird_data_model import ' + elclass.name + '\n')
                output_file.write('admin.site.register(' + elclass.name + ')\n')
        output_file.close()
