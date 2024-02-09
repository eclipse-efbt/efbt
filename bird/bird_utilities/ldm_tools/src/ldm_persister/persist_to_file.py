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
import os
from pyecore.resources import ResourceSet, URI
from ldm_utils.utils import Utils


from regdna import ELAttribute, ELClass, ELEnum, ELOperation, ELReference,SelectColumnAttributeAs


class PersistToFile:
    '''
    Documentation for PersistToFile
    '''

    def save_model_as_regdna_file(self, context):
        '''
        Save resources as regdna files
        '''
        if context.load_eil_from_website:
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "regdna", context.sdd_domains_package)
        else:        
            PersistToFile.persist_entity_model(
                self, context, context.input_tables_package,
                "regdna", context.il_domains_package)

        
        PersistToFile.persist_enum_model(
            self, context, context.il_domains_package, "regdna")
        
        PersistToFile.persist_types_model(
            self, context, context.types_package, "regdna")
        
        

    def persist_entity_model(self, context, the_package, extension, imported_package):
        '''
        Documentation for persist_entity_model
        '''

        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name + '.' + extension,
                 "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t import " + imported_package.name + ".*\r")
        
        if extension == "regdna":
            f.write("\t\t import types.*\r")
        for classifier in the_package.eClassifiers:
            if isinstance(classifier, ELClass):
                for annotation in classifier.eAnnotations:
                    f.write("\t\t\t@")
                    f.write(annotation.source.name)
                    f.write("(")
                    first_item = True
                    for detail in annotation.details:
                        if first_item:
                            first_item = False
                            f.write(detail.key + "=" + detail.value)
                        else:
                            f.write("," + detail.key + "=" + detail.value )
                    f.write(")\r")
                f.write("\t\t\t")
                if classifier.eAbstract:
                    f.write("abstract ")
                f.write("class " + classifier.name)
                if (hasattr(classifier, "eSuperTypes") and len(classifier.eSuperTypes) > 0):
                    f.write(" extends " + classifier.eSuperTypes[0].name)
                f.write(" {\r")
                for member in classifier.eStructuralFeatures:
                    for annotation in member.eAnnotations:
                        f.write("\t\t\t\t@")
                        f.write(annotation.source.name)
                        f.write("(")
                        first_item = True
                        for detail in annotation.details:
                            if first_item:
                                first_item = False
                                f.write(detail.key + "=" + detail.value)
                            else:
                                f.write("," + detail.key + "=" + detail.value )
                        f.write(")\r")
                    if isinstance(member, ELReference):
                        if member.containment:
                            f.write("\t\t\t\tcontains ")
                        else:
                            f.write("\t\t\t\trefers ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                            
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")
                    elif isinstance(member, ELAttribute):
                        f.write("\t\t\t\t")
                        if member.iD:
                            f.write("id ")

                        if member.eType.name == "String":
                            f.write("String  ")
                        elif member.eType.name == "Double":
                            f.write("double  ")
                        elif member.eType.name == "Integer":
                            f.write("int  ")
                        elif member.eType.name == "Date":
                            f.write("Date  ")
                        else:
                            f.write(member.eType.name + " ")

                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)
                           
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")

                        f.write(member.name)
                        f.write(" \r")

                for member in classifier.eOperations:
                    for annotation in member.eAnnotations:
                        f.write("\t\t\t\t@")
                        f.write(annotation.source.name)
                        f.write("(")
                        first_item = True
                        for detail in annotation.details:
                            if first_item:
                                first_item = False
                                f.write(detail.key + "=" + detail.value)
                            else:
                                f.write("," + detail.key + "=" + detail.value )
                        f.write(")\r")
                    if isinstance(member, ELOperation):
                        f.write("\t\t\t\top ")

                        f.write(member.eType.name + " ")
                        if ((member.lowerBound == 0) and (member.upperBound == 1)):
                            f.write(" ")
                        else:
                            upperBoundNumber = member.upperBound
                            upperBoundString = str(member.upperBound)                            
                            
                            f.write("[" + str(member.lowerBound) +
                                    ".." + upperBoundString + "] ")
                            
                        f.write(member.name)
                        f.write("() {}")
                            
                        f.write(" \r")

                f.write("\t\t\t}\r")

        f.close()

    def persist_types_model(self, context, the_package, extension):
        '''
        Documentation for persist_types_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        f.write("\t\t\ttype double wraps double\r")
        f.write("\t\t\ttype String wraps String\r")
        f.write("\t\t\ttype int wraps int\r")
        if extension == "regdna":
            f.write("\t\t\ttype Date wraps Date\r")
        else:
            f.write("\t\t\ttype Date wraps java.util.Date\r")
        f.write("\t\t\ttype boolean wraps boolean\r")
        f.close()

    def persist_enum_model(self, context, the_package, extension):
        '''
        Documentation for persist_enum_model
        '''
        f = open(context.output_directory + os.sep + extension +
                 os.sep + the_package.name +
                 '.' + extension, "a",  encoding='utf-8')
        f.write("\t\t package " + the_package.name + "\r")
        for classifier in the_package.eClassifiers:

            if isinstance(classifier, ELEnum):
                f.write("\t\t\tenum " + classifier.name)

                f.write(" { ")
                counter = 0
                splitcount = 1
                for the_literal in classifier.eLiterals:
                    counter = counter+1

                    if counter < 1500:
                        f.write(" " + the_literal.name + " as \"" +
                                the_literal.literal + "\" = " + str(the_literal.value))
                    else:
                        counter = 0
                        splitcount = splitcount+1
                        f.write(" }\r")
                        f.write("\t\t\tenum " + classifier.name +
                                "_" + str(splitcount))
                        f.write(" { ")

                f.write(" }\r")

        f.close()

    
    
    def save_model_as_xmi_file(self, context):
        '''
         save model as an xmi file representing an object tree.
        '''
        rset = ResourceSet()

        # This will create an XMI resource
        resource = rset.create_resource(
            URI(context.output_directory + os.sep + 'xmi' + os.sep + 'birdseed.xmi'))
        resource.append(context.module_list)
        resource.save()
  