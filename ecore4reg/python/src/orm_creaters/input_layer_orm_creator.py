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

'''

@author: Neil
'''
from ecore4reg import ELAttribute, ELClass
from ecore4reg import ELPublicOperation, ELReference


class CreateORMForIL(object):
    '''
    Documentation for CreateORMForIL
    '''
    def create_orm_for_il(self, context):
        '''
        create orm for il
        '''
        input_tables_package = context.input_tables_package
        input_tables_package_orm = context.input_tables_package_orm
        output_tables_package = context.output_tables_package
        output_tables_package_orm = context.output_tables_package_orm
        
        for classifier in input_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):
                eclass = CreateORMForIL.get_relationships_class(self,classifier)
                eclass_attributes = CreateORMForIL.get_attributes_class(self,classifier) 
                eclass_attributes_table = ELClass(name=eclass.name+"_attributes_table")  
                eclass_objects_table = ELClass(name=eclass.name+"_objects_table")
                
                attribute_reference = ELReference(name = "attributes")
                attribute_reference.upperBound = 1
                attribute_reference.lowerBound = 1
                attribute_reference.eType = eclass_attributes
                attribute_reference.containment = False
                eclass.eStructuralFeatures.append(attribute_reference)
                
                attribute_reference_list = ELReference(name = "attribute_list")
                attribute_reference_list.upperBound = -1
                attribute_reference_list.lowerBound = 0
                attribute_reference_list.eType = eclass_attributes
                attribute_reference_list.containment = True
                eclass_attributes_table.eStructuralFeatures.append(attribute_reference_list)
                
                object_reference_list = ELReference(name = "object_list")
                object_reference_list.upperBound = -1
                object_reference_list.lowerBound = 0
                object_reference_list.eType = eclass
                object_reference_list.containment = False
                eclass_objects_table.eStructuralFeatures.append(object_reference_list)
                
                object_table_to_attribute_table_link = ELReference(name = "object_table_to_attribute_table_link")
                object_table_to_attribute_table_link.upperBound = -1
                object_table_to_attribute_table_link.lowerBound = 0
                object_table_to_attribute_table_link.eType = eclass_attributes_table
                object_table_to_attribute_table_link.containment = False
                eclass_objects_table.eStructuralFeatures.append(object_table_to_attribute_table_link)
                
                init_operation = ELPublicOperation()
                init_operation.name = 'init'
                init_operation.eType = context.e_string
                eclass_objects_table.eOperations.append(init_operation)
            
                input_tables_package_orm.eClassifiers.append(eclass)
                input_tables_package_orm.eClassifiers.append(eclass_attributes_table)
                input_tables_package_orm.eClassifiers.append(eclass_attributes)
                input_tables_package_orm.eClassifiers.append(eclass_objects_table)
                
        for classifier in output_tables_package.eClassifiers:
            if isinstance(classifier, ELClass):         
                eclass_attributes = CreateORMForIL.get_operations_class(self,classifier)
                output_tables_package_orm.eClassifiers.append(eclass_attributes)
                fullTableName=classifier.name+"_Table"
                eclass_table = ELClass(
                            name=fullTableName)
                containment_reference = ELReference()
                containment_reference.name = eclass_attributes.name+"s"
                containment_reference.eType = eclass_attributes
                containment_reference.upperBound = -1
                containment_reference.lowerBound = 0
                containment_reference.containment = True
                eclass_table.eStructuralFeatures.append(
                    containment_reference)
                
                table_operation = ELPublicOperation()
                table_operation.name = eclass_attributes.name+"s"
                table_operation.eType = eclass_attributes
                table_operation.upperBound = -1
                table_operation.lowerBound = 0
                eclass_table.eOperations.append(table_operation)
                output_tables_package_orm.eClassifiers.append(eclass_table)
                
        # fix the relationship classes
        for classifier in input_tables_package_orm.eClassifiers:
            if isinstance(classifier, ELClass):
                for feature in classifier.eStructuralFeatures:
                    if isinstance(feature, ELReference):
                        ref_type = feature.eType
                        type_name = ref_type.name
                        eclass = CreateORMForIL.get_class_from_input_tables_package_orm(self,context,type_name)
                        feature.eType = eclass

    def get_class_from_input_tables_package_orm(self,context,type_name):
        for classifier in context.input_tables_package_orm.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == type_name:
                    return classifier
            
            
            
    def get_attributes_class(self,eclass):
        list_of_attrs_to_add = []
        attribute_class = ELClass(name = eclass.name + "_attributes")
        for feature in eclass.eStructuralFeatures:
            if isinstance(feature, ELAttribute):
                list_of_attrs_to_add.append(feature)
                
        for attr in list_of_attrs_to_add:
            attribute_class.eStructuralFeatures.append(attr)      
            
        return attribute_class
    
    def get_operations_class(self,eclass):
        list_of_ops_to_add = []
        operations_class = ELClass(name = eclass.name + "_attributes")
        for operation in eclass.eOperations:
                list_of_ops_to_add.append(operation)
                
        for op in list_of_ops_to_add:
            operations_class.eOperations.append(op)  
            
        return operations_class
    
    def get_relationships_class(self,eclass):
        list_of_refs_to_add = []
        relationships_class = ELClass(name = eclass.name)
        for feature in eclass.eStructuralFeatures:
            if isinstance(feature, ELReference):
                list_of_refs_to_add.append(feature)

        for ref in  list_of_refs_to_add:     
            relationships_class.eStructuralFeatures.append(ref)
                
        return relationships_class
                 
        
        
         