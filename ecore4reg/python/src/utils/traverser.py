from utils.utils import Utils
import os

from ecore4reg import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELPublicOperation, ELReference, ELAnnotation, ELStringToStringMapEntry

class SubtypeExploder(object):
    '''
    Documentation for SubtypeExploder
    '''

    def traverse(self, context):
        '''
        
        '''
        
        column_headers= []
        rows = []
        row = {}
        entity = SubtypeExploder.find_class_with_name(self, context, 'Instrument')
        #  Instrument_role Over_the_counter_OTC_Derivative_role
        SubtypeExploder.process_entity(self, context, entity,column_headers,row, rows)
        
        f = open(context.output_directory + os.sep + 'ecore' +
                 os.sep + 'discrimitor_combinations.csv',
                 "a",  encoding='utf-8')
        counter = 0
        for column in column_headers:
            if counter == 0:
                f.write(column)
                counter = 1
            else:
                
                f.write(',' + column)
                
        for the_row  in rows:
            counter = 0
            for column in column_headers:
                if counter == 0:
                    try:
                        f.write(the_row[column])
                    except KeyError:
                        print ("no col for " +column)
                    counter = 1
                else:
                    
                    try:
                        f.write(',' + the_row[column])
                    except KeyError:
                        f.write(',')
                        print ("no col for " +column)
            f.write("\n")
            
        
    def process_entity(self, context, entity,column_headers,row,rows):
        show_all_columns = False
        current_row = row
        rows.append(current_row)
        
        if show_all_columns:
            references = SubtypeExploder.get_non_discriminator_references(self, context, entity)
            for ref in references:
                column_headers.append(ref.name)
                current_row[ref.name] = 'X'

            
            attributes = SubtypeExploder.get_attributes(self, context, entity)
            
            for attribute in attributes:
                column_headers.append(attribute.name)
                current_row[attribute.name] = 'X'
            
        discrimitors = SubtypeExploder.get_discriminators(self, context, entity)
        for discriminator in discrimitors:
            column_headers.append(discriminator.name[0:len(discriminator.name)-9])
            entities = SubtypeExploder.get_possible_entities(self, context, discriminator)
            if not(entities is None):
                for entity in entities:
                    current_row_detached_clone = current_row.copy()
                    current_row_detached_clone[discriminator.name[0:len(discriminator.name)-9]] = entity.name
                    SubtypeExploder.process_entity(self, context, entity,column_headers,current_row_detached_clone,rows)

    def get_non_discriminator_references(self, context, entity):
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if not(ref.name.endswith('_delegate')):
                    reference_list.append(ref)
            
        return reference_list
    
    def get_attributes(self, context, entity):
        attribute_list = []
        for attribute in entity.eStructuralFeatures:
            if isinstance(attribute,ELAttribute):
                attribute_list.append(attribute)
        return attribute_list
    
    def get_discriminators(self, context, entity):
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if ref.name.endswith('_delegate'):
                    reference_list.append(ref)
        return reference_list
    
    def get_possible_entities(self,context, discriminator):
        entity_type = discriminator.eType
        subclasses = SubtypeExploder.get_subclasses(self,context, entity_type)
        return subclasses
        
    def get_subclasses(self,context, entity_type):
        subclass_list = []
        for eclassifier in context.input_tables_package.eClassifiers:
            if isinstance(eclassifier,ELClass):
                if len(eclassifier.eSuperTypes) > 0:
                    if eclassifier.eSuperTypes[0] == entity_type:
                        subclass_list.append(eclassifier)
        return subclass_list
        
    def find_class_with_name(self, context, name):
        '''
        get the class with this name from the input tables package
        '''
        for eclassifier in context.input_tables_package.eClassifiers:
            if isinstance(eclassifier, ELClass):
                if eclassifier.name == name:
                    return eclassifier
        
        