from utils.utils import Utils
import os

from ecore4reg import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELPublicOperation, ELReference, ELAnnotation, ELStringToStringMapEntry

class SubtypeExploder(object):
    '''
    Documentation for SubtypeExploder
    '''

    def traverse(self, context, entity_name,
                 show_all_columns_for_subtype_explosion):
        '''
        Traverse through the LDM and recursively, for each delegate,
        show what are the subclasses of the delegate.
        Note that delegates represent an arc that represents a disjoint
        subtype in the SQLDeveloper LDM.
        We also imagine that any subclasses that are not in a delegated class
        Are in a special, dummy, delegate class.
        We list all the different scenarios of subclass combination
        The way we list this is using the concept of a discriminator which shows
        A chosen subclass, this in fact is the way that the forward engineered
        Input Layer represents a particular set of combined subclasses.
        If we have set the show_all_columns flag to be true, then we also
        show the columns required by each subclass.
        '''
        column_headers= []
        rows = []
        row = {}
        entity = SubtypeExploder.find_class_with_name(self, context,entity_name)
        #  Instrument_role Over_the_counter_OTC_Derivative_role
        SubtypeExploder.process_entity(self, context, entity,column_headers,
                                       row, rows,
                                       show_all_columns_for_subtype_explosion)
        full_or_summary = "_summary"
        
        if show_all_columns_for_subtype_explosion:
            full_or_summary = "_full"
            
        f = open(context.output_directory + os.sep + 'csv' +
                 os.sep + entity_name + '_discrimitor_combinations' +
                 full_or_summary + '.csv',
                 "a",  encoding='utf-8')
        counter = 0
        for column in column_headers:
            if counter == 0:
                f.write(column)
                counter = 1
            else:
                
                f.write(',' + column)
        f.write("\n")    
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
            
        
    def process_entity(self, context, entity,column_headers,
                       row,rows,show_all_columns_for_subtype_explosion):
        '''
        For a specific entity, append the attributes of that entity
        to required column headers (if show_all_columns = True ).
        Also append to the row any discrimitars, showing that we have some
        subclasses to deal with or that we have delegates that can have
        subclasses that we need to deal with.
        Also, recursively deal with any of the subclasses or any subclasses 
        of delegates
        '''
        current_row = row
        rows.append(current_row)
        
        if show_all_columns_for_subtype_explosion:
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
                    SubtypeExploder.process_entity(self, context, entity,
                                                   column_headers,
                                                   current_row_detached_clone,
                                                   rows,
                                                   show_all_columns_for_subtype_explosion)
        # we act as if there is one more discriminator, which is the name of the class prefixed with TYP_
        # and this has all the actual subclasses of this entity
        dummy_discriminator_name = entity.name + "_type"
        column_headers.append(dummy_discriminator_name)
        subclasses = SubtypeExploder.get_subclasses(self,context, entity)
        if not(subclasses is None):
                for subclass in subclasses:
                    current_row_detached_clone = current_row.copy()
                    current_row_detached_clone[dummy_discriminator_name] = subclass.name
                    SubtypeExploder.process_entity(
                        self, context, subclass,column_headers,
                        current_row_detached_clone,rows,
                        show_all_columns_for_subtype_explosion)

    def get_non_discriminator_references(self, context, entity):
        '''
        get any references from the entity, which are not delegates.
        Note that the delegates can represent the arcs of the 
        BIRD SQLDevelope model used to describe disjoint subtyping
        '''
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if not(ref.name.endswith('_delegate')):
                    reference_list.append(ref)
            
        return reference_list
    
    def get_attributes(self, context, entity):
        '''
        get the attributes of an entity
        '''
        attribute_list = []
        for attribute in entity.eStructuralFeatures:
            if isinstance(attribute,ELAttribute):
                attribute_list.append(attribute)
        return attribute_list
    
    def get_discriminators(self, context, entity):
        '''
        get any references from the entity, which are  delegates.
        Note that the delegates can represent the arcs of the 
        BIRD SQLDevelope model used to describe disjoint subtyping
        '''
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if ref.name.endswith('_delegate'):
                    reference_list.append(ref)
        return reference_list
    
    def get_possible_entities(self,context, discriminator):
        '''
        get any subclasses related to a delegate
        Note that the delegates can represent the arcs of the 
        BIRD SQLDevelope model used to describe disjoint subtyping
        The discriminator is the name of the attribute holding the 
        delegated class (or more likely, its subclasses)
        '''
        entity_type = discriminator.eType
        subclasses = SubtypeExploder.get_subclasses(self,context, entity_type)
        return subclasses
        
    def get_subclasses(self,context, entity_type):
        '''
        Get the subcalsses of a class
        '''
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
        
        