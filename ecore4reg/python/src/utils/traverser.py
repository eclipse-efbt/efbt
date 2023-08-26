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
        input_layer_column_headers =[]
        rows = []
        row = {}
        entity = SubtypeExploder.find_class_with_name(self, context,entity_name)
        #  Instrument_role Over_the_counter_OTC_Derivative_role
        entity_list = []
        entity_list.append(entity)
        
        SubtypeExploder.process_entity(self, context, [], entity, entity_list,column_headers,
                                       input_layer_column_headers,row, rows,
                                       show_all_columns_for_subtype_explosion)
        full_or_summary = "_summary"
        
        if show_all_columns_for_subtype_explosion:
            full_or_summary = "_full"
            
        f = open(context.output_directory + os.sep + 'csv' +
                 os.sep + entity_name + '_discrimitor_combinations' +
                 full_or_summary + '.csv',
                 "a",  encoding='utf-8')
        counter = 0
        # write the columns, which are all LDM attributes
        for column in column_headers:
            if counter == 0:
                f.write(column)
                counter = 1
            else:
                
                f.write(',' + column)
        f.write("\n")  
        counter = 0
        # write the corresponding input layer columns also
        for column in input_layer_column_headers:
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
            
        
    def process_entity(self, context, discriminator_list, parent_entity, entity_combination,column_headers,input_layer_column_headers,
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

        count = 0            
        for discriminator in discriminator_list:
            qualified_attribute_name = parent_entity.name + "." + discriminator.name
            
            if not(qualified_attribute_name in column_headers):
                column_headers.append(qualified_attribute_name)
                input_layer_column_name = SubtypeExploder.get_input_layer_column(self,discriminator)
                input_layer_column_headers.append(input_layer_column_name)  
            current_row[qualified_attribute_name] = entity_combination[count].name
            count = count +1
        for entity in entity_combination:
            if show_all_columns_for_subtype_explosion:
                references = SubtypeExploder.get_non_discriminator_references(self, context, entity)
                for ref in references:
                    qualified_attribute_name = entity.name + "." + ref.name
                    if not(qualified_attribute_name in column_headers):
                        column_headers.append(qualified_attribute_name)
                        input_layer_column_name = SubtypeExploder.get_input_layer_column(self,ref)
                        input_layer_column_headers.append(input_layer_column_name)
                        current_row[qualified_attribute_name] = 'X'
    
                attributes = SubtypeExploder.get_attributes(self, context, entity)
                
                for attribute in attributes:
                    qualified_attribute_name = entity.name + "." + attribute.name
                    if not(qualified_attribute_name in column_headers):
                        column_headers.append(qualified_attribute_name)
                        input_layer_column_name = SubtypeExploder.get_input_layer_column(self,attribute)
                        input_layer_column_headers.append(input_layer_column_name)
                        current_row[entity.name + "."+ attribute.name] = 'X'

                discriminators = SubtypeExploder.get_discriminators(self, context, entity)
                columns = []
                for discriminator in discriminators:
                    SubtypeExploder.enrich_discrimitor_columns(self, context, discriminator,columns)
                   
                if len(columns) > 0:
                    count = 0
                    for each_entity in columns[0]:
                        entity_combination = []
                        
                        for column in columns: 
                            entity_combination.append(column[count])
                        
            
                        current_row_detached_clone = current_row.copy()
                        #current_row_detached_clone[qualified_attribute_name] = each_entity.name
                        SubtypeExploder.process_entity(self, context, discriminators, entity, entity_combination,
                                                       column_headers, input_layer_column_headers,
                                                       current_row_detached_clone,
                                                       rows,
                                                       show_all_columns_for_subtype_explosion)
                        count = count + 1
        rows.append(current_row)

    def enrich_discrimitor_columns(self, context, discriminator, list_of_lists):
        
        entities = SubtypeExploder.get_possible_entities(self, context, discriminator)
        discrimitors_entity_list = []
        if len(list_of_lists) == 0:
            for entity in entities:
                discrimitors_entity_list.append(entity)
        else:
            first_one = True
            for the_list in list_of_lists:
                list_copy = the_list.copy()
                first_one = False
                for entity in entities:
                    for item in list_copy:
                        if not(first_one):
                            the_list.append(item)
                        discrimitors_entity_list.append(entity)
                    first_one = True      
               
        list_of_lists.append(discrimitors_entity_list)
                
        
                
                
            
    def get_non_discriminator_references(self, context, entity):
        '''
        get any non-containment references from the entity, which are not delegates.
        Note that the delegates can represent the arcs of the 
        BIRD SQLDevelope model used to describe disjoint subtyping
        '''
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if not(ref.name.endswith('_delegate')) and not(ref.containment):
                    reference_list.append(ref)
            
        return reference_list
    
    def get_non_discriminator_containment_references(self, context, entity):
        '''
        get any containment references from the entity, which are not delegates.
        Note that the delegates can represent the arcs of the 
        BIRD SQLDevelope model used to describe disjoint subtyping
        '''
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference) and ref.containment:
                if not(ref.name.endswith('_delegate')):
                    reference_list.append(ref)
            
        return reference_list
    
    def get_input_layer_column(self,feature):
        '''
        From the annotation find the the link to input layer column
        '''
        return_value = "UNKNOWN"
        annotation = feature.eAnnotations
        if not(annotation is None):
            details = annotation.details
            
            for detail in details:
                if detail.key == "il_column":
                    return_value = detail.value
            
        return return_value
            
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
        BIRD SQLDeveloper model used to describe disjoint subtyping
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
        
        