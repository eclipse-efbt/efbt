from utils.utils import Utils
import os

from ecore4reg import ELAttribute, ELClass, ELEnum, ELEnumLiteral, ELPublicOperation, ELReference, ELAnnotation, ELStringToStringMapEntry

class SubtypeExploder(object):
    '''
    To make input layer test data for transformations, we want to have 
    some ’useful concrete things’ like an example of ‘credit credit debt’,
    and  not  ’abstract things’ like ‘loans and advances’. We also want
    to have concise things, we don’t want to fill in every column of
    every IL table for a credit card debt, we want to fill in just the
    columns that are needed for credit card debt.  This class helps us
    find the list of useful concrete things, shown in a concise way.

    Ultimately to show these in a concise way we want to show the input
    layer columns. But to make this easier we will show this first as a
    set of LDM entities, and then for each entity find the relevant input 
    layer columns.

    Note that  useful concrete things are usually represented as leaf nodes
    in the LDM (like credit card debt or other loans).
    If the leaf is a simple subtype directly under a root of a type hierarchy ,
    then the related set of LDM entities is just this leaf and each of its ancestors.

    Sometimes however a useful concrete thing is a pair or set of leaf entities,
    this is common when a SQLDeveloper hierarchy uses disjoint subtyping or
    has other identifying (owning/composition) relationships. An example of
    this is under Instrument where we might have a useful thing defined by
    its product type and its role (both of which are also hierarchies)…
    so for example a credit card debt acting in the role of an 
    on -balance sheet instrument, or a basic ‘other-loan’ acting in the
    role of collateral.

    This class provides a concise representation of all the useful concrete
    things in BIRD, and where these are defined as a pair/set of leaf
    entities it shows the related entities for the pair/set. Occasionally
    it will show things allowed by the LDM which are not useful (e.g. a
    loan acting in the role of an off-balance sheet item), these non-useful
    combinations are a known issue in the LDM, and there is an attempt to
    reduce them or at least document which combinations do not really
    relate to real world concepts.

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
            # here we work out the possible combinations of entities for the discriminotors
            # these are stored as a set of columns, these reresent a grid that can be read
            # row by row, where 1 row represents one possible combination of entities.
            for discriminator in discriminators:
                SubtypeExploder.enrich_discrimitor_columns(self, context, discriminator,columns)
               
            if len(columns) > 0:
                count = 0
                for each_entity in columns[0]:
                    entity_combination = []
                    
                    for column in columns:
                        # here we get one row form the grid, 
                        # e.g getting the nth item from each columns 
                        entity_combination.append(column[count])
                    
                    # for our row of csv data we have already worked out the value for sme columns
                    # we are going to copy/clone that row and add further information to it
                    # not that this is a recursive process.
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
        # For this base case of empty lists we just populate a list
        if len(list_of_lists) == 0:
            for entity in entities:
                discrimitors_entity_list.append(entity)
        # here we work out the possible combinations of entities for the discriminotors
        # these are stored as a set of columns, these reresent a grid that can be read
        # row by row, where 1 row represents one possible combination of entities.
        else:
            # first_one = True
            for the_list in list_of_lists:
                list_copy = the_list.copy()
                first_one = True
                for entity in entities:
                    for item in list_copy:
                        if not(first_one):
                            the_list.append(item)
                        discrimitors_entity_list.append(entity)
                    first_one = False      
               
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
        get any containment references, these represent identifying relationships
        in the LDM, and also arcs for disjoint subtyping.
        '''
        reference_list = []
        for ref in entity.eStructuralFeatures:
            if isinstance(ref,ELReference):
                if ref.containment:
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
        class_list = []
        # for disjoint subtypes we always delegate to an abstract 
        # class and provide concrete subclasses for each disjoint subclasses
        # we dont need to consider the abstract subclass in the processing.
        # For basic identifying (composition/containment) relationships
        # the entity may not be abstract, and so we should include it 
        # in processing and its subtypes
        if not (entity_type.eAbstract):
            class_list.append(entity_type)
            
        # get the subclasses, for disjoint subtyping there will
        # only be direct subtypes. we should consider that for 
        # identifying relationships there might be subtypes that have
        # subtypes, so we may need to amend this code to 
        # deal with that situation.
        for eclassifier in context.input_tables_package.eClassifiers:
            if isinstance(eclassifier,ELClass):
                if len(eclassifier.eSuperTypes) > 0:
                    if eclassifier.eSuperTypes[0] == entity_type:
                        class_list.append(eclassifier)
        return class_list
        
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
        
        