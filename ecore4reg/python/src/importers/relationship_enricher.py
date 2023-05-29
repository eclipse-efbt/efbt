from ecore4reg import ELAttribute, ELClass, ELReference

class RelationshipEnricher(object):
    '''
    Enrich a model which can from the SDD website,
    with the relationship information from SQLDeveloper
    '''
    def enrich(self, context):
        '''
        Enrich a model which can from the SDD website,
        with the relationship information from SQLDeveloper
        '''
        for fk_tuple in context.foreign_key_tuple:
            source_class_name= fk_tuple[0]
            reference_name = fk_tuple[1]
            target_class_name= fk_tuple[2]
            upper_bound = fk_tuple[3]
            lower_bound = fk_tuple[4]
            
            print("source_class_name")
            print(source_class_name)
            print("reference_name")
            print(reference_name)
            print("target_class_name")
            print(target_class_name)
            print("upper_bound")
            print(upper_bound)
            print("lower_bound")
            print(lower_bound)
            
            source_class = RelationshipEnricher.get_class_from_package(self, source_class_name, context.output_layer_entities_package)
            target_class = RelationshipEnricher.get_class_from_package(self, target_class_name, context.output_layer_entities_package)
            e_reference = ELReference()
            e_reference.name = reference_name
            e_reference.eType = target_class
            e_reference.upperBound = upper_bound
            e_reference.lowerBound = lower_bound
            e_reference.containment = False
            source_class.eStructuralFeatures.append(e_reference)
            
    def get_class_from_package(self, class_name, package): 
        for classifier in package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == class_name:
                    return classifier
            
            
            #create the pks
            
            #create the relationships
        
        