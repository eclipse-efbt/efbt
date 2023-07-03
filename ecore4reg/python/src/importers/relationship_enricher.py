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
            
            if context.use_codes:
                source_class = RelationshipEnricher.get_class_from_package(self, "BIRD_" + source_class_name + "_EIL", context.input_tables_package)
                target_class = RelationshipEnricher.get_class_from_package(self, "BIRD_" + target_class_name + "_EIL", context.input_tables_package)
            else:
                source_class = RelationshipEnricher.get_class_from_package(self, source_class_name, context.input_tables_package)
                target_class = RelationshipEnricher.get_class_from_package(self, target_class_name, context.input_tables_package)
            
            
            e_reference = ELReference()
            if RelationshipEnricher.relationshipExistsOnClass(self,source_class,reference_name):
                e_reference.name = reference_name + "2"
            else:
                e_reference.name = reference_name
            e_reference.eType = target_class
            e_reference.upperBound = upper_bound
            e_reference.lowerBound = lower_bound
            e_reference.containment = False
            source_class.eStructuralFeatures.append(e_reference)
            
    def relationshipExistsOnClass(self,source_class,reference_name):
        for feature in source_class.eStructuralFeatures:
            if feature.name == reference_name:
                return True
            
        return False
    def get_class_from_package(self, class_name, package): 
        for classifier in package.eClassifiers:
            if isinstance(classifier, ELClass):
                if classifier.name == class_name:
                    return classifier
            
            
            #create the pks
            
            #create the relationships
        
        