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
from regdna import ELAttribute, ELClass, ELReference



class LDMSearch(object):
    '''
    Given a LDM Entity, this class will search for all the attributes related to it
    and return a list of all the attributes that are related to the entity.
    This can mean that the attribute is directly related to the entity, 
    or it is related to an entity that is related to the entity.
    '''

    def get_all_related_entities(self, context, entity):
        '''
        Given a context and an entity, this function will return a list of all the attributes
        that are related to the entity.
        '''
        entities = []
        LDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, entity, entities, 0, 4)
        LDMSearch.get_associated_entities(self, context, entity, entities, 0, 4)
        #LDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, entity, entities, 0, 4)
        return entities

    def get_associated_entities(self, context, entity, entities, link_count, link_limit):
        '''
        Given a context and an entity, this function will return a list of all the attributes
        that are related to the entity.
        '''
        link_count = link_count + 1
        if link_count < link_limit:
            for feature in entity.eStructuralFeatures:
                if isinstance(feature, ELReference):
                    if not(feature.name.startswith("parent_")) and not(feature.name.endswith("_delegate")):
                        # get the class that the reference points to and all subclasses of that class
                        if not(feature.eType in entities):
                            entities.append(feature.eType)

                        #LDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, feature.eType, entities,  link_count, link_limit)
                        LDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, feature.eType, entities,  link_count, link_limit)
                        LDMSearch.get_associated_entities(self, context, feature.eType, entities,  link_count, link_limit)

    def get_superclasses_of_entity_and_their_associated_entities(self, context, entity, entities, link_count, link_limit):
        '''
        Given a context and an entity, this function will return a list of all the superclasses
        that are related to the entity.
        '''
        if len(entity.eSuperTypes) > 0:
            super_entity = entity.eSuperTypes[0]
            if not(super_entity in entities):
                if not (super_entity.eAbstract):
                    entities.append(super_entity)
            LDMSearch.get_associated_entities(self,context, super_entity, entities,  link_count, link_limit)
            LDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, super_entity, entities, link_count, link_limit)


        for feature in entity.eStructuralFeatures:
            if isinstance(feature, ELReference):
                if feature.name.startswith("parent_"):
                    super_entity = feature.eType
                    if not(super_entity in entities):
                        entities.append(super_entity)
                    LDMSearch.get_associated_entities(self, context, super_entity, entities, link_count, link_limit)
                    LDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, super_entity, entities, link_count, link_limit)



    def get_subclasses_of_entity_and_their_associated_entities(self, context, entity,  entities, link_count, link_limit):
        '''
        Given a context and an entity, this function will return a list of all the subclasses
        of the entity. this will include the dubclass of any delegated classes.
        '''
        for elclass in context.input_tables_package.eClassifiers:
            if isinstance(elclass, ELClass):
                if len(elclass.eSuperTypes) > 0:
                    if elclass.eSuperTypes[0] == entity:
                        sub_type_entity    = elclass
                        if not(sub_type_entity in entities):
                            entities.append(sub_type_entity)
                        LDMSearch.get_associated_entities(self,context, sub_type_entity, entities,  link_count, link_limit)
                        LDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, sub_type_entity, entities, link_count, link_limit)

        for feature in entity.eStructuralFeatures:
            if isinstance(feature, ELReference):
                if feature.name.endswith("_delegate"):
                        LDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, feature.eType, entities, link_count, link_limit)