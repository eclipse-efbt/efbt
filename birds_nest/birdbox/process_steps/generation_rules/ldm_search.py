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

from django.db.models.fields.related import ForeignKey

class ELDMSearch(object):
    '''
    Given a ELDM Entity, this class will search for all the attributes related to it
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
        ELDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, entity, entities, 0, 4)
        ELDMSearch.get_associated_entities(self, context, entity, entities, 0, 4)
        #ELDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, entity, entities, 0, 4)
        return entities

    def get_associated_entities(self, context, entity, entities, link_count, link_limit):
        '''
        Given a context and an entity, this function will return a list of all the attributes
        that are related to the entity.
        '''
        link_count = link_count + 1
        if link_count < link_limit:
            field_list = entity._meta.get_fields()
            for feature in field_list:
                if isinstance(feature, ForeignKey):
                    if not(feature.name.startswith("parent_")) and not(feature.name.endswith("_delegate")) :
                        # get the class that the reference points to and all subclasses of that class
                        if not(feature.related_model in entities):
                            entities.append(feature.related_model)

                        #ELDMSearch.get_subclasses_of_entity_and_their_associated_entities(self, context, feature.eType, entities,  link_count, link_limit)
                        ELDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, feature.related_model, entities,  link_count, link_limit)
                        ELDMSearch.get_associated_entities(self, context, feature.related_model, entities,  link_count, link_limit)

    def get_superclasses_of_entity_and_their_associated_entities(self, context, entity, entities, link_count, link_limit):
        '''
        Given a context and an entity, this function will return a list of all the superclasses
        that are related to the entity.
        '''

        if len((entity._meta.get_parent_list())) > 0:
            super_entity = entity._meta.get_parent_list()[0]
            if not(super_entity in entities):
                #if not (super_entity.eAbstract):  SHOULD WE KEEP THIS LINE?
                entities.append(super_entity)
            ELDMSearch.get_associated_entities(self,context, super_entity, entities,  link_count, link_limit)
            ELDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, super_entity, entities, link_count, link_limit)

        field_list = entity._meta.get_fields()
        for feature in field_list:
            if isinstance(feature, ForeignKey):
                if feature.name.startswith("parent_") and not(feature.name == "parent_member_id") and not(feature.name == "parent_axis_ordinate_id"):
                    super_entity = feature.related_model
                    if not(super_entity in entities):
                        entities.append(super_entity)
                    ELDMSearch.get_associated_entities(self, context, super_entity, entities, link_count, link_limit)
                    ELDMSearch.get_superclasses_of_entity_and_their_associated_entities(self, context, super_entity, entities, link_count, link_limit)



    