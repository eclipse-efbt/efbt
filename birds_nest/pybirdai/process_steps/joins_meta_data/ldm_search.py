# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
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
from django.apps import apps

class ELDMSearch:
    """
    A class for searching and retrieving related entities in a Django model hierarchy.
    """

    def get_all_related_entities(self, context, entity, memoization_parents_from_disjoint_subtyping_eldm_search):
        """
        Retrieve all related entities for a given entity.

        Args:
            context: The context in which the search is performed.
            entity: The entity for which to find related entities.

        Returns:
            list: A list of related entities.
        """
        entities = set()
        ELDMSearch._get_superclasses_and_associated_entities(
            context, entity, entities, 0, 4, memoization_parents_from_disjoint_subtyping_eldm_search
        )
        ELDMSearch._get_associated_entities(context, entity, entities, 0, 4, memoization_parents_from_disjoint_subtyping_eldm_search)
        return list(entities)

    def _get_associated_entities(
        context, entity, entities, link_count, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
    ):
        """
        Recursively retrieve associated entities through foreign key relationships.

        Args:
            context: The context in which the search is performed.
            entity: The entity for which to find associated entities.
            entities (set): The set to store found entities.
            link_count (int): The current depth of the recursive search.
            link_limit (int): The maximum depth of the recursive search.
        """
        if link_count >= link_limit:
            return

        for feature in entity._meta.get_fields():
            if (
                isinstance(feature, ForeignKey)
                and not feature.name.startswith("parent_")
                and not feature.name.endswith("_delegate")
            ):
                related_model = feature.related_model
                entities.add(related_model)

                ELDMSearch._get_superclasses_and_associated_entities(
                    context, related_model, entities, link_count + 1, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
                )
                ELDMSearch._get_associated_entities(
                    context, related_model, entities, link_count + 1, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
                )

    def _get_superclasses_and_associated_entities(
        context, entity, entities, link_count, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
    ):
        """
        Recursively retrieve superclasses and their associated entities.

        Args:
            context: The context in which the search is performed.
            entity: The entity for which to find superclasses and associated entities.
            entities (set): The set to store found entities.
            link_count (int): The current depth of the recursive search.
            link_limit (int): The maximum depth of the recursive search.
        """

        parents_from_disjoint_subtyping = ELDMSearch._get_parents_from_disjoint_subtyping(entity, memoization_parents_from_disjoint_subtyping_eldm_search)
        for parent in parents_from_disjoint_subtyping:
            if parent not in entities:
                entities.add(parent)
                ELDMSearch._get_associated_entities(
                context, parent, entities, link_count, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
                )
                ELDMSearch._get_superclasses_and_associated_entities(
                    context, parent, entities, link_count, link_limit, memoization_parents_from_disjoint_subtyping_eldm_search
                )

        parent_list = entity._meta.get_parent_list()

        if parent_list:
            super_entity = parent_list[0]
            if super_entity not in entities:
                entities.add(super_entity)
                ELDMSearch._get_associated_entities(
                    context, super_entity, entities, link_count, link_limit,memoization_parents_from_disjoint_subtyping_eldm_search
                )
                ELDMSearch._get_superclasses_and_associated_entities(
                    context, super_entity, entities, link_count, link_limit,memoization_parents_from_disjoint_subtyping_eldm_search
                )

        #for feature in entity._meta.get_fields():
        #    if (
        #        isinstance(feature, ForeignKey)
        #        and feature.name.startswith("parent_")
        #        and feature.name not in ("parent_member_id", "parent_axis_ordinate_id")
        #    ):
        #        super_entity = feature.related_model
        #        if super_entity not in entities:
        #            entities.append(super_entity)
        #        ELDMSearch._get_associated_entities(
        #            context, super_entity, entities, link_count, link_limit
        #        )
        #       ELDMSearch._get_superclasses_and_associated_entities(
        #            context, super_entity, entities, link_count, link_limit
        #        )


    def _get_parents_from_disjoint_subtyping(entity, memoization_parents_from_disjoint_subtyping_eldm_search):
        """
        Retrieve parents from disjoint subtyping relationships.

        Args:
            entity: The entity for which to find parents from disjoint subtyping.

        Returns:
            list: A list of parents from disjoint subtyping.
        """
        print(f"Getting parents from disjoint subtyping for {entity.__name__}")
        # get a link tot the full django model, then loop trhought all tables inthe model
        list_of_results = []
        if hash(entity) in memoization_parents_from_disjoint_subtyping_eldm_search:
            return memoization_parents_from_disjoint_subtyping_eldm_search[hash(entity)]
        for model in apps.get_models():
            if model._meta.app_label == 'pybirdai':
                #print(f"{model._meta.app_label}  -> {model.__name__}")
                for feature in model._meta.get_fields():
                    if (
                        isinstance(feature, ForeignKey)
                        and feature.name.endswith("_delegate")
                    ) and feature.name[0:len(feature.name)-9] == entity.__name__:
                        list_of_results.append(model)
        memoization_parents_from_disjoint_subtyping_eldm_search[hash(entity)] = list_of_results
        return list_of_results
