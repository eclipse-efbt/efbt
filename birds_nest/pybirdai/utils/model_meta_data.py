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
import django
from django.apps import AppConfig
from django.db.models.fields.related import ForeignKey


class ModelMetaDataUtils(AppConfig):
    path = "/workspaces/efbt/bird/birdseed_creator/birds_nest"

    def ready(self):
        from django.apps import apps

        model_list = apps.get_models()

        for model in model_list:
            print(f"{model._meta.app_label}  -> {model.__name__}")
            print("superclasses are ")
            print(model._meta.get_parent_list())
            field_list = model._meta.get_fields()
            for field in field_list:
                print(field.name)
                print("class")
                print(field.__class__)
                if isinstance(field, ForeignKey):
                    pass
                try:
                    print(field.db_column)
                    print(field.db_comment)
                except:
                    pass

    def print_meta_data(self):
        pass

    def print_table_meta_data(self):
        pass


if __name__ == "__main__":
    django.setup()
    ModelMetaDataUtils("pybirdai", "birds_nest").ready()
