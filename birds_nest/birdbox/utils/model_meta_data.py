import django
from django.apps import AppConfig

class ModelMetaDataUtils(AppConfig):
    
    path = '/workspaces/efbt/bird/birdseed_creator/birds_nest'

    def ready(self):
        
        from django.apps import apps
        model_list = apps.get_models()

        for model in model_list:
            if model.__name__ == 'VG_SNSTVTY':
                import pdb;pdb.set_trace()
            print(f"{model._meta.app_label}  -> {model.__name__}")
            print("superclasses are ")
            print(model._meta.get_parent_list())
            field_list = model._meta.get_fields()
            for field in field_list:
                print(field.name)
                try:
                    print(field.db_column)
                except:
                    pass

    def print_meta_data(self):
        pass

    def print_table_meta_data(self):
        pass

if __name__ == '__main__':
    django.setup()
    ModelMetaDataUtils('birdbox','birds_nest').ready()
