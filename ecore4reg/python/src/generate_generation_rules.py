from importers.context import Context
from importers.import_input_layer_to_ecore4reg import InputLayerImport
from importers.import_output_layer_to_ecore4reg import ROLImport
from importers.import_finrep_vtl import ImportFinrepVTL
from importers.persist_to_file import PersistToFile
from importers.ecore4reg_to_ecore_converter import Ecore4regToEcoreConverter
from importers.main_catagory_finder import MainCatagoryFinder
from importers.generation_rule_creator import GenerationRuleCreator

if __name__ == '__main__':
    context = Context()

    context.persist_to_ecore4reg = True
    context.use_subdomains_in_rol = False
    context.load_eil_from_website  = True
    context.input_from_website = True
    context.persist_vtl_comments = False
    context.set_up_csv_indexes()
    context.file_directory = 'C:\\Users\\LENOVO\\git\\efbt_ecore4reg\\ecore4reg\\python\\resources_website'
    context.output_directory = 'C:\\Users\\LENOVO\\git\\efbt_ecore4reg\\ecore4reg\\python\\results_website'

    InputLayerImport().do_import(context)
    ROLImport().doImport(context)
    
    # sdd_context.file_directory = '/workspaces/efbt/ecore4reg/python/resources'
    # sdd_context.output_directory = '/workspaces/efbt/ecore4reg/python/results'
    # sdd_context.input_from_website = True
    # sdd_context.set_up_csv_indexes()
    # ImportSDD().import_sdd(sdd_context)

    #Ecore4regToEcoreConverter().convert_packages_in_context(context)
    MainCatagoryFinder().create_report_to_main_catogory_maps(context)
    GenerationRuleCreator().generate_generation_rules(context)
    persister = PersistToFile()
    # persister.save_analysis_model_as_json_files(sdd_context)

    
    persister.save_model_as_ecore_file(context)
    persister.save_model_as_ecore4reg_file(context)
    persister.save_model_as_json_files(context)