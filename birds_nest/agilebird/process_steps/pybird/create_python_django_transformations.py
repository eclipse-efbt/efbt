from agilebird.utils.utils import Utils
from agilebird.sdd_models import *
import os


class CreatePythonTransformations:

    @staticmethod
    def create_python_transformations(context, sdd_context):
        '''
        Read in the transformation meta data and create python classes
        '''
        CreatePythonTransformations.create_output_classes( sdd_context)
        CreatePythonTransformations.create_slice_classes(sdd_context)
        # get all the cube_links for a report

    def create_output_classes(  sdd_context):
        
         #get all the cubes_structure_items for that cube and make a related Pythin class.
        file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  'output_tables.py', "a",  encoding='utf-8') 
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            print(f"report_id: {report_id}")
            file.write("class " + report_id + ":")
            file.write("from " + report_id  + "_logic import  " + report_id  + "_Logic")
            file.write("unionOfLayers = None #  " + report_id + "_UnionItem  unionOfLayers ")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.rol_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                print(f"No cube structure items for report_id: {report_id}")
            for cube_structure_item in cube_structure_items:
                print(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id

                domain = variable.domain_id.domain_id
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '() -> int:')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '() -> datetime:')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '() -> float:')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '() -> bool:')
                else:
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                    file.write('\'\'\' return string from ' + domain + ' enumeration \'\'\'')

                file.write('unionOfLayers.' + variable.variable_id + '()')
                file.write('\n')
            file.write('\n')
            file.write("class " +report_id + "_table :" )
            file.write("\t " + report_id + "_UnionTable  unionOfLayersTable # " + report_id + "_UnionTable")
            file.write("\t " + report_id + "s #" + report_id + "[]")
            file.write("\t def  calc_" + report_id + "s() -> " + report_id + "[] :")
            file.write("\t\t items = [] # " + report_id + "[]")
            file.write("\t\t for item in unionOfLayersTable." + report_id + "_UnionItems:")
            file.write("\t\t\t newItem = " + report_id + "()")
            file.write("\t\t\t newItem.unionOfLayers = item")
            file.write("\t\t\t items.append(newItem)")
            file.write("\t\t return items")
            file.write("\t def init(self):")
            file.write("\t\t orchestration.init(self)")
            file.write("\t\t self." + report_id + "s.append(calc_" + report_id + "s())")
            file.write("\t\t\ return None")
            file.write('\n')



    def create_slice_classes( sdd_context):
        
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  report_id + '_logic.py', "a",  encoding='utf-8')   

            file.write("from agilebird.ldm_models import *\n")
            file.write("from . import output_tables\n")
            file.write("class " + report_id + "_UnionItem:\n")
            file.write("\t base = None #" + report_id + "_Base\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.rol_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                print(f"No cube structure items for report_id: {report_id}")
            for cube_structure_item in cube_structure_items:
                print(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id

                domain = variable.domain_id.domain_id
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '() -> int:')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '() -> datetime:')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '() -> float:')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '() -> bool:')
                else:
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                    file.write('\'\'\' return string from ' + domain + ' enumeration \'\'\'')

                file.write('base.' + variable.variable_id + '()')
                file.write('\n')


            file.write("class " + report_id + "_Base:\n")
            cube_structure_items = []
            try:
                cube_structure_items = sdd_context.rol_cube_structure_item_dictionary[report_id+ '_cube_structure']
            except KeyError:
                print(f"No cube structure items for report_id: {report_id}")
            for cube_structure_item in cube_structure_items:
                print(f"cube_structure_item: {cube_structure_item}")
                variable = cube_structure_item.variable_id

                domain = variable.domain_id.domain_id
                if domain == 'String':
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '() -> int:')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '() -> datetime:')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '() -> float:')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '() -> bool:')
                else:
                    file.write('\tdef ' + variable.variable_id + '() -> str:')
                    file.write('\'\'\' return string from ' + domain + ' enumeration \'\'\'')

                file.write('\t\tpass')
                file.write('\n')


            file.write("class " + report_id + "_UnionTable :")
            file.write("\t" + report_id + "_UnionItems # " +  report_id + "_UnionItem []" )
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\t" + cube_link.join_identifier + "_Table # " +  cube_link.join_identifier ) 
            file.write("def calc_" + report_id + "_UnionItems() -> " + report_id + "_UnionItem [] :")
            file.write("items = [] # " + report_id + "_UnionItem []")
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\t for item in " + cube_link.join_identifier + "_Table." + cube_link.join_identifier + "s:")
                        file.write("\t\t newItem = " + report_id + "_UnionItem()")
                        file.write("\t\t newItem.base = item")
                        file.write("\t\t items.append(newItem)")
            file.write("\t return items")
            file.write("\n")

            file.write("def init(self):")
            file.write("\t orchestration.init(this)")
            file.write("\t self." + report_id + "_UnionItems.extend(calc_" + report_id + "_UnionItems())")
            file.write("\t return None")
            file.write("\n")					 
			
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("class " + cube_link.join_identifier + "extends " + report_id + "_Base:")
                        
                        for cube_structure_item_link in sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]:
                            file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + " = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)
                        for cube_structure_item_link in sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]:
                            file.write("\t def " + cube_structure_item_link.foreign_cube_variable_code.variable_id + "() :")
                            file.write("\t\t return " +  cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "." + cube_structure_item_link.primary_cube_variable_code.variable_id)

		
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("class " + cube_link.join_identifier + "_Table: " )
                        for cube_structure_item_link in sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]:
                            file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + "_Table = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id)
                        file.write("\t" + cube_link.join_identifier + "s = []# " + cube_link.join_identifier + "[]")
                        file.write("\t def calc_" + cube_link.join_identifier + "s() :")
                        file.write("\t\t items = [] # " + cube_link.join_identifier + "[]")
                        file.write("\t\t# Join up any refered tables that you need to join")
                        file.write("\t\t# loop through the main table")
                        file.write("\t\t # set any references you want to on the new Item so that it can refer to themin operations")
                        file.write("\t\t return items")
                        file.write("\t def init(self):")
                        file.write("\t\t orchestration.init(swlf)")
                        file.write("\t\t self." + cube_link.join_identifier + "s.extend(calc_" + cube_link.join_identifier + "s())")
                        file.write("\t\t return None")
                        file.write("\n")

