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
        file.write("from agilebird.utils.orchestration import Orchestration\n")
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            print(f"report_id: {report_id}")
            file.write("from " + report_id  + "_logic import  " + report_id  + "_Logic\n")
            file.write("\nclass " + report_id + ":\n")
            file.write("\tunionOfLayers = None #  " + report_id + "_UnionItem  unionOfLayers\n")
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
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '(self) -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '(self) -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '(self) -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '(self) -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\treturn self.unionOfLayers.' + variable.variable_id + '()\n')
                file.write('\n')
            file.write('\n')
            file.write("class " +report_id + "_table :\n" )
            file.write("\tunionOfLayersTable = None # " + report_id + "_UnionTable\n" )
            file.write("\t" + report_id + "s = None #" + report_id + "[]\n" )
            file.write("\tdef  calc_" + report_id + "s(self) -> list[" + report_id + "] :\n" )
            file.write("\t\titems = [] # " + report_id + "[]\n" )
            file.write("\t\tfor item in self.unionOfLayersTable." + report_id + "_UnionItems:\n" )
            file.write("\t\t\tnewItem = " + report_id + "()\n" )
            file.write("\t\t\tnewItem.unionOfLayers = item\n" )
            file.write("\t\t\titems.append(newItem)\n" )
            file.write("\t\treturn items\n" )
            file.write("\tdef init(self):\n" )
            file.write("\t\tOrchestration.init(self)\n" )
            file.write("\t\tself." + report_id + "s.append(self.calc_" + report_id + "s())\n" )
            file.write("\t\treturn None\n" )
            file.write('\n')

    def create_slice_classes( sdd_context):
        for report_id, cube_links in sdd_context.cube_link_to_foreign_cube_map.items():
            file = open(sdd_context.output_directory + os.sep + 'generated_python' + os.sep +  report_id + '_logic.py', "a",  encoding='utf-8')   
            file.write("from agilebird.ldm_models import *\n")
            file.write("from . import output_tables\n")
            file.write("from agilebird.utils.orchestration import Orchestration\n")
        
            file.write("\nclass " + report_id + "_UnionItem:\n")
            file.write("\tbase = None #" + report_id + "_Base\n")
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
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '(self) -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '(self) -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '(self) -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '(self) -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '(self) -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\treturn self.base.' + variable.variable_id + '()')
                file.write('\n')


            file.write("\nclass " + report_id + "_Base:\n")
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
                    file.write('\tdef ' + variable.variable_id + '() -> str:\n')
                elif domain == 'Integer':
                    file.write('\tdef ' + variable.variable_id + '() -> int:\n')
                elif domain == 'Date':
                    file.write('\tdef ' + variable.variable_id + '() -> datetime:\n')
                elif domain == 'Float':
                    file.write('\tdef ' + variable.variable_id + '() -> float:\n')
                elif domain == 'Boolean':
                    file.write('\tdef ' + variable.variable_id + '() -> bool:\n')
                else:
                    file.write('\tdef ' + variable.variable_id + '() -> str:\n')
                    file.write('\t\t\'\'\' return string from ' + domain + ' enumeration \'\'\'\n')

                file.write('\t\tpass')
                file.write('\n')


            file.write("\nclass " + report_id + "_UnionTable :\n")
            file.write("\t" + report_id + "_UnionItems = None # " +  report_id + "_UnionItem []\n" )
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\t" + cube_link.join_identifier.replace(' ','_') + "_Table = None # " +  cube_link.join_identifier.replace(' ','_') + "\n") 
            file.write("\tdef calc_" + report_id + "_UnionItems() -> list[" + report_id + "_UnionItem] :\n")
            file.write("\t\titems = [] # " + report_id + "_UnionItem []\n")
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\t\tfor item in " + cube_link.join_identifier.replace(' ','_') + "_Table." + cube_link.join_identifier.replace(' ','_') + "s:\n")
                        file.write("\t\t\tnewItem = " + report_id + "_UnionItem()\n")
                        file.write("\t\t\tnewItem.base = item\n")
                        file.write("\t\t\titems.append(newItem)\n")
            file.write("\t\treturn items\n")
            file.write("\n")

            file.write("\tdef init(self):\n")
            file.write("\t\tOrchestration.init(self)\n")
            file.write("\t\tself." + report_id + "_UnionItems.extend(self.calc_" + report_id + "_UnionItems())\n")
            file.write("\t\treturn None\n")
            file.write("\n")					 
			
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\nclass " + cube_link.join_identifier.replace(' ','_') + "(" + report_id + "_Base):\n")
                        
                        cube_structure_item_links = []
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            print(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")
                        
                        for cube_structure_item_link in cube_structure_item_links:
                            file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + " = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                        for cube_structure_item_link in cube_structure_item_links:
                            file.write("\tdef " + cube_structure_item_link.foreign_cube_variable_code.variable_id.variable_id + "(self):\n")
                            file.write("\t\treturn " +  cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "." + cube_structure_item_link.primary_cube_variable_code.variable_id.variable_id + "\n")

		
            for join_for_report_id, cube_links in sdd_context.cube_link_to_join_for_report_id_map.items():                
                for cube_link in cube_links:                   
                    the_report_id = cube_link.foreign_cube_id.cube_id
                    if the_report_id == report_id:
                        file.write("\nclass " + cube_link.join_identifier.replace(' ','_') + "_Table:\n" )
                        cube_structure_item_links = []  
                        try:
                            cube_structure_item_links = sdd_context.cube_structure_item_link_to_cube_link_map[cube_link.cube_link_id]
                        except KeyError:
                            print(f"No cube structure item links for cube_link: {cube_link.cube_link_id}")
                        for cube_structure_item_link in cube_structure_item_links:
                            file.write("\t" + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id  + "_Table = None # " + cube_structure_item_link.cube_link_id.primary_cube_id.cube_id + "\n")
                        file.write("\t" + cube_link.join_identifier.replace(' ','_') + "s = []# " + cube_link.join_identifier.replace(' ','_') + "[]\n")
                        file.write("\tdef calc_" + cube_link.join_identifier.replace(' ','_') + "s(self) :\n")
                        file.write("\t\titems = [] # " + cube_link.join_identifier.replace(' ','_') + "[\n")
                        file.write("\t\t# Join up any refered tables that you need to join\n")
                        file.write("\t\t# loop through the main table\n")
                        file.write("\t\t# set any references you want to on the new Item so that it can refer to themin operations\n")
                        file.write("\t\treturn items\n")
                        file.write("\tdef init(self):\n")
                        file.write("\t\tOrchestration.init(self)\n")
                        file.write("\t\tself." + cube_link.join_identifier.replace(' ','_') + "s.extend(self.calc_" + cube_link.join_identifier.replace(' ','_') + "s())\n")
                        file.write("\t\treturn None\n")
                        file.write("\n")

