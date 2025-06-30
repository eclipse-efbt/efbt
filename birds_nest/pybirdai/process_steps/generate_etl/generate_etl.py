import csv
import os
import shutil
from .simple_context import SimpleContext

import math

class GenerateETL(object):

    def __init__(self):
        self.entity_columns_by_schema = {}
        self.slice_configurations_by_schema = {}
        
    def create_etl_guide(self, csv_dir, output_file, context): 
        self.process_all_tables()

    def process_table(self, table_name, source_schema):
        
        self._initialize_storage_for_schema_and_table(source_schema, table_name)
        
        os.makedirs('etl_results', exist_ok=True)

        leaf_entities_hierarchy = self._load_entity_hierarchy(table_name, leafs_only=True)
        all_entities_hierarchy = self._load_entity_hierarchy(table_name, leafs_only=False)
        entity_column_mappings = self._load_entity_column_mappings(table_name, leaf_entities_hierarchy)
        
        leaf_combinations = self._extract_leaf_combinations(table_name, leaf_entities_hierarchy)
        
        self._process_entity_columns(
            source_schema,
            all_entities_hierarchy,
            entity_column_mappings)
        
        self._generate_slice_configurations(
            table_name,
            leaf_combinations,
            leaf_entities_hierarchy,
            source_schema)
        
        self._write_consolidated_slices_file(table_name, source_schema)
        
        self._create_filter_functions(table_name, source_schema, leaf_entities_hierarchy)

    def _initialize_storage_for_schema_and_table(self, source_schema, table_name):
        if source_schema not in self.entity_columns_by_schema:
            self.entity_columns_by_schema[source_schema] = {}
        if source_schema not in self.slice_configurations_by_schema:
            self.slice_configurations_by_schema[source_schema] = {}
        if table_name not in self.slice_configurations_by_schema[source_schema]:
            self.slice_configurations_by_schema[source_schema][table_name] = {}

    def _load_entity_hierarchy(self, table_name, leafs_only):
        return self.get_entity_to_hierarchy_dict(table_name, leafs_only)

    def _load_entity_column_mappings(self, table_name, entity_hierarchy):
        return self.get_entity_to_columns_dict(table_name, entity_hierarchy)

    def _extract_leaf_combinations(self, table_name, entity_hierarchy):
        return self.get_leaf_list_list(table_name, entity_hierarchy)

    def _process_entity_columns(self, source_schema, all_entities_hierarchy, entity_column_mappings):
        return self.generate_part_for_each_entity(
            source_schema, all_entities_hierarchy, entity_column_mappings, {})

    def _generate_slice_configurations(self, table_name, leaf_combinations, leaf_entities_hierarchy, source_schema):
        return self.create_slice_per_leaf_list(
            table_name, leaf_combinations, leaf_entities_hierarchy, source_schema)

    def _write_consolidated_slices_file(self, table_name, source_schema):
        return self.write_final_slices_file("", table_name, source_schema)

    def _create_filter_functions(self, table_name, source_schema, leaf_entities_hierarchy):
        output_directory = f'etl_results'
        return self.create_filter_function_per_leaf(output_directory, leaf_entities_hierarchy, source_schema)
    
    
    def generate(self, table_name, source_schema):
        return self.process_table(table_name, source_schema)
    
    def generate_part_for_each_entity(self, 
                                              source_schema,
                                              entity_to_hierarchy_dict_all_nodes,
                                              entity_to_columns_dict,
                                              populated_mappings_dict):
        
        if source_schema not in self.entity_columns_by_schema:
            self.entity_columns_by_schema[source_schema] = {}
            
        for entity, column_list in entity_to_columns_dict.items():
            csv_columns = []
            for column in column_list:
                parts = column.split('.')
                col = ""
                if len(parts) == 1:
                    col = parts[0]
                elif len(parts) == 2:
                    col = parts[1]

                if not(self._is_column_inherited_from_parent(
                                            col,
                                            entity,
                                            entity_to_hierarchy_dict_all_nodes,
                                            entity_to_columns_dict)):
                    csv_columns.append(col)
            
            
            csv_content = ','.join(csv_columns)
            
            
            self.entity_columns_by_schema[source_schema][entity] = {
                'csv': csv_content
            }

    def create_slice_per_leaf_list(self,
                                           table,
                                           leaf_list_list,
                                           entity_to_hierarchy_dict,
                                           source_schema):   
        used_names_count = {}
        
        
        all_entity_columns = set()
        for entity, entity_data in self.entity_columns_by_schema.get(source_schema, {}).items():
            if entity_data['csv']:
                columns = [col.strip() for col in entity_data['csv'].split(',') if col.strip()]
                all_entity_columns.update(columns)
        
        sorted_column_list = sorted(list(all_entity_columns))
        
        for leaf_combination in leaf_list_list:
            if len(leaf_combination) > 0:
                parent_entities = []
                slice_name = self._generate_slice_name(table, leaf_combination, used_names_count)
                
                
                for leaf_entity in leaf_combination:
                    hierarchy = entity_to_hierarchy_dict[leaf_entity]
                    hierarchy_parts = hierarchy.split('.')
                    for entity_part in hierarchy_parts:
                        if not self._is_internal_entity(entity_part):
                            if entity_part not in parent_entities:
                                parent_entities.append(entity_part)
                
                
                header_content = self._generate_csv_header(sorted_column_list)
                
                
                csv_filter = self._generate_csv_filter(leaf_combination)
                
                
                relevant_columns = self._collect_relevant_columns(source_schema, parent_entities)
                
                
                csv_row = self._build_csv_row(csv_filter, sorted_column_list, relevant_columns)
                
                
                self.slice_configurations_by_schema[source_schema][table][slice_name] = {
                    'csv': csv_row,
                    'header': header_content
                }

    def _generate_slice_name(self, table, leaf_combination, used_names_count):
        return self.get_leaf_combination_name_under_150_chars(table, leaf_combination, used_names_count)

    def _is_internal_entity(self, entity_part):
        return (entity_part.endswith('delegate') or
                entity_part.endswith('_disc') or
                entity_part.endswith('_association') or
                entity_part.endswith('_composition'))

    def _generate_csv_header(self, sorted_column_list):
        header_content = "filter,"
        for column in sorted_column_list:
            header_content += column + ","
        
        if sorted_column_list:
            header_content += sorted_column_list[-1]
        header_content += "\n"
        return header_content

    def _generate_csv_filter(self, leaf_combination):
        csv_filter = ""
        for leaf_entity in leaf_combination:
            csv_filter += " is_" + leaf_entity + "() and"
        csv_filter += " TRUE"
        return csv_filter

    def _collect_relevant_columns(self, source_schema, parent_entities):
        relevant_columns = set()
        for parent_entity in parent_entities:
            entity_data = self.entity_columns_by_schema.get(source_schema, {}).get(parent_entity, {})
            if entity_data.get('csv'):
                columns = [col.strip() for col in entity_data['csv'].split(',') if col.strip()]
                relevant_columns.update(columns)
        return relevant_columns

    def _build_csv_row(self, csv_filter, sorted_column_list, relevant_columns):
        csv_row = csv_filter + ","
        for column in sorted_column_list:
            if column in relevant_columns:
                csv_row += "X,"
            else:
                csv_row += ","
        return csv_row

    def _is_column_inherited_from_parent(self, col, the_entity, entity_to_hierarchy_dict, entity_to_columns_dict):
        return self.super_type_contains_column(col, the_entity, entity_to_hierarchy_dict, entity_to_columns_dict)

    def write_final_slices_file(self, output_directory, table, source_schema):
        
        target_file = f'etl_results{os.sep}{table}_all_slices.csv'
        
        
        slice_configurations = self.slice_configurations_by_schema.get(source_schema, {}).get(table, {})
        header = None
        
        
        for slice_data in slice_configurations.values():
            if slice_data.get('header'):
                header = slice_data['header']
                break
        
        with open(target_file, 'w', encoding='utf-8') as f:
            
            if header:
                f.write(header)
            
            
            for slice_data in slice_configurations.values():
                if slice_data.get('csv'):
                    
                    f.write(slice_data['csv'] + '\n')

    
    def get_leaf_list_list(self, table, entity_to_hierarchy_dict_leafs_only):
        input_file_location = 'results' + os.sep + 'csv' + os.sep + '' + \
                                table + \
                                '_discrimitor_combinations_summary.csv'
        header_skipped = False
        leaf_list_list = []
        with open(input_file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                row_is_valid = True
                
                if (not header_skipped):
                    header_skipped = True
                else:
                    leaf_list = []
                    for item in row:
                        if not(item == '') and not(item is None):
                            if item in entity_to_hierarchy_dict_leafs_only.\
                                                                    keys():
                                leaf_list.append(item)
                            else:
                                
                                
                                
                                
                                
                                has_leaf = \
                                    self.non_leaf_has_corresponding_leaf_in_row(
                                            item,
                                            row,
                                            entity_to_hierarchy_dict_leafs_only)

                    if row_is_valid:
                        leaf_list_list.append(leaf_list)
        return leaf_list_list

    def non_leaf_has_corresponding_leaf_in_row(
                                        self,
                                        item,
                                        row,
                                        entity_to_hierarchy_dict_leafs_only):
        return_value = False
        for entity, hierarchy in entity_to_hierarchy_dict_leafs_only.items():
            hierarchy_parts = hierarchy.split('.')
            if item in hierarchy_parts:
                if entity in row:
                    return_value = True

        return return_value
    
    def get_entity_to_columns_dict(self, table, entity_to_hierarchy_dict):
        input_file_location = 'results' + os.sep + 'csv' + os.sep + '' + table + \
                    '_discrimitor_combinations_full.csv'
        column_index_to_full_ldm_column_name_list = []
        column_index_to_il_column_name_list = []
        subset_column_index_to_full_ldm_column_name_list = []
        subset_column_index_to_il_column_name_list = []
        entity_to_columns_dict = {}

        header_skipped = False
        header2_skipped = False
        with open(input_file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                    column_index_to_full_ldm_column_name_list = row
                elif(not header2_skipped):
                    column_index_to_il_column_name_list = row
                    header2_skipped = True
                else:
                    pass

            count = 0
            for il_item in column_index_to_il_column_name_list:

                ldm_item = column_index_to_full_ldm_column_name_list[count]
                subset_column_index_to_full_ldm_column_name_list.append(
                                                                    ldm_item)
                subset_column_index_to_il_column_name_list.append(il_item)
               
                count = count + 1   

            count = 0
            for il_item in subset_column_index_to_il_column_name_list:
                ldm_item = \
                    subset_column_index_to_full_ldm_column_name_list[count]

                table_name = self.get_table_name_from_ldm_column(ldm_item)
                il_column_list = []
                try:
                    il_column_list = entity_to_columns_dict[table_name]
                except KeyError:
                    entity_to_columns_dict[table_name] = il_column_list

                if not(il_item == 'UNKNOWN'):    
                    
                    if not(il_item in il_column_list):
                        il_column_list.append(il_item)
                count = count + 1

            for item in entity_to_hierarchy_dict.keys():
                if not (item in entity_to_columns_dict.keys()) \
                            and not (item is None):
                    entity_to_columns_dict[item] = []  

        return entity_to_columns_dict  
        
    def super_type_contains_column(self, col,
                                   the_entity,
                                   entity_to_hierarchy_dict,
                                   entity_to_columns_dict):
        super_type_contains_column = False
        
        for entity, hierarchy in entity_to_hierarchy_dict.items():
            if the_entity == entity:
                hierarchy_parts = hierarchy.split('.')
                
                for leaf in hierarchy_parts:
                    if leaf == entity:
                        break
                    else:
                        if not (leaf.endswith('delegate') or 
                                leaf.endswith('_disc') or 
                                leaf.endswith('_association') or 
                                leaf.endswith('_composition')):
                            try:
                                columns = entity_to_columns_dict[leaf]
                                for column in columns:
                                    main_column_part = ""
                                    parts = column.split('.')                                        
                                    if len(parts) == 1:
                                        main_column_part = parts[0]
                                    elif len(parts) == 2:
                                        main_column_part = parts[1]
                                    if main_column_part == col:
                                        super_type_contains_column = True
                            except:
                                pass

        return super_type_contains_column
        
    def get_entity_to_hierarchy_dict(self, table, leafs_only):
        input_file_location = 'results' + os.sep + 'csv' + os.sep + '' + table + \
                '_discrimitor_combinations_summary.csv'
        column_index_to_full_ldm_column_name_list = []
        entity_to_hierarchy_dict = {}
        items_to_pop = []
        header_skipped = False
        header2_skipped = False
        with open(input_file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                if (not header_skipped):
                    header_skipped = True
                    column_index_to_full_ldm_column_name_list = row
                elif(not header2_skipped):
                    pass
                    header2_skipped = True
                else:
                    counter = 0
                    for item in row:
                        if not(item == ""):
                            entity_to_hierarchy_dict[item] = \
                                column_index_to_full_ldm_column_name_list\
                                    [counter] + "." + item
                                                                     
                            hierarchy_parts = \
                                column_index_to_full_ldm_column_name_list\
                                    [counter].split('.')
                            
                            for leaf in hierarchy_parts:
                                if not (leaf.endswith('delegate') or 
                                        leaf.endswith('_disc') or 
                                        leaf.endswith('_association') or 
                                        leaf.endswith('_composition')) \
                                            and not(leaf == item):
                                    items_to_pop.append(leaf)
                        counter = counter + 1    

        if leafs_only:
            for item in items_to_pop:
                try:
                    entity_to_hierarchy_dict.pop(item)
                except:
                    pass

                    
        return entity_to_hierarchy_dict
    
    def get_leaf_combination_name_under_64_chars(self,
                                                 table,
                                                 leaf_list,
                                                 used_names_count):
        table_length = len(table)
        remaining = 61 - table_length
        num_of_leafs = len(leaf_list)
        target_length_per_leaf = math.floor(remaining / num_of_leafs)
        leaf_combination = ""
        leafs_left = num_of_leafs
        for leaf in leaf_list:
            used_leaf_name = leaf
            if len(leaf) > target_length_per_leaf:
                used_leaf_name = used_leaf_name[0:7] + \
                used_leaf_name[len(leaf) - target_length_per_leaf + 9:len(leaf)]
                remaining = remaining - len(used_leaf_name)
            else:
                leafs_left = leafs_left - 1
                remaining = remaining - len(used_leaf_name)
                if (leafs_left > 0):
                    target_length_per_leaf = \
                        math.floor(remaining / leafs_left)

            leaf_combination = leaf_combination + ":" + used_leaf_name
        try:
            used_count = used_names_count[leaf_combination]
            used_names_count[leaf_combination] = \
                used_names_count[leaf_combination] + 1
            leaf_combination = leaf_combination + str(used_count)
        except KeyError:
            used_names_count[leaf_combination] = 1

        return leaf_combination
    
    def get_leaf_combination_name_under_150_chars(self,
                                                 table,
                                                 leaf_list,
                                                 used_names_count):
        table_length = len(table)
        remaining = 130 - table_length
        num_of_leafs = len(leaf_list)
        target_length_per_leaf = math.floor(remaining / num_of_leafs)
        leaf_combination = ""
        leafs_left = num_of_leafs
        for leaf in leaf_list:
            used_leaf_name = leaf
            if len(leaf) > target_length_per_leaf:
                used_leaf_name = used_leaf_name[0:7] + \
                used_leaf_name[len(leaf) - target_length_per_leaf + 9:len(leaf)]
                remaining = remaining - len(used_leaf_name)
            else:
                leafs_left = leafs_left - 1
                remaining = remaining - len(used_leaf_name)
                if (leafs_left > 0):
                    target_length_per_leaf = \
                        math.floor(remaining / leafs_left)

            leaf_combination = leaf_combination + ":" + used_leaf_name
        try:
            used_count = used_names_count[leaf_combination]
            used_names_count[leaf_combination] = \
                used_names_count[leaf_combination] + 1
            leaf_combination = leaf_combination + str(used_count)
        except KeyError:
            used_names_count[leaf_combination] = 1

        return leaf_combination

    def create_filter_function_per_leaf(self,
                                        output_directory,
                                        entity_to_hierarchy_dict,
                                        source_schema):            
        filter_definitions_file =\
            open(output_directory + os.sep + 'python_functions' + os.sep + '' +
                 'python_filter_definitions' + os.sep + 'filter_definitions.py',
                 "a", encoding='utf-8')
        
        for entity, hierarchy in entity_to_hierarchy_dict.items():
           
            if entity is not None:
                filter_definitions_file.write(
                    "def is_" + entity + "():\n")
                filter_definitions_file.write("\treturn False\n\n")
                
             
                    
        filter_definitions_file.close()
       
        

    def get_table_name_from_ldm_column(self, ldm_item):
        qualified_name_list = ldm_item.split('.')
        table_name = qualified_name_list[len(qualified_name_list) - 2]

        return table_name
    
    def process_all_tables(self):
        
        context = SimpleContext()
        all_tables_to_process = context.get_all_related_tables()
        
        
        self._setup_output_directories()
        
        
        source_schema = 'all'
        self._create_schema_directories(source_schema)

        
        for table_name in all_tables_to_process:
            self._create_mapping_template(table_name, source_schema)

        
        for table_name in all_tables_to_process:
            self.process_table(table_name, source_schema)

    def generate_all(self):
        return self.process_all_tables()

    def _setup_output_directories(self):
        
        if os.path.exists('etl_results'):
            shutil.rmtree('etl_results')
        os.mkdir('etl_results')
        os.mkdir('etl_results' + os.sep + 'mapping_templates')
        os.mkdir('etl_results' + os.sep + 'python_functions')
        os.mkdir('etl_results' + os.sep + 'python_functions' + os.sep + 'python_filter_definitions')

    def _create_schema_directories(self, source_schema):
        os.mkdir('etl_results' + os.sep + 'mapping_templates' + os.sep + source_schema)

    def _create_mapping_template(self, table_name, source_schema):
        return self.generate_mapping_template(table_name, source_schema)

        
       

    def generate_mapping_template(self, table, source_schema):
        output_directory = source_schema + os.sep + table

        entity_to_hierarchy_dict_leafs_only = \
            self.get_entity_to_hierarchy_dict(table, True)
        entity_to_columns_dict = \
            self.get_entity_to_columns_dict(table,
                                           entity_to_hierarchy_dict_leafs_only)
        self.create_mapping_templates(table,
                                     'etl_results' + os.sep + 'mapping_templates' + os.sep + '' +
                                     source_schema, source_schema, entity_to_columns_dict)

    def create_mapping_templates(self,
                                 table,
                                 output_directory,
                                 source_schema,
                                 entity_to_columns_dict):
        
        file = open(output_directory + os.sep + 
                    source_schema + '_to_' + 
                    table + ".csv", "a", encoding='utf-8')
        for entity, column_list in entity_to_columns_dict.items():
            for column in column_list:
                file.write(entity + "," + column + ",\'?\',\n")

    def get_populated_mappings(self, source_schema, table):
        mappings_selection = 'source_mappings'
        
        file_location = mappings_selection + os.sep + \
                        source_schema + os.sep + source_schema + \
                        '_to_' + table + ".csv"
        mappings_dict = {}

        with open(file_location, encoding='utf-8') as csvfile:
            filereader = csv.reader(csvfile, delimiter=',', quotechar='"')
            for row in filereader:
                entity = row[0]
                column = row[1]
                mapped_val = row[2]
                mappings_dict[entity + column] = mapped_val

        return mappings_dict
            
if __name__ == "__main__":
   gen = GenerateETL() 
   gen.generate_all()