import csv
import os

class LinkProcessor:
    """
    Processes link files based on mappings from a source file,
    replacing IDs and codes using standard CSV handling.
    """
    def __init__(self, mapping_file_path, cube_link_file_path, csi_link_file_path, output_cube_link_path, output_csi_link_path):
        """
        Initializes the LinkProcessor with input and output file paths.

        Args:
            mapping_file_path (str): Path to the mapping CSV file.
            cube_link_file_path (str): Path to the input cube link CSV file.
            csi_link_file_path (str): Path to the input CSI link CSV file.
            output_cube_link_path (str): Path for the processed cube link output CSV file.
            output_csi_link_path (str): Path for the processed CSI link output CSV file.
        """
        self.mapping_file_path = mapping_file_path
        self.cube_link_file_path = cube_link_file_path
        self.csi_link_file_path = csi_link_file_path
        self.output_cube_link_path = output_cube_link_path
        self.output_csi_link_path = output_csi_link_path

        self.mapping_data = []
        self.cube_link_data = []
        self.csi_link_data = []

        self.mapping_dict = {}
        self.mapping_dict_tuple = {}
        self.mapping_cube = {}

    def read_csv_data(self, file_path):
        """
        Reads CSV data into a list of dictionaries, using the first row as headers.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            list[dict]: A list of dictionaries representing the CSV data.
                        Returns an empty list if the file is empty or not found.
        """
        data = []
        try:
            # Use utf-8 encoding as it's common. newline='' is crucial for csv module.
            with open(file_path, mode='r', newline='', encoding='utf-8') as infile:
                reader = csv.DictReader(infile)
                data = list(reader) # Read all rows into a list
        except FileNotFoundError:
            print(f"Error: Input file not found at {file_path}")
        except Exception as e:
            print(f"Error reading file {file_path}: {e}")
        return data

    def read_csv_headers(self, file_path):
        """
        Reads only the header row from a CSV file.

        Args:
            file_path (str): Path to the CSV file.

        Returns:
            list[str]: A list of header strings. Returns an empty list if the file
                       is empty or not found, or an error occurs.
        """
        headers = []
        try:
            with open(file_path, mode='r', newline='', encoding='utf-8') as infile:
                reader = csv.reader(infile)
                try:
                    headers = next(reader)
                except StopIteration:
                    pass # File is empty
        except FileNotFoundError:
             print(f"Error: Header file not found at {file_path}")
        except Exception as e:
            print(f"Error reading headers from {file_path}: {e}")
        return headers

    def write_csv_data(self, file_path, data, fieldnames):
        """
        Writes a list of dictionaries to a CSV file.

        Args:
            file_path (str): Path for the output CSV file.
            data (list[dict]): A list of dictionaries to write.
            fieldnames (list[str]): A list of strings defining the CSV headers and
                                    the order of fields for the dictionaries.
        """
        if not fieldnames:
            print(f"Warning: No fieldnames provided for writing to {file_path}. Skipping write.")
            return

        # Ensure the output directory exists
        output_dir = os.path.dirname(file_path)
        if output_dir and not os.path.exists(output_dir):
            try:
                os.makedirs(output_dir)
            except OSError as e:
                print(f"Error creating output directory {output_dir}: {e}")
                return

        try:
            with open(file_path, mode='w', newline='', encoding='utf-8') as outfile:
                writer = csv.DictWriter(outfile, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(data)
        except Exception as e:
            print(f"Error writing data to {file_path}: {e}")


    def build_mappings(self):
        """
        Builds mapping dictionaries (mapping_dict, mapping_dict_tuple, mapping_cube)
        from the loaded mapping data.
        Skips rows where required mapping columns are missing or empty.
        """
        if not self.mapping_data:
             print("Warning: Mapping data not loaded. Cannot build mappings.")
             return

        # Expected columns in mapping file
        required_cols = ["Logical_Object_Name", "Entity_Name", "Relational_Object_Name", "Table_Name"]

        for row in self.mapping_data:
            # Get values for required columns, defaulting to None if key is missing
            values = [row.get(col) for col in required_cols]

            # Check if all required columns exist and have non-empty string values
            # Original pandas used .dropna() on selected columns. DictReader gives empty strings.
            # Treat None or empty string as missing data.
            if not all(values) or any(value == '' for value in values):
                # Skip row if any required mapping column is missing or empty
                # print(f"Skipping mapping row due to missing/empty data: {row}") # Optional debug
                continue

            logical_object_name = values[0]
            entity_name = values[1]
            relational_object_name = values[2]
            table_name = values[3]

            # Apply string replacement (like pandas .str.replace(" ","_"))
            logical_object_name_processed = logical_object_name.replace(" ", "_")
            entity_name_processed = entity_name.replace(" ", "_")

            # Build mapping_dict
            # Key: Table_Name__Relational_Object_Name__
            # Value: Entity_Name__Logical_Object_Name
            self.mapping_dict[f"{table_name}__{relational_object_name}__"] = f"{entity_name_processed}__{logical_object_name_processed}"

            # Build mapping_dict_tuple
            # Key: (Table_Name, Relational_Object_Name)
            # Value: (Entity_Name, Logical_Object_Name)
            self.mapping_dict_tuple[(table_name, relational_object_name)] = (entity_name_processed, logical_object_name_processed)

            # Build mapping_cube
            # Key: Table_Name
            # Value: Entity_Name (processed)
            self.mapping_cube[table_name] = entity_name_processed

    def process_cube_links(self):
        """
        Processes the cube link data based on cube mappings (Table_Name -> Entity_Name).
        """
        if not self.cube_link_data:
            print("Warning: Cube link data not loaded. Cannot process cube links.")
            return []
        if not self.mapping_cube:
             print("Warning: Mapping cube data not built. Cannot process cube links.")
             return []

        new_cube_links = []
        # Assuming required column is 'PRIMARY_CUBE_ID'
        required_col = 'PRIMARY_CUBE_ID'
        replace_cols = ['CUBE_LINK_ID',"NAME","DESCRIPTION"]

        for row in self.cube_link_data:
            primary_cube_id = row.get(required_col,"")

            eil_cube = self.mapping_cube.get(primary_cube_id,"")

            if eil_cube:
                # Create a new row (dictionary) for modification to avoid changing original data
                new_row = row.copy()
                # Replace the value in the 'PRIMARY_CUBE_ID' column with the mapped value
                new_row[required_col] = eil_cube
                for col in replace_cols:
                    new_row[col] = new_row[col].replace(primary_cube_id, eil_cube)
                new_cube_links.append(new_row)
            # else:
                 # Skip the row if a mapping for PRIMARY_CUBE_ID is not found in mapping_cube
                 # (This matches the original code's implicit behaviour)

        return new_cube_links

    def process_csi_links(self):
        """
        Processes the CSI link data based on item mappings
        (Table_Name__Relational_Object_Name__ -> Entity_Name__Logical_Object_Name
         and (Table_Name, Relational_Object_Name) -> (Entity_Name, Logical_Object_Name)).
        """
        if not self.csi_link_data:
            print("Warning: CSI link data not loaded. Cannot process CSI links.")
            return []
        if not self.mapping_dict or not self.mapping_dict_tuple:
             print("Warning: Mapping dictionary or tuple mapping not built. Cannot process CSI links.")
             return []

        new_csi_links = []
        # Assuming required headers are 'PRIMARY_CUBE_VARIABLE_CODE', 'CUBE_STRUCTURE_ITEM_LINK_ID', 'CUBE_LINK_ID'
        required_cols = ['PRIMARY_CUBE_VARIABLE_CODE', 'CUBE_STRUCTURE_ITEM_LINK_ID', 'CUBE_LINK_ID']

        for row in self.csi_link_data:
            # Check if all required columns exist and have non-empty values
            values = [row.get(col) for col in required_cols]
            if not all(values) or any(value == '' for value in values):
                # Skip row if essential columns are missing or empty
                # print(f"Skipping CSI link row due to missing/empty required data: {row}") # Optional debug
                continue

            primary_cube_variable_code = values[0]
            csi_link_id = values[1]
            cube_link_id = values[2]

            # --- Process PRIMARY_CUBE_VARIABLE_CODE mapping ---
            row_code_parts = primary_cube_variable_code.rsplit("__", 1)
            row_code_prefix = row_code_parts[0] + "__" # Original adds "__" back

            eil_csi = self.mapping_dict.get(row_code_prefix)

            # --- Process CUBE_STRUCTURE_ITEM_LINK_ID and CUBE_LINK_ID mapping ---
            csil_split = csi_link_id.split(":")
            # Expecting format like "prefix:old_cube:mid1:mid2:variable" (5 parts)
            if len(csil_split) != 5:
                 # Skip if CUBE_STRUCTURE_ITEM_LINK_ID format is unexpected
                 # print(f"Skipping CSI link row due to unexpected CUBE_STRUCTURE_ITEM_LINK_ID format: {csi_link_id}") # Optional debug
                 continue

            # Extract old_cube and variable from the split string
            # Use try-except just in case, though len check should prevent IndexError
            old_cube = csil_split[1]
            variable = csil_split[4]

            mapping_tuple_result = self.mapping_dict_tuple.get((old_cube, variable))

            # --- Apply mappings if both parts are found ---
            # Original logic implicitly requires both mappings to exist (it checks eil_csi,
            # then accesses the result of mapping_dict_tuple.get which caused a None error).
            # We explicitly check for both here.
            if eil_csi and mapping_tuple_result:
                new_row = row.copy() # Create a new row dict for modification

                # Apply mapping for PRIMARY_CUBE_VARIABLE_CODE
                # Replace only the prefix part with the new mapped value
                new_row['PRIMARY_CUBE_VARIABLE_CODE'] = primary_cube_variable_code.replace(row_code_prefix, eil_csi)

                # Apply mapping for CUBE_STRUCTURE_ITEM_LINK_ID
                (new_cube, new_variable) = mapping_tuple_result
                new_row['CUBE_STRUCTURE_ITEM_LINK_ID'] = ":".join([
                    csil_split[0], new_cube, csil_split[2], csil_split[3], new_variable
                ])

                # Apply mapping for CUBE_LINK_ID
                # Replace the old_cube part with new_cube
                # Original code did a simple replace. Assuming old_cube substring exists in CUBE_LINK_ID.
                # A safer approach might involve checking if old_cube is a part of CUBE_LINK_ID first.
                # Sticking to original logic's direct replace.
                new_row['CUBE_LINK_ID'] = cube_link_id.replace(old_cube, new_cube)

                new_csi_links.append(new_row)
            # else:
                 # Skip the row if either the variable code mapping (eil_csi)
                 # or the cube/variable tuple mapping (mapping_tuple_result) is not found.

        return new_csi_links


    def process_all_files(self):
        """
        Orchestrates the loading of data, building mappings, processing links,
        and writing output files.
        """
        print("Starting link processing...")

        # Read headers first to use for writing output files
        # This ensures output files have correct headers even if processing results in no rows
        cube_link_headers = self.read_csv_headers(self.cube_link_file_path)
        csi_link_headers = self.read_csv_headers(self.csi_link_file_path)

        if not cube_link_headers:
            print(f"Warning: Could not read headers from {self.cube_link_file_path}. Output for cube links might be skipped or incorrect.")
        if not csi_link_headers:
            print(f"Warning: Could not read headers from {self.csi_link_file_path}. Output for CSI links might be skipped or incorrect.")


        # Load input data using DictReader
        print(f"Loading mapping data from {self.mapping_file_path}...")
        self.mapping_data = self.read_csv_data(self.mapping_file_path)
        print(f"Loading cube link data from {self.cube_link_file_path}...")
        self.cube_link_data = self.read_csv_data(self.cube_link_file_path)
        print(f"Loading CSI link data from {self.csi_link_file_path}...")
        self.csi_link_data = self.read_csv_data(self.csi_link_file_path)

        # Build mappings from mapping data
        print("Building mappings...")
        self.build_mappings()
        print(f"Built {len(self.mapping_dict)} variable code mappings.")
        print(f"Built {len(self.mapping_dict_tuple)} cube/variable mappings.")
        print(f"Built {len(self.mapping_cube)} cube mappings.")


        # Process link data using the built mappings
        print("Processing cube links...")
        processed_cube_links = self.process_cube_links()
        print(f"Processed {len(processed_cube_links)} cube link rows.")

        print("Processing CSI links...")
        processed_csi_links = self.process_csi_links()
        print(f"Processed {len(processed_csi_links)} CSI link rows.")

        # Write output files, using the headers read earlier
        print(f"Writing processed cube links to {self.output_cube_link_path}...")
        self.write_csv_data(self.output_cube_link_path, processed_cube_links, cube_link_headers)

        print(f"Writing processed CSI links to {self.output_csi_link_path}...")
        self.write_csv_data(self.output_csi_link_path, processed_csi_links, csi_link_headers)

        print("Link processing complete.")

    @classmethod
    def handle(cls):
        # --- Execution part: Instantiate the class and run the process ---

        # Define file paths based on the original script
        MAPPING_FILE = "resources/ldm/mappings.csv"
        CUBE_LINK_INPUT_FILE = "resources/joins_export/export_filecube_link.csv"
        CSI_LINK_INPUT_FILE = "resources/joins_export/export_filecube_structure_item_link.csv"
        CUBE_LINK_OUTPUT_FILE = "resources/joins_export/new_cube_links_df.csv" # Naming matches original output
        CSI_LINK_OUTPUT_FILE = "resources/joins_export/new_csi_links_df.csv"   # Naming matches original output

        # Instantiate the processor
        processor = cls(
            mapping_file_path=MAPPING_FILE,
            cube_link_file_path=CUBE_LINK_INPUT_FILE,
            csi_link_file_path=CSI_LINK_INPUT_FILE,
            output_cube_link_path=CUBE_LINK_OUTPUT_FILE,
            output_csi_link_path=CSI_LINK_OUTPUT_FILE
        )

        # Run the processing method
        processor.process_all_files()
