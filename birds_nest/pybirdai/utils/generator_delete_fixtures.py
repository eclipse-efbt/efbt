"""
Copyright 2025 Arfa Digital Consulting

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
"""

# Import required libraries
import os

import re
from logger_factory import return_logger, Path
logger = return_logger(str(Path(__file__).resolve()).rsplit("/",1)[-1])

def process_sql_file(file_path: str):
    """
    Process SQL file and convert insert/update statements to deletes
    Args:
        file_path (str): Path to the SQL file to process
    """
    try:
        # Read the SQL file
        with open(file_path) as f:
            data = f.readlines()
            logger.debug("Successfully read SQL file")
    except Exception as e:
        logger.error(f"Error reading SQL file: {str(e)}")
        raise

    def convert_into_info(line:str):
        """
        Convert INSERT statement to DELETE statement
        Args:
            line (str): INSERT SQL statement
        Returns:
            str: Converted DELETE statement
        """
        try:
            # Parse the INSERT statement
            table_columns, values = line.strip("INSERT INTO ").rstrip(";\n").split(" VALUES")
            values = eval(values.replace("NULL","None"))
            table_name, columns = table_columns.rstrip(")").split("(")
            columns = columns.split(",")
            rowid_column = columns[0]
            rowid_value = values[0]
            # Create DELETE statement
            return f"DELETE FROM {table_name} WHERE {rowid_column} = {rowid_value};"
        except Exception as e:
            logger.error(f"Error converting line: {str(e)}")
            raise

    # Process each line in the file
    result_data = list()
    for line in data:
        if not line.strip(): continue
        try:
            match line:
                case _ if "UPDATE" in line:
                    # Handle UPDATE statements by setting values to NULL
                    result_data.append(re.sub(
                       r"(?==).*(?>WHERE)",
                       "=NULL WHERE",
                       line
                   ).replace("\n\n","\n"))
                    logger.debug("Processed UPDATE statement")
                case _ if "INSERT INTO" in line:
                    # Convert INSERT statements to DELETE statements
                    result_data.append(convert_into_info(line))
                    logger.debug("Processed INSERT statement")
                case _:
                    pass
        except Exception as e:
            logger.error(f"Error processing line: {line}")
            logger.error(f"Error details: {str(e)}")
            break

    # Write results to output file
    output_file = file_path.replace("inserts", "deletes")
    with open(output_file, "w") as f:
        for line in result_data:
            f.write(line + "\n")
