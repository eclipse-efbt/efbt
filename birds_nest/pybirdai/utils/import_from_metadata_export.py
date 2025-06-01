import csv
import zipfile
import io
import os
import glob
from django.db import transaction
from pybirdai import bird_meta_data_model

class CSVDataImporter:
    def __init__(self):
        self.model_map = {}
        self.valid_table_names = set()
        self._build_model_map()

    def _build_model_map(self):
        """Build mapping of table names to model classes"""
        import inspect
        from django.db import models

        for name, obj in inspect.getmembers(bird_meta_data_model):
            if inspect.isclass(obj) and issubclass(obj, models.Model) and obj != models.Model:
                self.valid_table_names.add(obj._meta.db_table)
                self.model_map[obj._meta.db_table] = obj

    def _get_table_name_from_csv_filename(self, filename):
        """Convert CSV filename back to table name"""
        base_name = filename.replace('.csv', '')
        if base_name.startswith('bird_'):
            return f"pybirdai_{base_name.replace('bird_', '')}"
        else:
            return f"pybirdai_{base_name}"

    def _parse_csv_content(self, csv_content):
        """Parse CSV content and return headers and rows"""
        csv_reader = csv.reader(io.StringIO(csv_content))
        headers = next(csv_reader)  # First row is headers
        rows = list(csv_reader)
        return headers, rows

    def _get_field_mapping(self, model_class, csv_headers):
        """Map CSV headers to model fields"""
        field_mapping = {}
        model_fields = {field.name: field for field in model_class._meta.fields}

        for csv_header in csv_headers:
            field_name = csv_header.lower()
            if field_name in model_fields:
                field_mapping[csv_header] = model_fields[field_name]

        return field_mapping

    def _convert_value(self, field, value):
        """Convert CSV string value to appropriate Python type for the field"""
        from django.db import models

        if not value or value == '':
            return None

        if isinstance(field, models.IntegerField):
            return int(value)
        elif isinstance(field, models.FloatField):
            return float(value)
        elif isinstance(field, models.BooleanField):
            return value.lower() in ('true', '1', 'yes')
        elif isinstance(field, models.ForeignKey):
            return int(value) if value else None
        else:
            return value

    def import_csv_file(self, csv_filename, csv_content):
        """Import a single CSV file"""
        table_name = self._get_table_name_from_csv_filename(csv_filename)

        if table_name not in self.valid_table_names:
            raise ValueError(f"No model found for table: {table_name}")

        model_class = self.model_map[table_name]
        headers, rows = self._parse_csv_content(csv_content)
        field_mapping = self._get_field_mapping(model_class, headers)

        imported_objects = []

        with transaction.atomic():
            for row in rows:
                if not any(row):  # Skip empty rows
                    continue

                obj_data = {}
                for i, value in enumerate(row):
                    if i < len(headers):
                        header = headers[i]
                        if header in field_mapping:
                            field = field_mapping[header]
                            converted_value = self._convert_value(field, value)
                            obj_data[field.name] = converted_value

                if obj_data:
                    obj = model_class(**obj_data)
                    obj.save()
                    imported_objects.append(obj)

        return imported_objects

    def import_from_csv_string(self, csv_string, filename="data.csv"):
        """Import CSV data from a string"""
        try:
            imported_objects = self.import_csv_file(filename, csv_string)
            return {
                filename: {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            }
        except Exception as e:
            return {
                filename: {
                    'success': False,
                    'error': str(e)
                }
            }

    def import_from_path(self, path):
        """Import CSV files from either a zip file or a directory"""
        if os.path.isfile(path):
            if path.endswith('.zip'):
                return self.import_zip_file(path)
            elif path.endswith('.csv'):
                # Single CSV file
                with open(path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                filename = os.path.basename(path)
                imported_objects = self.import_csv_file(filename, csv_content)
                return {
                    filename: {
                        'success': True,
                        'imported_count': len(imported_objects),
                        'objects': imported_objects
                    }
                }
            else:
                raise ValueError(f"Unsupported file type: {path}")
        elif os.path.isdir(path):
            return self.import_folder(path)
        else:
            raise ValueError(f"Path does not exist: {path}")

    def import_folder(self, folder_path):
        """Import all CSV files from a folder"""
        results = {}

        # Find all CSV files in the folder
        csv_files = glob.glob(os.path.join(folder_path, "*.csv"))

        for csv_file_path in csv_files:
            filename = os.path.basename(csv_file_path)
            try:
                with open(csv_file_path, 'r', encoding='utf-8') as f:
                    csv_content = f.read()
                imported_objects = self.import_csv_file(filename, csv_content)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        return results

    def import_zip_file(self, zip_file_path_or_content):
        if isinstance(zip_file_path_or_content, str):
            # It's a file path
            with zipfile.ZipFile(zip_file_path_or_content, 'r') as zip_file:
                return self._process_zip_contents(zip_file)
        else:
            # It's file content (bytes)
            with zipfile.ZipFile(io.BytesIO(zip_file_path_or_content), 'r') as zip_file:
                return self._process_zip_contents(zip_file)

    def _process_zip_contents(self, zip_file):
        """Process contents of an opened zip file"""
        results = {}
        csv_files = [f for f in zip_file.namelist() if f.endswith('.csv')]

        for csv_filename in csv_files:
            try:
                csv_content = zip_file.read(csv_filename).decode('utf-8')
                imported_objects = self.import_csv_file(csv_filename, csv_content)
                results[csv_filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                results[csv_filename] = {
                    'success': False,
                    'error': str(e)
                }

        return results

    def import_from_csv_strings(self, csv_strings_list):
        """Import CSV data from a list of CSV strings"""
        results = {}

        for filename, csv_string in csv_strings_list.items():
            try:
                imported_objects = self.import_csv_file(filename, csv_string)
                results[filename] = {
                    'success': True,
                    'imported_count': len(imported_objects),
                    'objects': imported_objects
                }
            except Exception as e:
                results[filename] = {
                    'success': False,
                    'error': str(e)
                }

        return results


def import_bird_data_from_csv_export(path_or_content):
    """
    Convenience function to import bird data from a CSV export.

    Args:
        path_or_content: Either a file path (string) to a zip file, folder, or CSV file, or file content (bytes) for zip

    Returns:
        Dictionary with import results for each CSV file
    """
    importer = CSVDataImporter()

    # If it's bytes, treat as zip content
    if isinstance(path_or_content, bytes):
        return importer.import_zip_file(path_or_content)

    # If it's a string, use the general import_from_path method
    return importer.import_from_path(path_or_content)
