import requests
import os
import shutil
import zipfile
import subprocess
import argparse
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from collections import defaultdict
from itertools import islice
from functools import lru_cache
import traceback

from import_classes import CSVExporter
from id_resolver import EBAIDResolver
from logging_config import get_logger, log_process_start, log_process_end, log_data_summary, log_warning_with_context, log_error_with_context
import csv
import json

BASE_URL = "https://www.eba.europa.eu/sites/default/files/"
URL_TO__2_0 = BASE_URL+"2024-12/11b02b99-1486-4a54-815d-289558589773/dpm_2.0_release_4.0.zip"
URL_TO__1_0 = BASE_URL+"2024-12/330f4dba-be0d-4cdd-b0ed-b5a6b1fbc049/dpm_database_v4_0_20241218.zip"

def fetch_dpm_database(
    url_to_file : str = URL_TO__1_0
):
    """
    Fetch and extract DPM database from EBA website.

    Args:
        url_to_file (str): URL to the DPM database ZIP file
    """
    print(f"Fetching DPM database from: {url_to_file}")
    content = requests.get(url_to_file)
    print(f"Download status: {content.status_code}")

    if content.status_code != 200:
        raise Exception(f"Failed to download database: HTTP {content.status_code}")

    path_to_zip_file = "dpm_database.zip"
    directory_to_extract_to = "dpm_database"

    # Download and extract
    with open(path_to_zip_file,"wb") as f:
        f.write(content.content)

    with zipfile.ZipFile(path_to_zip_file, 'r') as zip_ref:
        zip_ref.extractall(directory_to_extract_to)

    # Find and copy the .accdb file
    db_files = [f for f in os.listdir(directory_to_extract_to) if f.endswith(".accdb")]
    if not db_files:
        raise Exception("No .accdb file found in downloaded archive")

    db_file = db_files[0]
    path_of_src_file = os.path.join(directory_to_extract_to, db_file)
    path_of_target_file = os.path.join(directory_to_extract_to, "dpm_database.accdb")
    shutil.copyfile(path_of_src_file, path_of_target_file)

    print(f"Database extracted and prepared: {path_of_target_file}")

def process_database(database_path="dpm_database/dpm_database.accdb"):
    """
    Process the DPM database to extract CSV files and template information.

    Args:
        database_path (str): Path to the DPM database file
    """
    print(f"Processing DPM database: {database_path}")
    result = subprocess.run(["bash","pybirdai/utils/dpm_importer/process.sh", database_path])
    if result.returncode != 0:
        print(f"Error processing database: {database_path}")
        sys.exit(1)
    print("Database processing completed successfully")

    # Extract template information
    print("Extracting template information...")
    extract_template_information()

def extract_template_information(csv_directory="target", output_file="template_information.json"):
    """
    Extract template information from DPM CSV files and create a comprehensive mapping with EBA ID resolution.

    Args:
        csv_directory (str): Directory containing DPM CSV files
        output_file (str): Output file for template information
    """
    # Use optimized data structures for better performance
    template_info = {
        'templates': {},
        'tables': {},
        'table_versions': {},  # Add version support
        'axes': {},           # Add axis support
        'ordinates': {},      # Add ordinate support
        'cells': {},          # Add cell support
        'cell_positions': {}, # Add cell position mapping
        'dimensions': {},
        'variables': {},
        'members': {},
        'domains': {},
        'relationships': dict(),  # Use defaultdict for auto-initialization
        'lookup_indices': {   # Pre-computed lookup indices for O(1) access
            'template_id_to_eba': {},        # template_id -> eba_id mapping
            'table_id_to_eba': {},           # table_id -> eba_id mapping
            'template_to_tables': dict(),  # template_eba_id -> set of table_eba_ids
            'domain_members': dict(),      # domain_code -> set of member_eba_ids
            'framework_templates': dict(), # framework -> set of template_eba_ids
        },
        'metadata': {
            'extraction_date': subprocess.run(['date'], capture_output=True, text=True).stdout.strip(),
            'source_directory': csv_directory,
            'eba_id_resolution': True,
            'optimization_enabled': True
        }
    }

    # Initialize EBA ID resolver
    resolver = EBAIDResolver()

    # Define key files to analyze for template information (order matters for reference data)
    key_files = [
        'ReportingFramework.csv',  # Load first for framework lookup
        'Domain.csv',              # Load second for domain lookup
        'Member.csv',              # Load third for member-domain mapping
        'Template.csv',
        'Table.csv',
        'TableVersion.csv',        # Add version support
        'Concept.csv',
        'Axis.csv',               # Add axis processing
        'AxisOrdinate.csv',       # Add ordinate processing
        'TableCell.csv',
        'CellPosition.csv',       # Add cell position mapping
        'Dimension.csv',
        'Hierarchy.csv'
    ]

    # First pass: Load reference data
    print("Loading reference data for EBA ID resolution...")
    framework_data = []
    domain_data = []
    member_data = []
    template_group_data = []
    template_group_template_data = []
    taxonomy_data = []
    taxonomy_table_version_data = []
    table_data = []
    table_version_data = []

    for csv_file in ['ReportingFramework.csv', 'Domain.csv', 'Member.csv', 'TemplateGroup.csv', 'TemplateGroupTemplate.csv', 'Taxonomy.csv', 'TaxonomyTableVersion.csv', 'Table.csv', 'TableVersion.csv']:
        csv_path = os.path.join(csv_directory, csv_file)
        if os.path.exists(csv_path):
            try:
                with open(csv_path, 'r', encoding='utf-8') as csvfile:
                    reader = csv.DictReader(csvfile)
                    data = list(reader)

                    if csv_file == 'ReportingFramework.csv':
                        framework_data = data
                        print(f"Loaded {len(data)} framework records")
                    elif csv_file == 'Domain.csv':
                        domain_data = data
                        print(f"Loaded {len(data)} domain records")
                    elif csv_file == 'Member.csv':
                        member_data = data
                        print(f"Loaded {len(data)} member records")
                    elif csv_file == 'TemplateGroup.csv':
                        template_group_data = data
                        print(f"Loaded {len(data)} template group records")
                    elif csv_file == 'TemplateGroupTemplate.csv':
                        template_group_template_data = data
                        print(f"Loaded {len(data)} template-group mapping records")
                    elif csv_file == 'Taxonomy.csv':
                        taxonomy_data = data
                        print(f"Loaded {len(data)} taxonomy records")
                    elif csv_file == 'TaxonomyTableVersion.csv':
                        taxonomy_table_version_data = data
                        print(f"Loaded {len(data)} taxonomy-table version mapping records")
                    elif csv_file == 'Table.csv':
                        table_data = data
                        print(f"Loaded {len(data)} table records")
                    elif csv_file == 'TableVersion.csv':
                        table_version_data = data
                        print(f"Loaded {len(data)} table version records")

            except Exception as e:
                print(f"Error loading reference data from {csv_file}: {str(e)}")
                template_info['metadata'][f'error_{csv_file}'] = str(e)

    # Initialize resolver with reference data including template group mappings and taxonomy versioning
    resolver.load_reference_data(framework_data, domain_data, member_data, template_group_data, template_group_template_data, taxonomy_data, taxonomy_table_version_data, table_data, table_version_data)

    # Second pass: Process main data files with EBA ID resolution (parallelized)
    print("Processing main data files in parallel...")
    process_files_parallel(key_files, csv_directory, template_info, resolver)

    # Create cross-references and relationships
    create_template_relationships(template_info, resolver)

    # Add resolution statistics
    template_info['metadata']['resolution_stats'] = resolver.get_resolution_stats()

    # Save template information with enhanced filename using streaming for large datasets
    base_name = output_file.replace('.json', '')
    enhanced_output_file = f"{base_name}_enhanced.json"
    output_path = os.path.join(csv_directory, enhanced_output_file)

    # Calculate dataset size to determine output strategy
    total_items = sum([
        len(template_info.get('templates', {})),
        len(template_info.get('tables', {})),
        len(template_info.get('variables', {})),
        len(template_info.get('members', {})),
        len(template_info.get('domains', {}))
    ])

    if total_items > 10000:  # Use streaming for large datasets
        print(f"Large dataset detected ({total_items} items), using streaming JSON output...")
        stream_json_output(template_info, output_path)
    else:
        # Use standard output for smaller datasets
        with open(output_path, 'w', encoding='utf-8') as f:
            json.dump(template_info, f, indent=2, ensure_ascii=False)

    print(f"Enhanced template information extracted to: {output_path}")
    print(f"Found {len(template_info['templates'])} templates, "
          f"{len(template_info['tables'])} tables, "
          f"{len(template_info.get('table_versions', {}))} table versions, "
          f"{len(template_info.get('axes', {}))} axes, "
          f"{len(template_info.get('ordinates', {}))} ordinates, "
          f"{len(template_info.get('cells', {}))} cells, "
          f"{len(template_info['variables'])} variables, "
          f"{len(template_info['members'])} members, "
          f"{len(template_info['domains'])} domains")

    # Print resolution summary
    resolver.print_resolution_summary()

def stream_json_output(template_info, output_path):
    """
    Stream JSON output for large datasets to avoid memory constraints.

    Args:
        template_info (dict): Template information dictionary
        output_path (str): Path to output JSON file
    """
    import io

    # Define the order of sections for consistent output
    sections = [
        'metadata', 'templates', 'tables', 'table_versions', 'axes',
        'ordinates', 'cells', 'cell_positions', 'dimensions', 'variables',
        'members', 'domains', 'relationships', 'lookup_indices'
    ]

    with open(output_path, 'w', encoding='utf-8') as f:
        f.write('{\n')

        section_count = 0
        total_sections = len([s for s in sections if s in template_info])

        for section in sections:
            if section not in template_info:
                continue

            section_count += 1
            f.write(f'  "{section}": ')

            # Stream large sections in chunks
            section_data = template_info[section]
            if isinstance(section_data, dict) and len(section_data) > 1000:
                stream_large_dict_section(f, section_data)
            else:
                # Use standard JSON encoding for smaller sections
                try:
                    json_str = json.dumps(section_data, indent=2, ensure_ascii=False)
                except Exception as _:
                    print(section_data)
                    traceback.print_exc()
                    sys.exit(1)
                # Indent the JSON to maintain structure
                indented_lines = []
                for i, line in enumerate(json_str.split('\n')):
                    if i == 0:
                        indented_lines.append(line)
                    else:
                        indented_lines.append('  ' + line)
                f.write('\n'.join(indented_lines))

            # Add comma if not the last section
            if section_count < total_sections:
                f.write(',')
            f.write('\n')

            # Force flush for large sections
            if section_count % 3 == 0:
                f.flush()

        f.write('}\n')

def stream_large_dict_section(file_handle, section_data):
    """
    Stream a large dictionary section in chunks to manage memory usage.

    Args:
        file_handle: Open file handle for writing
        section_data (dict): Dictionary data to stream
    """
    file_handle.write('{\n')

    items = list(section_data.items())
    total_items = len(items)
    chunk_size = 500  # Process in chunks of 500 items

    for chunk_start in range(0, total_items, chunk_size):
        chunk_end = min(chunk_start + chunk_size, total_items)
        chunk_items = items[chunk_start:chunk_end]

        for i, (key, value) in enumerate(chunk_items):
            # Calculate global position for comma handling
            global_pos = chunk_start + i

            # Write key-value pair with proper indentation
            key_json = json.dumps(key, ensure_ascii=False)
            value_json = json.dumps(value, indent=2, ensure_ascii=False)

            # Indent the value JSON to maintain structure
            indented_value_lines = []
            for j, line in enumerate(value_json.split('\n')):
                if j == 0:
                    indented_value_lines.append(line)
                else:
                    indented_value_lines.append('    ' + line)
            indented_value = '\n'.join(indented_value_lines)

            file_handle.write(f'    {key_json}: {indented_value}')

            # Add comma if not the last item
            if global_pos < total_items - 1:
                file_handle.write(',')
            file_handle.write('\n')

        # Flush every chunk
        file_handle.flush()

    file_handle.write('  }')

def process_files_parallel(key_files, csv_directory, template_info, resolver):
    """
    Process CSV files in parallel using ThreadPoolExecutor for improved performance.

    Args:
        key_files (list): List of CSV files to process
        csv_directory (str): Directory containing CSV files
        template_info (dict): Template information dictionary to update
        resolver (EBAIDResolver): EBA ID resolver instance
    """
    # Files to process in parallel (exclude reference data files)
    parallel_files = [f for f in key_files if f not in ['ReportingFramework.csv', 'Domain.csv', 'Member.csv']]

    # Prepare file data for parallel processing
    file_tasks = []
    for csv_file in parallel_files:
        csv_path = os.path.join(csv_directory, csv_file)
        if os.path.exists(csv_path):
            file_tasks.append((csv_path, csv_file))

    for csv_path, csv_file in file_tasks:
        process_template_file_isolated(csv_path, csv_file, resolver)


def process_template_file_isolated(csv_path, filename, resolver):
    """
    Process a single CSV file in isolation (for parallel processing).
    Returns file data instead of modifying global template_info.

    Args:
        csv_path (str): Path to CSV file
        filename (str): Name of the CSV file
        resolver (EBAIDResolver): EBA ID resolver instance

    Returns:
        dict: Processed data for this file
    """
    file_data = {
        'templates': {},
        'tables': {},
        'table_versions': {},
        'axes': {},
        'ordinates': {},
        'cells': {},
        'cell_positions': {},
        'dimensions': {},
        'variables': {},
        'members': {},
        'domains': {}
    }

    # Process file using existing logic but store in local file_data
    process_template_file(csv_path, filename, {'templates': file_data['templates'],
                                              'tables': file_data['tables'],
                                              'table_versions': file_data['table_versions'],
                                              'axes': file_data['axes'],
                                              'ordinates': file_data['ordinates'],
                                              'cells': file_data['cells'],
                                              'cell_positions': file_data['cell_positions'],
                                              'dimensions': file_data['dimensions'],
                                              'variables': file_data['variables'],
                                              'members': file_data['members'],
                                              'domains': file_data['domains']}, resolver)

    return file_data

def merge_file_data(template_info, file_data, csv_file):
    """
    Merge data from a processed file into the main template_info dictionary.

    Args:
        template_info (dict): Main template information dictionary
        file_data (dict): Data from processed file
        csv_file (str): Name of the source CSV file for error tracking
    """
    try:
        # Merge main data categories with defensive checks
        for category in ['templates', 'tables', 'table_versions', 'axes', 'ordinates',
                        'cells', 'cell_positions', 'dimensions', 'variables', 'members', 'domains']:
            if category in file_data and file_data[category]:
                # Ensure template_info has the category initialized
                if category not in template_info:
                    template_info[category] = {}

                # Safely update with error handling for each item
                for key, value in file_data[category].items():
                    try:
                        template_info[category][key] = value
                    except Exception as item_error:
                        print(f"Warning: Failed to merge {category} item '{key}' from {csv_file}: {str(item_error)}")

        # Update lookup indices for fast access
        update_lookup_indices(template_info, file_data, csv_file)

    except Exception as e:
        print(f"Error merging data from {csv_file}: {str(e)}")
        # Ensure metadata dict exists before adding error
        if 'metadata' not in template_info:
            template_info['metadata'] = {}
        template_info['metadata'][f'merge_error_{csv_file}'] = str(e)

def update_lookup_indices(template_info, file_data, csv_file):
    """
    Update pre-computed lookup indices for optimized access patterns.

    Args:
        template_info (dict): Main template information dictionary
        file_data (dict): Data from processed file
        csv_file (str): Source CSV file name
    """
    # Ensure lookup_indices exists
    if 'lookup_indices' not in template_info:
        template_info['lookup_indices'] = {
            'template_id_to_eba': {},
            'framework_templates': defaultdict(set),
            'table_id_to_eba': {},
            'template_to_tables': defaultdict(set),
            'domain_members': defaultdict(set)
        }

    indices = template_info['lookup_indices']

    # Update template indices
    if 'templates' in file_data:
        for eba_id, template_data in file_data['templates'].items():
            try:
                original_id = template_data.get('original_id')
                framework = template_data.get('framework')
                if original_id:
                    indices['template_id_to_eba'][original_id] = eba_id
                if framework:
                    # Ensure framework_templates is initialized
                    if 'framework_templates' not in indices:
                        indices['framework_templates'] = defaultdict(set)
                    indices['framework_templates'][framework].add(eba_id)
            except Exception as e:
                print(f"Warning: Failed to update template index for {eba_id}: {str(e)}")

    # Update table indices
    if 'tables' in file_data:
        for eba_id, table_data in file_data['tables'].items():
            try:
                original_id = table_data.get('original_id')
                template_id = table_data.get('template_id')
                if original_id:
                    indices['table_id_to_eba'][original_id] = eba_id
                if template_id and template_id in indices['template_id_to_eba']:
                    template_eba_id = indices['template_id_to_eba'][template_id]
                    # Ensure template_to_tables is initialized
                    if 'template_to_tables' not in indices:
                        indices['template_to_tables'] = defaultdict(set)
                    indices['template_to_tables'][template_eba_id].add(eba_id)
            except Exception as e:
                print(f"Warning: Failed to update table index for {eba_id}: {str(e)}")

    # Update member-domain indices
    if 'members' in file_data:
        for eba_id, member_data in file_data['members'].items():
            try:
                domain_code = member_data.get('domain_code')
                if domain_code:
                    # Ensure domain_members is initialized
                    if 'domain_members' not in indices:
                        indices['domain_members'] = defaultdict(set)
                    indices['domain_members'][domain_code].add(eba_id)
            except Exception as e:
                print(f"Warning: Failed to update member-domain index for {eba_id}: {str(e)}")

def process_csv_in_batches(csv_path, batch_size=2000):
    """
    Generator that yields batches of rows from a CSV file for memory-efficient processing.

    Args:
        csv_path (str): Path to CSV file
        batch_size (int): Number of rows per batch

    Yields:
        list: Batch of rows as dictionaries
    """
    with open(csv_path, 'r', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        batch = []

        for row in reader:
            batch.append(row)
            if len(batch) >= batch_size:
                yield batch
                batch = []

        # Yield remaining rows
        if batch:
            yield batch

def process_template_file(csv_path, filename, template_info, resolver):
    """
    Process individual CSV file for template information with EBA ID resolution.
    Uses batch processing for improved memory efficiency and performance.

    Args:
        csv_path (str): Path to CSV file
        filename (str): Name of the CSV file
        template_info (dict): Template information dictionary to update
        resolver (EBAIDResolver): EBA ID resolver instance
    """
    # Determine batch size based on file type (larger files get smaller batches)
    file_size = os.path.getsize(csv_path)
    if file_size > 50 * 1024 * 1024:  # Files > 50MB
        batch_size = 1000
    elif file_size > 10 * 1024 * 1024:  # Files > 10MB
        batch_size = 2000
    else:
        batch_size = 5000  # Smaller files can handle larger batches

    # Process file in batches for memory efficiency
    total_processed = 0
    for batch_num, batch in enumerate(process_csv_in_batches(csv_path, batch_size)):
        process_template_file_batch(batch, filename, template_info, resolver)
        total_processed += len(batch)

        # Progress indicator for large files
        if batch_num > 0 and batch_num % 10 == 0:
            print(f"    Processed {total_processed} rows from {filename}")

    if total_processed > batch_size:
        print(f"    Completed {filename}: {total_processed} total rows processed")

def process_template_file_batch(batch, filename, template_info, resolver):
    """
    Process a batch of rows from a CSV file for template information.

    Args:
        batch (list): List of row dictionaries from CSV
        filename (str): Name of the CSV file
        template_info (dict): Template information dictionary to update
        resolver (EBAIDResolver): EBA ID resolver instance
    """
    if not batch:
        return

    if filename == 'Template.csv':
        for row in batch:
            template_id = row.get('TemplateID', '')
            template_code = row.get('TemplateCode', '')
            concept_id = row.get('ConceptID', '')

            # Resolve EBA ID for template using enhanced resolver
            resolved = resolver.resolve_template_id(template_id, template_code, concept_id)
            eba_id = resolved['eba_id']

            # Also resolve cube structure ID for template->cube mapping
            cube_resolved = resolver.resolve_cube_structure_id(template_id, template_code, resolved.get('framework'))
            cube_structure_id = cube_resolved['eba_id']

            template_info['templates'][eba_id] = {
                **resolved,
                'label': row.get('TemplateLabel', ''),
                'concept_id': concept_id,
                'cube_structure_id': cube_structure_id,  # Add cube mapping
                'tables': [],
                'axes': [],  # Track axes for this template
                'cells': [],  # Track cells for this template
                'raw_data': row
            }

    elif filename == 'Table.csv':
        for row in batch:
            table_id = row.get('TableID', '')
            template_id = row.get('TemplateID', '')
            original_code = row.get('OriginalTableCode', '')

            # Resolve EBA ID for table
            resolved = resolver.resolve_table_id(table_id, template_id, original_code)
            eba_id = resolved['eba_id']

            template_info['tables'][eba_id] = {
                **resolved,
                'original_label': row.get('OriginalTableLabel', ''),
                'concept_id': row.get('ConceptID', ''),
                'cells': [],
                'raw_data': row
            }

            # Link table to template using EBA IDs
            template_eba_id = None
            for tid, tdata in template_info['templates'].items():
                if tdata.get('original_id') == template_id:
                    template_eba_id = tid
                    break

            if template_eba_id:
                template_info['templates'][template_eba_id]['tables'].append(eba_id)

    elif filename == 'Concept.csv':
        for row in batch:
            concept_id = row.get('ConceptID', '')
            concept_code = row.get('ConceptCode', '')
            data_type = row.get('DataType', '')

            # Resolve EBA ID for concept/variable
            resolved = resolver.resolve_variable_id(concept_id, concept_code, data_type)
            eba_id = resolved['eba_id']

            template_info['variables'][eba_id] = {
                **resolved,
                'label': row.get('ConceptLabel', ''),
                'dimension_id': row.get('DimensionID', ''),
                'tables': [],
                'raw_data': row
            }

    elif filename == 'Member.csv':
        for row in batch:
            member_id = row.get('MemberID', '')
            domain_id = row.get('DomainID', '')
            member_code = row.get('MemberCode', '')
            member_xbrl_code = row.get('MemberXbrlCode', '')

            # Resolve EBA ID for member
            resolved = resolver.resolve_member_id(member_id, domain_id, member_code, member_xbrl_code)
            eba_id = resolved['eba_id']

            template_info['members'][eba_id] = {
                    **resolved,
                    'label': row.get('MemberLabel', ''),
                    'description': row.get('MemberDescription', ''),
                    'is_default': row.get('IsDefaultMember', ''),
                    'concept_id': row.get('ConceptID', ''),
                    'raw_data': row
                }

    elif filename == 'Domain.csv':
        for row in batch:
            domain_id = row.get('DomainID', '')
            domain_code = row.get('DomainCode', '')

            # Resolve EBA ID for domain
            resolved = resolver.resolve_domain_id(domain_id, domain_code)
            eba_id = resolved['eba_id']

            template_info['domains'][eba_id] = {
                    **resolved,
                    'label': row.get('DomainLabel', ''),
                    'description': row.get('DomainDescription', ''),
                    'is_typed': row.get('IsTypedDomain', ''),
                    'data_type_id': row.get('DataTypeID', ''),
                    'concept_id': row.get('ConceptID', ''),
                    'raw_data': row
                }

    elif filename == 'TableVersion.csv':
        for row in batch:
            table_vid = row.get('TableVID', '')
            table_id = row.get('TableID', '')
            version_code = row.get('TableVersionCode', '')
            version_label = row.get('TableVersionLabel', '')
            from_date = row.get('FromDate', '')
            to_date = row.get('ToDate', '')

            # Resolve version-aware table ID
            resolved = resolver.resolve_table_id(table_vid, table_id, version_code, version_code)
            eba_id = resolved['eba_id']

            template_info['table_versions'][eba_id] = {
                    **resolved,
                    'table_vid': table_vid,
                    'table_id': table_id,
                    'version_code': version_code,
                    'version_label': version_label,
                    'from_date': from_date,
                    'to_date': to_date,
                    'raw_data': row
                }

    elif filename == 'Axis.csv':
        for row in batch:
            axis_id = row.get('AxisID', '')
            table_vid = row.get('TableVID', '')
            orientation = row.get('AxisOrientation', '')
            axis_label = row.get('AxisLabel', '')

            # Find table version info for context
            table_code = 'UNK'
            framework = 'FINREP'
            version = ''
            for tv_id, tv_data in template_info['table_versions'].items():
                if tv_data.get('table_vid') == table_vid:
                    table_code = tv_data.get('version_code', '')
                    framework = resolver._determine_framework_from_code(table_code) or 'FINREP'
                    break

            # Resolve axis ID using enhanced resolver
            axis_resolved = resolver.resolve_axis_id(axis_id, table_code, framework, version, orientation)
            eba_id = axis_resolved['eba_id']

            template_info['axes'][eba_id] = {
                    **axis_resolved,
                    'table_vid': table_vid,
                    'label': axis_label,
                    'ordinates': [],
                    'raw_data': row
                }

    elif filename == 'AxisOrdinate.csv':
        missing_axis_count = 0
        processed_ordinate_count = 0

        for row in batch:
            ordinate_id = row.get('OrdinateID', '')
            axis_id = row.get('AxisID', '')
            ordinate_code = row.get('OrdinateCode', '')
            ordinate_label = row.get('OrdinateLabel', '')

            # Find corresponding axis info
            axis_info = None
            for ax_id, ax_data in template_info['axes'].items():
                if ax_data.get('original_id') == axis_id:
                    axis_info = ax_data
                    break

            if axis_info:
                try:
                    # Resolve ordinate ID using axis context
                    ordinate_resolved = resolver.resolve_axis_ordinate_id(ordinate_id, axis_info, ordinate_code)
                    eba_id = ordinate_resolved['eba_id']

                    template_info['ordinates'][eba_id] = {
                            **ordinate_resolved,
                            'label': ordinate_label,
                            'level': row.get('Level', ''),
                            'order': row.get('Order', ''),
                            'path': row.get('Path', ''),
                            'is_abstract': row.get('IsAbstractHeader', ''),
                            'raw_data': row
                        }

                    # Link ordinate to axis
                    axis_eba_id = axis_info.get('eba_id')
                    if axis_eba_id in template_info['axes']:
                        template_info['axes'][axis_eba_id]['ordinates'].append(eba_id)

                    processed_ordinate_count += 1

                except Exception as e:
                    print(f"    Warning: Failed to process ordinate {ordinate_id}: {str(e)}")
            else:
                # Handle case where axis info is not found - store for analysis
                missing_axis_count += 1

                # Store missing axis information for later analysis
                if 'missing_axes' not in template_info['metadata']:
                    template_info['metadata']['missing_axes'] = {}

                if axis_id not in template_info['metadata']['missing_axes']:
                    template_info['metadata']['missing_axes'][axis_id] = []

                template_info['metadata']['missing_axes'][axis_id].append({
                    'ordinate_id': ordinate_id,
                    'ordinate_code': ordinate_code,
                    'ordinate_label': ordinate_label
                })

                # Only print warning for first few instances to avoid spam
                if missing_axis_count <= 5:
                    print(f"    Warning: No axis found for ordinate {ordinate_id} with axis_id {axis_id}, skipping ordinate processing")
                elif missing_axis_count == 6:
                    print(f"    Warning: {missing_axis_count} missing axes found, suppressing further warnings...")

        # Print summary at end of batch
        if missing_axis_count > 0:
            print(f"    Completed AxisOrdinate.csv batch: {processed_ordinate_count} processed, {missing_axis_count} skipped (missing axes)")

    elif filename == 'TableCell.csv':
        for row in batch:
            cell_id = row.get('CellID', '')
            table_vid = row.get('TableVID', '')
            is_shaded = row.get('IsShaded', '')
            cell_code = row.get('CellCode', '')

            # Resolve cell ID using enhanced resolver
            context_data = {'TableVID': table_vid, 'CellCode': cell_code}
            cell_resolved = resolver.resolve_cell_id(cell_id, context_data)
            eba_id = cell_resolved['eba_id']

            template_info['cells'][eba_id] = {
                    **cell_resolved,
                    'table_vid': table_vid,
                    'is_shaded': is_shaded,
                    'cell_code': cell_code,
                    'ordinates': [],  # Will be populated by CellPosition
                    'raw_data': row
                }

            # Link cell to table version
            for tv_id, tv_data in template_info['table_versions'].items():
                if tv_data.get('table_vid') == table_vid:
                    if 'cells' not in tv_data:
                        tv_data['cells'] = []
                    tv_data['cells'].append(eba_id)
                    break

    elif filename == 'CellPosition.csv':
        for row in batch:
            cell_id = row.get('CellID', '')
            axis_ordinate_id = row.get('OrdinateID', '')

            # Find corresponding cell and ordinate EBA IDs
            cell_eba_id = None
            ordinate_eba_id = None

            for c_id, c_data in template_info['cells'].items():
                if c_data.get('original_id') == cell_id:
                    cell_eba_id = c_id
                    break

            for o_id, o_data in template_info['ordinates'].items():
                if o_data.get('original_id') == axis_ordinate_id:
                    ordinate_eba_id = o_id
                    break

            if cell_eba_id and ordinate_eba_id:
                # Create cell position mapping
                position_id = f"{cell_eba_id}_{ordinate_eba_id}"
                template_info['cell_positions'][position_id] = {
                        'cell_eba_id': cell_eba_id,
                        'ordinate_eba_id': ordinate_eba_id,
                        'raw_data': row
                    }

                # Link ordinate to cell
                if cell_eba_id in template_info['cells']:
                    template_info['cells'][cell_eba_id]['ordinates'].append(ordinate_eba_id)
            else:
                # Log failed lookups for debugging
                if not cell_eba_id and not ordinate_eba_id:
                    print(f"    Warning: Could not find both cell_id {cell_id} and ordinate_id {axis_ordinate_id} in CellPosition processing")
                elif not cell_eba_id:
                    print(f"    Warning: Could not find cell_id {cell_id} in CellPosition processing")
                elif not ordinate_eba_id:
                    print(f"    Warning: Could not find ordinate_id {axis_ordinate_id} in CellPosition processing")

    elif filename == 'Dimension.csv':
        for row in batch:
            dimension_id = row.get('DimensionID', '')
            dimension_code = row.get('DimensionCode', '')

            # Treat dimensions as a special type of domain
            resolved = resolver.resolve_domain_id(dimension_id, dimension_code)
            eba_id = f"EBA_DIM_{dimension_code}" if dimension_code else f"EBA_DIM_{dimension_id}"

            template_info['dimensions'][eba_id] = {
                'original_id': dimension_id,
                'eba_id': eba_id,
                'code': dimension_code,
                'label': row.get('DimensionLabel', ''),
                'concept_id': row.get('ConceptID', ''),
                'members': [],
                'raw_data': row
            }

def create_template_relationships(template_info, resolver):
    """
    Create cross-references and relationships between template components with EBA IDs.
    Uses optimized lookup indices for improved performance.

    Args:
        template_info (dict): Template information dictionary to update
        resolver (EBAIDResolver): EBA ID resolver instance
    """
    indices = template_info['lookup_indices']

    # Create template to cell mappings using pre-computed indices
    for template_id, template in template_info['templates'].items():
        template['cells'] = []
        # Use pre-computed template-to-tables mapping for faster lookup
        if template_id in indices['template_to_tables']:
            for table_id in indices['template_to_tables'][template_id]:
                if table_id in template_info['tables']:
                    template['cells'].extend(template_info['tables'][table_id].get('cells', []))
        template['cells'] = list(set(template['cells']))  # Remove duplicates using set for efficiency

    # Use pre-computed framework groupings
    frameworks = dict(indices['framework_templates'])  # Convert defaultdict to regular dict

    # Use pre-computed domain member groupings
    domain_members = dict(indices['domain_members'])   # Convert defaultdict to regular dict

    # Create optimized relationships structure
    template_info['relationships'].update({
        'frameworks': frameworks,
        'domain_members': domain_members,
        'eba_id_mappings': indices,  # Use the pre-computed lookup indices directly
        'optimized_indices': {
            'template_count_by_framework': {fw: len(templates) for fw, templates in frameworks.items()},
            'member_count_by_domain': {domain: len(members) for domain, members in domain_members.items()},
            'total_relationships': sum(len(tables) for tables in indices['template_to_tables'].values())
        }
    })

    # Create summary statistics
    template_info['metadata']['summary'] = {
        'total_templates': len(template_info['templates']),
        'total_tables': len(template_info['tables']),
        'total_table_versions': len(template_info.get('table_versions', {})),
        'total_axes': len(template_info.get('axes', {})),
        'total_ordinates': len(template_info.get('ordinates', {})),
        'total_cells': len(template_info.get('cells', {})),
        'total_cell_positions': len(template_info.get('cell_positions', {})),
        'total_variables': len(template_info['variables']),
        'total_members': len(template_info['members']),
        'total_domains': len(template_info['domains']),
        'total_dimensions': len(template_info['dimensions']),
        'frameworks_found': list(frameworks.keys()),
        'domains_found': list(domain_members.keys())
    }

def export_mapped_data(csv_directory="target", export_directory="export_debug"):
    """
    Export mapped DPM data for debugging and validation.

    Args:
        csv_directory (str): Directory containing CSV files
        export_directory (str): Directory for exported mapped data
    """
    print(f"Starting export of mapped data from {csv_directory} to {export_directory}")

    exporter = CSVExporter(csv_directory=csv_directory, export_directory=export_directory)
    export_stats = exporter.export_mapped_data()

    print("\nExport Summary:")
    print(f"  Total Files: {export_stats['total_files']}")
    print(f"  Successful Exports: {export_stats['successful_exports']}")
    print(f"  Failed Exports: {export_stats['failed_exports']}")
    print(f"  Total Records Processed: {export_stats['total_records_processed']}")
    print(f"  Total Records Exported: {export_stats['total_records_exported']}")

    if export_stats['errors']:
        print(f"  Errors: {len(export_stats['errors'])}")
        for error in export_stats['errors'][:5]:  # Show first 5 errors
            print(f"    - {error}")
        if len(export_stats['errors']) > 5:
            print(f"    ... and {len(export_stats['errors']) - 5} more errors")

    print(f"\nDetailed reports available in: {export_directory}")
    return export_stats

def import_to_database(csv_directory="target"):
    """
    Import CSV data to the database.

    Args:
        csv_directory (str): Directory containing CSV files
    """
    print(f"Starting import to database from {csv_directory}")
    # Create importer with specific directory
    from import_classes import CSVImporter
    importer = CSVImporter(csv_directory=csv_directory)
    results = importer.import_all_csv_files()

    if results.get('success', True):
        print("Database import completed successfully")
        print(f"Files processed: {results.get('total_files', 0)}")
        print(f"Records created: {results.get('total_records_created', 0)}")
        print(f"Records updated: {results.get('total_records_updated', 0)}")
    else:
        print("Database import failed")
        if 'error' in results:
            print(f"Error: {results['error']}")
        raise Exception("Database import failed")

DIRECTORY_TO_EXTRACT_TO = "dpm_database"

def main():
    """
    Main function with support for different operation modes.
    """

    parser = argparse.ArgumentParser(
        description="DPM Import Tool with Export and Debug Functionality",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  uv run python main_script.py --mode export                 # Export mappings for debugging only
  uv run python main_script.py --mode import                 # Import to database only
  uv run python main_script.py --mode both                   # Export first, then import
  uv run python main_script.py --mode full                   # Full workflow: fetch, process, export, import
  uv run python main_script.py --skip-fetch --mode export    # Skip database fetch, export only
        """
    )

    parser.add_argument(
        '--mode',
        choices=['export', 'import', 'both', 'full'],
        default='full',
        help='Operation mode: export (debug only), import (database only), both (export then import), full (complete workflow)'
    )

    parser.add_argument(
        '--csv-directory',
        default='target',
        help='Directory containing CSV files (default: target)'
    )

    parser.add_argument(
        '--export-directory',
        default='export_debug',
        help='Directory for exported mapped data (default: export_debug)'
    )

    parser.add_argument(
        '--database-path',
        default='dpm_database/dpm_database.accdb',
        help='Path to DPM database file (default: dpm_database/dpm_database.accdb)'
    )

    parser.add_argument(
        '--url',
        default=URL_TO__1_0,
        help='URL to DPM database ZIP file'
    )

    parser.add_argument(
        '--skip-fetch',
        action='store_true',
        help='Skip database fetch step (use existing database file)'
    )

    parser.add_argument(
        '--skip-process',
        action='store_true',
        help='Skip database processing step (use existing CSV files)'
    )

    args = parser.parse_args()

    # Initialize logging
    logger = get_logger("main", log_level="INFO")
    log_process_start(logger, "DPM Import Tool",
                     mode=args.mode,
                     csv_directory=args.csv_directory,
                     export_directory=args.export_directory)

    print(f"DPM Import Tool - Mode: {args.mode}")
    print(f"CSV Directory: {args.csv_directory}")
    print(f"Export Directory: {args.export_directory}")
    print("-" * 50)

    # # Step 1: Fetch database (unless skipped or not needed)
    # if not args.skip_fetch and args.mode in ['full']:
    #     for file in os.listdir(DIRECTORY_TO_EXTRACT_TO):
    #         os.remove(os.path.join(DIRECTORY_TO_EXTRACT_TO, file))
    #     os.makedirs(DIRECTORY_TO_EXTRACT_TO, exist_ok=True)
    #     fetch_dpm_database(args.url)

    # # Step 2: Process database to CSV (unless skipped or not needed)
    # if not args.skip_process and args.mode in ['full']:
    #     process_database(args.database_path)

    # Step 3: Export mapped data (if requested)
    if args.mode in ['export', 'both', 'full']:
        export_stats = export_mapped_data(args.csv_directory, args.export_directory)

        # Check if export was successful before proceeding
        if export_stats['failed_exports'] > 0:
            print(f"\nWarning: {export_stats['failed_exports']} exports failed!")
            if args.mode == 'both':
                response = input("Continue with database import anyway? (y/N): ")
                if response.lower() != 'y':
                    print("Stopping before database import due to export failures.")
                    sys.exit(1)

    # Step 4: Import to database (if requested)
    if args.mode in ['import', 'both', 'full']:
        # import_to_database(args.csv_directory)
        pass

    print("\nOperation completed successfully!")


if __name__ == "__main__":
    main()
