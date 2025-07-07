from functools import lru_cache
import sys
from datetime import datetime
import re

def safe_call(func, fallback_value=None):
    """
    Safely call a function and return fallback value on error.

    Args:
        func: Function to call
        fallback_value: Value to return if function fails

    Returns:
        Function result or fallback value
    """
    try:
        return func()
    except Exception as e:
        print(f"Warning: Function call failed: {e}", file=sys.stderr)
        return fallback_value

@lru_cache(maxsize=2000)
def transform_boolean(value):
    """
    Transform various boolean representations to standardized format.
    Returns 1 for True, 0 for False, None for unknown.
    """
    if value is None or value == '':
        return 0

    if isinstance(value, bool):
        return 1 if value else 0

    if isinstance(value, (int, float)):
        return 1 if value != 0 else 0

    if isinstance(value, str):
        value = value.lower().strip()
        if value in ['true', 'yes', '1', 'y', 't']:
            return 1
        elif value in ['false', 'no', '0', 'n', 'f']:
            return 0

    return 0

def transform_id(value, prefix=''):
    """
    Transform ID values to ensure they are valid strings.
    Adds prefix if provided.
    """
    if value is None or value == '':
        return None

    str_value = str(value).strip()
    if prefix and not str_value.startswith(prefix):
        return f"{prefix}{str_value}"

    return str_value

def safe_get(row, key, default=None):
    """
    Safely get value from row dictionary with default fallback.
    """
    return row.get(key, default) if row.get(key) is not None else default

def validate_id_components(owner=None, framework=None, template=None, version=None, context="ID"):
    """
    Validate components before ID construction to ensure data quality.

    Args:
        owner (str): Owner prefix (EBA, NODE, etc.)
        framework (str): Framework code (FINREP, COREP, etc.)
        template (str): Template code (F_08.01, C_01.00, etc.)
        version (str): Version string (2.9, 3.0, etc.)
        context (str): Context for error reporting

    Returns:
        tuple: (is_valid, error_messages)
    """
    errors = []

    # Validate owner
    if owner:
        if not isinstance(owner, str) or len(owner.strip()) == 0:
            errors.append(f"{context}: Invalid owner '{owner}' - must be non-empty string")
        elif len(owner) > 10:
            errors.append(f"{context}: Owner '{owner}' too long (max 10 chars)")
        elif not owner.replace('_', '').isalnum():
            errors.append(f"{context}: Owner '{owner}' contains invalid characters")

    # Validate framework
    if framework:
        if not isinstance(framework, str) or len(framework.strip()) == 0:
            errors.append(f"{context}: Invalid framework '{framework}' - must be non-empty string")
        elif len(framework) > 20:
            errors.append(f"{context}: Framework '{framework}' too long (max 20 chars)")

    # Validate template
    if template:
        if not isinstance(template, str) or len(template.strip()) == 0:
            errors.append(f"{context}: Invalid template '{template}' - must be non-empty string")
        elif len(template) > 50:
            errors.append(f"{context}: Template '{template}' too long (max 50 chars)")

    # Validate version
    if version:
        if not isinstance(version, str) or len(version.strip()) == 0:
            errors.append(f"{context}: Invalid version '{version}' - must be non-empty string")
        elif len(version) > 10:
            errors.append(f"{context}: Version '{version}' too long (max 10 chars)")
        # Check version format (should be like 2.9, 3.0, etc.)
        import re
        if not re.match(r'^\d+\.\d+(\.\d+)?$', version.strip()):
            errors.append(f"{context}: Version '{version}' should follow format 'X.Y' or 'X.Y.Z'")

    return len(errors) == 0, errors

def sanitize_id_component(component, max_length=50):
    """
    Sanitize a component for use in ID construction.
    Removes dangerous characters and ensures proper format.

    Args:
        component (str): Component to sanitize
        max_length (int): Maximum allowed length

    Returns:
        str: Sanitized component
    """
    if not component:
        return ''

    # Convert to string and clean
    clean = str(component).strip().upper()

    # Remove or replace dangerous characters
    import re
    clean = re.sub(r'[^\w\.\-]', '_', clean)  # Allow word chars, dots, hyphens
    clean = re.sub(r'_+', '_', clean)  # Collapse multiple underscores
    clean = clean.strip('_')  # Remove leading/trailing underscores

    # Truncate if too long
    if len(clean) > max_length:
        clean = clean[:max_length]

    return clean

def log_transformation_warning(message, row_data=None):
    """
    Log transformation warnings for debugging and monitoring.

    Args:
        message (str): Warning message
        row_data (dict): Optional row data for context
    """
    import sys
    warning_msg = f"DPM Transformation Warning: {message}"
    if row_data and isinstance(row_data, dict):
        # Include relevant row identifiers for debugging
        row_info = []
        for key in ['TableID', 'TemplateID', 'OwnerID', 'CubeID', 'ModuleID']:
            if key in row_data and row_data[key] is not None:
                row_info.append(f"{key}={row_data[key]}")
        if row_info:
            warning_msg += f" [Row: {', '.join(row_info)}]"

    print(warning_msg, file=sys.stderr)

@lru_cache(maxsize=3000)
def resolve_maintenance_agency_cached(owner_id, framework_id, template_id, taxonomy_id, module_id, context_type="unknown"):
    """
    Intelligent maintenance agency resolution based on multiple data sources.

    This function determines the appropriate maintenance agency using a hierarchy
    of data sources, avoiding hardcoded "NODE" values where possible.

    Args:
        row (dict): Data row containing potential agency information
        context_type (str): Type of entity (table, cube, framework, etc.)

    Returns:
        str: Maintenance agency ID (e.g., "EBA", "ECB", "NODE")
    """
    # Priority 1: Explicit owner data

    if owner_id and owner_id in LOOKUP_TABLES['owners']:
        owner_prefix = LOOKUP_TABLES['owners'][owner_id].get('OwnerPrefix')
        if owner_prefix and owner_prefix.strip():
            return owner_prefix.upper()

    # Priority 2: Framework-based resolution

    if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
        framework_code = LOOKUP_TABLES['frameworks'][framework_id].get('FrameworkCode', '')
        # Return EBA for regulatory frameworks
        if framework_code in ['FINREP', 'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY', 'IF', 'GSII', 'MREL', 'ESG', 'IPU', 'PILLAR3', 'IRRBB', 'DORA', 'FC', 'MICA']:
            return 'EBA'

    # Priority 3: Template code analysis

    if template_id and template_id in LOOKUP_TABLES['templates']:
        template_code = LOOKUP_TABLES['templates'][template_id].get('TemplateCode', '')
        # Extract framework from template code pattern (F=FINREP, C=COREP, A=AE, etc.)
        if template_code:
            template_prefix = template_code.strip().upper()
            if template_prefix.startswith('F '):
                return 'EBA'  # FINREP
            elif template_prefix.startswith('C '):
                return 'EBA'  # COREP
            elif template_prefix.startswith('A '):
                return 'EBA'  # AE (Asset Encumbrance)
        return 'EBA'

    # Priority 4: Taxonomy-based resolution

    if taxonomy_id and taxonomy_id in LOOKUP_TABLES['taxonomies']:
        taxonomy_info = LOOKUP_TABLES['taxonomies'][taxonomy_id]
        framework_id = taxonomy_info.get('FrameworkID')
        if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
            framework_code = LOOKUP_TABLES['frameworks'][framework_id].get('FrameworkCode', '')
            # Return EBA for regulatory frameworks
            if framework_code in ['FINREP', 'COREP', 'AE', 'FP', 'SBP', 'REM', 'RES', 'PAY', 'IF', 'GSII', 'MREL', 'ESG', 'IPU', 'PILLAR3', 'IRRBB', 'DORA', 'FC', 'MICA']:
                return 'EBA'

    # Priority 7: Default based on context type
    if context_type in ['framework', 'template', 'table', 'cube']:
        # These are typically regulatory entities - use EBA for regulatory data
        return 'EBA'

    # Final fallback
    return 'NODE'

def resolve_maintenance_agency(row, context_type="unknown"):
    """
    Wrapper function for resolve_maintenance_agency_cached that extracts IDs from row data.

    Args:
        row (dict): Data row containing potential agency information
        context_type (str): Type of entity (table, cube, framework, etc.)

    Returns:
        str: Maintenance agency ID (e.g., "EBA", "ECB", "NODE")
    """
    # Extract relevant IDs from the row
    owner_id = row.get('OwnerID') or row.get('MaintenanceAgencyID')
    framework_id = row.get('FrameworkID')
    template_id = row.get('TemplateID')
    taxonomy_id = row.get('TaxonomyID')
    module_id = row.get('ModuleID')

    return resolve_maintenance_agency_cached(owner_id, framework_id, template_id, taxonomy_id, module_id, context_type)

def derive_description(row, primary_fields=None, fallback_prefix=""):
    """
    Intelligently derive description from available fields.

    Args:
        row (dict): Data row
        primary_fields (list): Fields to check in priority order
        fallback_prefix (str): Prefix for generated descriptions

    Returns:
        str: Derived description or None
    """
    if primary_fields is None:
        primary_fields = ['Description', 'Label', 'Name', 'Code']

    # Try to find existing description
    for field in primary_fields:
        value = row.get(field)
        if value and str(value).strip():
            return str(value).strip()

    # Generate description from available data
    name = row.get('Name') or row.get('Label') or row.get('Code')
    if name:
        return f"{fallback_prefix}{name}".strip()

    # Last resort - use ID
    for id_field in ['ID', 'TableID', 'CubeID', 'ModuleID', 'TemplateID']:
        if id_field in row and row[id_field] is not None:
            return f"{fallback_prefix}ID {row[id_field]}".strip()

    return None

def derive_valid_dates(row, context_type="unknown"):
    """
    Derive appropriate validity dates based on context and available data.
    Uses DateManager for consistent date handling.

    Args:
        row (dict): Data row
        context_type (str): Type of entity for appropriate date selection

    Returns:
        tuple: (valid_from, valid_to)
    """
    # Try to get explicit dates first
    valid_from = None
    valid_to = None

    # Check for explicit validity dates
    for from_field in ['ValidFrom', 'VALID_FROM', 'FromDate', 'StartDate']:
        if from_field in row and row[from_field]:
            valid_from = DateManager.parse_date(row[from_field], context_type, row)
            break

    for to_field in ['ValidTo', 'VALID_TO', 'ToDate', 'EndDate']:
        if to_field in row and row[to_field]:
            valid_to = DateManager.parse_date(row[to_field], context_type, row)
            break

    # Derive from taxonomy if available
    if not valid_from or not valid_to:
        taxonomy_dates = DateManager.derive_dates_from_taxonomy(row)
        if taxonomy_dates[0] and not valid_from:
            valid_from = taxonomy_dates[0]
        if taxonomy_dates[1] and not valid_to:
            valid_to = taxonomy_dates[1]

    # Set appropriate defaults based on context
    if context_type in ['regulatory', 'framework', 'template', 'cube', 'table']:
        return DateManager.get_regulatory_dates(valid_from, valid_to)
    else:
        # Generic defaults
        return (valid_from or DateManager.REGULATORY_START,
                valid_to or DateManager.REGULATORY_END)

def derive_cube_structure_id(row):
    """
    Derive cube structure ID from template or table relationships.

    Args:
        row (dict): Data row

    Returns:
        str: Derived cube structure ID or None
    """
    # Try to build from template
    template_id = row.get('TemplateID')
    if template_id and template_id in LOOKUP_TABLES['templates']:
        template_code = LOOKUP_TABLES['templates'][template_id].get('TemplateCode', '')
        if template_code:
            return f"STRUCT_{normalize_template_code(template_code)}"

    # Try to build from table
    table_id = row.get('TableID')
    if table_id:
        return f"STRUCT_TBL_{table_id}"

    # Try to build from cube
    cube_id = row.get('CubeID')
    if cube_id:
        return f"STRUCT_CUBE_{cube_id}"

    return None

def derive_primary_concept(row):
    """
    Derive primary concept from variable or dimension data.

    Args:
        row (dict): Data row

    Returns:
        str: Derived concept ID or None
    """
    # Look for concept references
    concept_id = row.get('ConceptID')
    if concept_id:
        return concept_id

    # Derive from code patterns
    code = row.get('Code', '')
    if code and isinstance(code, str):
        # Generate concept ID from code
        clean_code = sanitize_id_component(code, 20)
        return f"CONCEPT_{clean_code}"

    return None

def derive_metric_from_datapoint(row):
    """
    Derive metric information from datapoint context.

    Args:
        row (dict): Data row

    Returns:
        str: Derived metric ID or None
    """
    # Direct metric reference
    metric_id = row.get('MetricID')
    if metric_id:
        return metric_id

    # Derive from categorization key patterns
    cat_key = row.get('CategorisationKey', '')
    if 'ATY' in cat_key:
        # Extract metric from categorization pattern
        parts = cat_key.split('ATY')
        if len(parts) > 1:
            metric_part = parts[1][:4]  # First 4 chars after ATY
            try:
                return int(metric_part)
            except ValueError:
                pass

    return None

def derive_framework_subdomain(row):
    """
    Derive framework subdomain from domain and template relationships.

    Args:
        row (dict): Data row

    Returns:
        str: Derived subdomain ID or None
    """
    domain_id = row.get('DOMAIN_ID') or row.get('DomainID')
    template_id = row.get('TemplateID')

    if domain_id and template_id:
        return f"SUB_{domain_id}_{template_id}"

    return None

class DateManager:
    """
    Centralized date management for consistent handling across all mappings.
    Provides standardized date parsing, validation, and transformation.
    """

    # Standard date formats for different contexts
    REGULATORY_START = '1900-01-01'
    REGULATORY_END = '9999-12-31'
    CURRENT_DATE_PLACEHOLDER = 'CURRENT_DATE'

    @staticmethod
    def parse_date(date_str, context="generic", row:dict=None):
        """
        Parse date string to standardized ISO format (YYYY-MM-DD).

        Args:
            date_str: Input date string in various formats
            context: Context for date interpretation

        Returns:
            str: Standardized date string or None
        """

        if date_str == "01/00/00 00:00:00":
            parsed_date = datetime(1900, 1, 1)
            return parsed_date.strftime('%Y-%m-%d')

        if not date_str or str(date_str).strip() == '':
            return None

        date_str = str(date_str).strip()

        # Handle special placeholders
        if date_str.upper() == 'CURRENT_DATE':
            return datetime.now().strftime('%Y-%m-%d')

        # Format: MM/DD/YY HH:MM:SS or MM/DD/YYYY
        if '/' in date_str:
            date_part = date_str.split(' ')[0]  # Remove time component
            parts = date_part.split('/')
            if len(parts) == 3:
                month, day, year = parts
                # Handle 2-digit years
                if len(year) == 2:
                    year_int = int(year)
                    if year_int < 50:
                        year = '20' + year
                    else:
                        year = '19' + year
                try:
                    parsed_date = datetime(int(year), int(month), int(day))
                    return parsed_date.strftime('%Y-%m-%d')
                except ValueError:
                    pass

        # Format: YYYY-MM-DD (already standard)
        if re.match(r'^\d{4}-\d{2}-\d{2}', date_str):
            return date_str[:10]  # Take only date part

        # Format: DD/MM/YYYY (European)
        european_match = re.match(r'^(\d{1,2})/(\d{1,2})/(\d{4})', date_str)
        if european_match and context == "european":
            day, month, year = european_match.groups()
            try:
                parsed_date = datetime(int(year), int(month), int(day))
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                pass

        # Format: YYYYMMDD
        if re.match(r'^\d{8}$', date_str):
            try:
                parsed_date = datetime.strptime(date_str, '%Y%m%d')
                return parsed_date.strftime('%Y-%m-%d')
            except ValueError:
                pass

        # Log parsing issues
        log_transformation_warning(f"Could not parse date: '{date_str}'",str(row))
        return None

    @staticmethod
    def get_regulatory_dates(start_date=None, end_date=None):
        """
        Get appropriate regulatory validity dates.

        Args:
            start_date: Optional start date override
            end_date: Optional end date override

        Returns:
            tuple: (valid_from, valid_to)
        """
        valid_from = start_date or DateManager.REGULATORY_START
        valid_to = end_date or DateManager.REGULATORY_END
        return valid_from, valid_to

    @staticmethod
    def derive_dates_from_taxonomy(row):
        """
        Derive dates from taxonomy context.

        Args:
            row: Data row with potential taxonomy references

        Returns:
            tuple: (valid_from, valid_to) or (None, None)
        """
        taxonomy_id = row.get('TaxonomyID')
        if taxonomy_id and taxonomy_id in LOOKUP_TABLES['taxonomies']:
            # This could be enhanced with actual taxonomy date fields
            # For now, return regulatory defaults
            return DateManager.get_regulatory_dates()

        return None, None

def transform_date(date_str, context="generic", row:dict=None):
    """
    Enhanced date transformation using DateManager.

    Args:
        date_str: Input date string
        context: Context for date interpretation

    Returns:
        str: Standardized date string or None
    """
    return DateManager.parse_date(date_str, context, row)

def transform_taxonomy_date(date_str, context="taxonomy", row:dict=None):
    """
    Transform taxonomy-specific date strings to standardized format.
    Handles publication dates and notional dates.
    """
    return DateManager.parse_date(date_str, context, row)

def transform_expression(expression_str):
    """
    Parse and standardize formula expressions.
    Handles table-based formulas and logical expressions.
    """
    if not expression_str or expression_str.strip() == '':
        return None

    # Clean up expression string
    expression = expression_str.strip()

    # Replace common patterns for standardization
    expression = expression.replace('{', '').replace('}', '')
    expression = expression.replace('$', '')

    return expression

def transform_multilingual(row, primary_key, fallback_key=None):
    """
    Handle multilingual text fields with fallback.
    """
    primary_value = row.get(primary_key)
    if primary_value and primary_value.strip():
        return primary_value.strip()

    if fallback_key:
        fallback_value = row.get(fallback_key)
        if fallback_value and fallback_value.strip():
            return fallback_value.strip()

    return None

def derive_version_from_date(date_str):
    """
    Derive framework version from date string.
    Uses standard version mapping based on effective dates.

    Args:
        date_str (str): Date string to analyze

    Returns:
        str: Framework version (e.g., "2.8", "2.9", "3.0", "3.2")
    """
    if not date_str:
        return "2.9"  # Default version

    try:
        # Parse date to extract year
        if '/' in date_str:
            # Format: MM/DD/YY or MM/DD/YYYY
            year_part = date_str.split('/')[-1].split(' ')[0]
            if len(year_part) == 2:
                year = int(year_part)
                year = 2000 + year if year < 50 else 1900 + year
            else:
                year = int(year_part)
        elif '-' in date_str:
            # Format: YYYY-MM-DD
            year = int(date_str.split('-')[0])
        else:
            return "2.9"  # Default if can't parse

        # Map year to framework version
        if year >= 2023:
            return "3.2"
        elif year >= 2021:
            return "3.0"
        elif year >= 2018:
            return "2.9"
        else:
            return "2.8"

    except (ValueError, IndexError):
        return "2.9"  # Default fallback

def transform_version(version_str):
    """
    Standardize version numbering.
    """
    if not version_str:
        return "1.0"

    version = str(version_str).strip()

    # Handle quoted versions
    if version.startswith('"') and version.endswith('"'):
        version = version[1:-1]

    return version

def normalize_code(code_str, max_length=50):
    """
    Normalize code strings for consistency.
    """
    if not code_str:
        return None

    code = str(code_str).strip()

    # Remove extra whitespace
    import re
    code = re.sub(r'\s+', '_', code)

    # Truncate if too long
    if len(code) > max_length:
        code = code[:max_length]

    return code.upper()

# ==================== ENHANCED ID CONSTRUCTION FUNCTIONS ====================

# Global lookup tables (will be populated during import)
LOOKUP_TABLES = {
    'owners': {},           # OwnerID -> {OwnerPrefix, OwnerName}
    'frameworks': {},       # FrameworkID -> {FrameworkCode, FrameworkLabel}
    'templates': {},        # TemplateID -> {TemplateCode, TemplateLabel}
    'taxonomies': {},       # TaxonomyID -> {DpmPackageCode, FrameworkID}
    'domains': {},          # DomainID -> {DomainCode, DomainLabel}
    'template_framework': {}, # TemplateID -> FrameworkID (derived from context)
    'table_versions': {},   # TableVID -> {TableVersionCode, FromDate, ToDate, Framework, Version}
    'modules': {}           # ModuleID -> {ModuleCode, ModuleLabel, TaxonomyID, ConceptualModuleID, Version, FrameworkID, FrameworkCode}
}

@lru_cache(maxsize=2000)
def normalize_template_code(template_code):
    """
    Convert template codes like 'F 08.01' to 'F_08.01' format.
    Handles various template code formats consistently including edge cases.

    Examples:
        'F 08.01' -> 'F_08.01'
        'F  08.01.a' -> 'F_08.01.a' (multiple spaces)
        ' F 08.01 ' -> 'F_08.01' (leading/trailing whitespace)
        'C   01.00' -> 'C_01.00' (multiple consecutive spaces)
        'F-08.01' -> 'F_08.01' (hyphens to underscores)
    """
    if not template_code:
        return ''

    # Convert to string and clean whitespace
    code = str(template_code).strip()

    if not code:
        return ''

    # Replace multiple consecutive spaces with single underscore
    import re
    code = re.sub(r'\s+', '_', code)

    # Replace hyphens with underscores (some templates might use hyphens)
    code = code.replace('-', '_')

    # Clean up multiple consecutive underscores
    code = re.sub(r'_+', '_', code)

    # Remove leading/trailing underscores that might result from edge cases
    code = code.strip('_')

    # Ensure uppercase for consistency (regulatory codes are typically uppercase)
    code = code.upper()

    # Handle special case: normalize multiple consecutive dots
    code = re.sub(r'\.{2,}', '.', code)

    return code

def build_table_id(row, owner_prefix=None, framework_code=None, template_code=None, version=None):
    """
    Build TABLE_ID following EBA technical export pattern.
    Examples: EBA_AE_EBA_F_32.01_AE_3.2, EBA_FINREP_EBA_F_08.01_FINREP_2.9

    Pattern: EBA_{framework}_EBA_{table_code}_{framework}_{version}
    """
    try:
        # Extract table information
        table_code = row.get('OriginalTableCode', row.get('Code', ''))
        table_id = row.get('TableID', '')
        template_id = row.get('TemplateID', '')

        # Determine framework from table code or template lookup
        framework = None
        if table_code:
            if table_code.startswith('F '):
                framework = 'FINREP'
            elif table_code.startswith('C '):
                framework = 'COREP'
            elif table_code.startswith('A '):
                framework = 'AE'
            else:
                # Try template lookup
                if template_id and template_id in LOOKUP_TABLES['templates']:
                    template_info = LOOKUP_TABLES['templates'][template_id]
                    template_code_lookup = template_info.get('TemplateCode', '')
                    if template_code_lookup.startswith('F '):
                        framework = 'FINREP'
                    elif template_code_lookup.startswith('C '):
                        framework = 'COREP'
                    elif template_code_lookup.startswith('A '):
                        framework = 'AE'

        # Fallback framework detection
        if not framework:
            framework = framework_code or lookup_framework_from_table(row) or 'UNK'

        # Clean table code for EBA format
        cleaned_table_code = normalize_template_code(table_code) if table_code else f"T_{table_id}"

        # Get version
        ver = version or lookup_version_from_table(row) or '1.0'

        # Build EBA table ID following the actual pattern
        table_id_final = f"EBA_{framework}_EBA_{cleaned_table_code}_{framework}_{ver}"

        # Sanitize final ID
        table_id_final = sanitize_id_component(table_id_final, 255)

        return table_id_final

    except Exception as e:
        log_transformation_warning(f"Error building TABLE_ID: {e}", row)
        table_id = row.get('TableID', 'UNK')
        return f"EBA_TABLE_{table_id}_1.0"

def build_cube_id(row, owner_prefix=None, framework_code=None, template_code=None, version=None):
    """
    Build CUBE_ID following EBA pattern from technical export format.
    Examples: EBA_FINREP_F_08.01_FINREP_2.9, EBA_AE_F_32.01_AE_3.2

    Enhanced to match actual technical export IDs rather than generic patterns.
    """
    try:
        # Determine components based on actual data patterns
        module_id = row.get('ModuleID', '')
        module_code = row.get('ModuleCode', row.get('Code', ''))  # ModuleCode is the correct field
        framework_id = row.get('FrameworkID', '')
        taxonomy_id = row.get('TaxonomyID', '')

        # Enhanced framework resolution using actual module code patterns
        owner = 'EBA'  # Default to EBA for regulatory modules
        framework = 'UNK'

        if module_code:
            module_code_upper = module_code.upper()
            if 'FINREP' in module_code_upper:
                framework = 'FINREP'
            elif 'COREP' in module_code_upper:
                framework = 'COREP'
            elif 'AE' in module_code_upper or 'ASSET' in module_code_upper:
                framework = 'AE'
            elif 'LCR' in module_code_upper:
                framework = 'COREP'  # LCR is part of COREP
            elif 'NSFR' in module_code_upper:
                framework = 'COREP'  # NSFR is part of COREP
            elif 'LE' in module_code_upper:
                framework = 'COREP'  # Large Exposures is part of COREP

        # Fallback to framework lookup if module code detection failed
        if framework == 'UNK' and framework_id and framework_id in LOOKUP_TABLES['frameworks']:
            framework_info = LOOKUP_TABLES['frameworks'][framework_id]
            framework_code_detected = framework_info.get('FrameworkCode', '')

            if 'FINREP' in framework_code_detected:
                framework = 'FINREP'
            elif 'COREP' in framework_code_detected:
                framework = 'COREP'
            elif 'AE' in framework_code_detected:
                framework = 'AE'

        # Create template code from module code (for cube identification)
        if module_code:
            # Clean module code for EBA format - remove underscores and make it more template-like
            template_cleaned = module_code.replace('_', '_').upper()
            template = template_cleaned
        else:
            template = f"MOD_{module_id}"

        # Version lookup from taxonomy
        ver = version or lookup_version_from_module(row) or '1.0'

        # Build EBA-style cube ID
        cube_id = f"{owner}_{framework}_{template}_{framework}_{ver}"

        # Sanitize final ID
        cube_id = sanitize_id_component(cube_id, 255)

        return cube_id

    except Exception as e:
        log_transformation_warning(f"Error building CUBE_ID: {e}", row)
        module_id = row.get('ModuleID', 'UNK')
        return f"EBA_MODULE_{module_id}_1.0"

def build_framework_id(row, owner_prefix=None, framework_code=None):
    """
    Build FRAMEWORK_ID following pattern: {OWNER_PREFIX}_{FRAMEWORK_CODE}
    Example: EBA_FINREP
    """
    owner = (owner_prefix or 'NODE').upper()
    framework = (framework_code or row.get('FrameworkCode', f"FW_{row.get('FrameworkID', 'UNK')}")).upper()

    return f"{owner}_{framework}"

def build_framework_id_from_module(row):
    """
    Build FRAMEWORK_ID from module data following EBA pattern.
    Enhanced to use Module -> Taxonomy -> Framework relationship chain.
    Example: EBA_AE, EBA_FINREP, EBA_COREP
    """
    module_id = row.get('ModuleID')
    taxonomy_id = row.get('TaxonomyID')
    framework_id = row.get('FrameworkID')

    # Method 1: Direct lookup via ModuleID -> cached framework info
    if module_id and module_id in LOOKUP_TABLES['modules']:
        module_info = LOOKUP_TABLES['modules'][module_id]
        framework_code = module_info.get('FrameworkCode', '')
        if framework_code:
            return f"EBA_{framework_code}"

    # Method 2: Module -> Taxonomy -> Framework relationship chain
    if module_id and module_id in LOOKUP_TABLES['modules']:
        module_info = LOOKUP_TABLES['modules'][module_id]
        module_taxonomy_id = module_info.get('TaxonomyID')

        if module_taxonomy_id and module_taxonomy_id in LOOKUP_TABLES['taxonomies']:
            taxonomy_info = LOOKUP_TABLES['taxonomies'][module_taxonomy_id]
            tax_framework_id = taxonomy_info.get('FrameworkID')

            if tax_framework_id and tax_framework_id in LOOKUP_TABLES['frameworks']:
                framework_info = LOOKUP_TABLES['frameworks'][tax_framework_id]
                framework_code = framework_info.get('FrameworkCode', '')
                if framework_code:
                    return f"EBA_{framework_code}"

    # Method 3: Direct taxonomy lookup from row
    if taxonomy_id and taxonomy_id in LOOKUP_TABLES['taxonomies']:
        taxonomy_info = LOOKUP_TABLES['taxonomies'][taxonomy_id]
        tax_framework_id = taxonomy_info.get('FrameworkID')

        if tax_framework_id and tax_framework_id in LOOKUP_TABLES['frameworks']:
            framework_info = LOOKUP_TABLES['frameworks'][tax_framework_id]
            framework_code = framework_info.get('FrameworkCode', '')
            if framework_code:
                return f"EBA_{framework_code}"

    # Method 4: Direct framework lookup from row
    if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
        framework_info = LOOKUP_TABLES['frameworks'][framework_id]
        framework_code = framework_info.get('FrameworkCode', '')
        if framework_code:
            return f"EBA_{framework_code}"

    # Method 5: Fallback to module code analysis
    module_code = row.get('ModuleCode', '')
    if module_code:
        module_code_upper = module_code.upper()
        if 'FINREP' in module_code_upper:
            return 'EBA_FINREP'
        elif 'COREP' in module_code_upper:
            return 'EBA_COREP'
        elif 'AE' in module_code_upper:
            return 'EBA_AE'
        elif 'FP' in module_code_upper:
            return 'EBA_FP'
        elif 'SBP' in module_code_upper:
            return 'EBA_SBP'
        elif 'REM' in module_code_upper:
            return 'EBA_REM'
        elif 'RES' in module_code_upper:
            return 'EBA_RES'
        elif 'PAY' in module_code_upper:
            return 'EBA_PAY'

    return 'EBA_UNK'

def build_axis_id(row):
    """
    Build AXIS_ID following EBA technical export pattern.
    Enhanced to use Template.csv information for accurate framework/table code resolution.
    Examples: EBA_AE_EBA_A_00.01_AE_3.2_1, EBA_FINREP_EBA_F_32.01_FINREP_2.9_2

    Pattern: EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}
    """
    try:
        # Get basic axis information
        axis_id = row.get('AxisID', '')
        table_vid = row.get('TableVID', '')
        axis_order = row.get('AxisOrder', row.get('ORDER', ''))
        axis_orientation = row.get('AxisOrientation', row.get('ORIENTATION', ''))

        # Debug: ensure we have the axis ID
        if not axis_id:
            return "EBA_AXIS_UNK"

        # Step 1: Resolve framework and version using enhanced lookup functions
        framework = lookup_framework_from_template(row)
        if framework == 'UNK':
            framework = 'FINREP'  # Default fallback

        # Step 2: Resolve table code and version from template information
        table_code = 'UNK'
        version = '2.9'  # Default version

        # Try to get table code from TableVersion lookup
        if table_vid and table_vid in LOOKUP_TABLES['table_versions']:
            table_version_info = LOOKUP_TABLES['table_versions'][table_vid]
            template_code = table_version_info.get('TableVersionCode', '')
            if template_code:
                table_code = extract_table_code_from_template(template_code)
                # Extract version from the TableVersion if available
                from_date = table_version_info.get('FromDate', '')
                if from_date and '/' in from_date:
                    # Derive version from date - this is an approximation
                    year = from_date.split('/')[-1].split(' ')[0]
                    if len(year) == 2:
                        year = '20' + year if int(year) < 50 else '19' + year
                    year_int = int(year)
                    if year_int >= 2023:
                        version = '3.2'
                    elif year_int >= 2021:
                        version = '3.0'
                    elif year_int >= 2018:
                        version = '2.9'
                    else:
                        version = '2.8'

        # Fallback to template lookup from row
        if table_code == 'UNK':
            template_id = row.get('TemplateID')
            if template_id and template_id in LOOKUP_TABLES['templates']:
                template_info = LOOKUP_TABLES['templates'][template_id]
                template_code = template_info.get('TemplateCode', '')
                if template_code:
                    table_code = extract_table_code_from_template(template_code)

        # Final fallback
        if table_code == 'UNK':
            table_code = f"TABLE_{table_vid}" if table_vid else f"AXIS_{axis_id}"

        # Step 3: Extract framework-specific version if available
        if framework == 'AE':
            # Asset Encumbrance specific versions
            if 'AE_3.2' in str(row):
                version = '3.2'
            elif 'AE_3.0' in str(row):
                version = '3.0'
            elif 'AE_2.8' in str(row):
                version = '2.8'
        elif framework == 'FINREP':
            # FINREP versions based on context
            if table_vid and int(table_vid) > 2000:
                version = '2.9'
            else:
                version = ''  # Older FINREP may not have version in ID

        # Step 4: Determine axis number based on orientation
        if axis_orientation:
            orientation_upper = str(axis_orientation).upper()
            if orientation_upper == 'X':
                axis_number = '1'
            elif orientation_upper == 'Y':
                axis_number = '2'
            elif orientation_upper == 'Z':
                axis_number = '3'
            else:
                axis_number = str(axis_order) if axis_order else '1'
        else:
            axis_number = str(axis_order) if axis_order else '1'

        # Step 5: Build EBA axis ID following the pattern
        if version:
            axis_id_final = f"EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}"
        else:
            # For older frameworks without version
            axis_id_final = f"EBA_{framework}_EBA_{table_code}_{framework}_{axis_number}"

        # Sanitize final ID
        axis_id_final = sanitize_id_component(axis_id_final, 255)

        return axis_id_final

    except Exception as e:
        log_transformation_warning(f"Error building AXIS_ID: {e}", row)
        axis_id = row.get('AxisID', 'UNK')
        return f"EBA_AXIS_{axis_id}"

def build_axis_ordinate_id(row):
    """
    Build AXIS_ORDINATE_ID following EBA technical export pattern.
    Enhanced to use Template.csv information for accurate framework/table code resolution.
    Examples: EBA_AE_EBA_F_35.00.a_AE_3.2_3_, EBA_FINREP_EBA_F_40.02_FINREP_2_090

    Pattern: EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}_{ordinate_code}
    """
    try:
        # Get basic ordinate information
        ordinate_id = row.get('OrdinateID', '')
        axis_id = row.get('AxisID', '')
        ordinate_code = row.get('OrdinateCode', row.get('CODE', ''))
        table_vid = row.get('TableVID', '')

        # Step 1: Resolve framework and version using enhanced lookup functions
        framework = lookup_framework_from_template(row)
        if framework == 'UNK':
            framework = 'FINREP'  # Default fallback

        # Step 2: Resolve table code and version from template information
        table_code = 'UNK'
        version = '2.9'  # Default version

        # Try to get table code from TableVersion lookup
        if table_vid and table_vid in LOOKUP_TABLES['table_versions']:
            table_version_info = LOOKUP_TABLES['table_versions'][table_vid]
            template_code = table_version_info.get('TableVersionCode', '')
            if template_code:
                table_code = extract_table_code_from_template(template_code)
                # Extract version from the TableVersion if available
                from_date = table_version_info.get('FromDate', '')
                if from_date and '/' in from_date:
                    # Derive version from date - this is an approximation
                    year = from_date.split('/')[-1].split(' ')[0]
                    if len(year) == 2:
                        year = '20' + year if int(year) < 50 else '19' + year
                    year_int = int(year)
                    if year_int >= 2023:
                        version = '3.2'
                    elif year_int >= 2021:
                        version = '3.0'
                    elif year_int >= 2018:
                        version = '2.9'
                    else:
                        version = '2.8'

        # Fallback to template lookup from row
        if table_code == 'UNK':
            template_id = row.get('TemplateID')
            if template_id and template_id in LOOKUP_TABLES['templates']:
                template_info = LOOKUP_TABLES['templates'][template_id]
                template_code = template_info.get('TemplateCode', '')
                if template_code:
                    table_code = extract_table_code_from_template(template_code)

        # Final fallback
        if table_code == 'UNK':
            table_code = f"TABLE_{axis_id}" if axis_id else f"ORD_{ordinate_id}"

        # Step 3: Extract framework-specific version if available
        if framework == 'AE':
            # Asset Encumbrance specific versions
            if 'AE_3.2' in str(row):
                version = '3.2'
            elif 'AE_3.0' in str(row):
                version = '3.0'
            elif 'AE_2.8' in str(row):
                version = '2.8'
        elif framework == 'FINREP':
            # FINREP versions based on context
            if table_vid and int(table_vid) > 2000:
                version = '2.9'
            else:
                version = ''  # Older FINREP may not have version in ID

        # Step 4: Determine axis number from axis lookup or row data
        axis_number = '1'  # Default
        axis_orientation = row.get('AxisOrientation', '')

        # Try to get axis info from lookup tables if available
        try:
            if axis_id and 'axes' in LOOKUP_TABLES and axis_id in LOOKUP_TABLES['axes']:
                axis_info = LOOKUP_TABLES['axes'][axis_id]
                axis_orientation = axis_info.get('AxisOrientation', axis_orientation)
        except KeyError:
            # LOOKUP_TABLES may not be fully initialized yet
            pass

        if axis_orientation:
            orientation_upper = str(axis_orientation).upper()
            if orientation_upper == 'X':
                axis_number = '1'
            elif orientation_upper == 'Y':
                axis_number = '2'
            elif orientation_upper == 'Z':
                axis_number = '3'
            else:
                # Try to extract from axis order
                axis_order = row.get('AxisOrder', '')
                if axis_order:
                    axis_number = str(axis_order)

        # Step 5: Handle ordinate code (can be empty for open axes or abstract headers)
        ordinate_suffix = f"_{ordinate_code}" if ordinate_code and ordinate_code.strip() else "_"

        # Step 6: Build EBA axis ordinate ID following the pattern
        if version:
            axis_ordinate_id_final = f"EBA_{framework}_EBA_{table_code}_{framework}_{version}_{axis_number}{ordinate_suffix}"
        else:
            # For older frameworks without version
            axis_ordinate_id_final = f"EBA_{framework}_EBA_{table_code}_{framework}_{axis_number}{ordinate_suffix}"

        # Sanitize final ID
        axis_ordinate_id_final = sanitize_id_component(axis_ordinate_id_final, 255)

        return axis_ordinate_id_final

    except Exception as e:
        log_transformation_warning(f"Error building AXIS_ORDINATE_ID: {e}", row)
        ordinate_id = row.get('OrdinateID', 'UNK')
        return f"EBA_ORDINATE_{ordinate_id}"

def build_cell_id(row):
    """
    Build CELL_ID following EBA technical export pattern.
    Simplified to match the ground truth pattern from technical exports.
    Examples: EBA_73938, EBA_71113, EBA_65779

    Pattern: EBA_{cell_number}
    """
    try:
        cell_id = row.get('CellID', '')

        if cell_id and str(cell_id).strip():
            # Use the cell ID directly with EBA prefix - this is the primary pattern
            return f"EBA_{cell_id}"
        else:
            # Generate a sequential cell ID if none provided
            # This should be based on available identifiers or context
            data_point_vid = row.get('DataPointVID', '')
            table_cell_id = row.get('TableCellID', '')

            if data_point_vid:
                # Use DataPointVID as cell identifier
                return f"EBA_{data_point_vid}"
            elif table_cell_id:
                # Use TableCellID as cell identifier
                return f"EBA_{table_cell_id}"
            else:
                # Generate from available context - this is a fallback
                table_vid = row.get('TableVID', '')
                ordinate_id = row.get('OrdinateID', '')

                if table_vid and ordinate_id:
                    # Combine table and ordinate for unique cell ID
                    combined_id = f"{table_vid}{ordinate_id}"
                    # Use hash to create consistent numeric ID
                    import hashlib
                    hash_obj = hashlib.md5(combined_id.encode())
                    numeric_id = int(hash_obj.hexdigest()[:6], 16)  # Use first 6 hex chars as number
                    return f"EBA_{numeric_id}"
                elif table_vid:
                    # Use table VID with padding
                    return f"EBA_{int(table_vid) * 1000 if table_vid.isdigit() else 999999}"
                else:
                    # Final fallback - generate a random-ish ID
                    import random
                    fallback_id = random.randint(100000, 999999)
                    return f"EBA_{fallback_id}"

    except Exception as e:
        log_transformation_warning(f"Error building CELL_ID: {e}", row)
        # Even in error case, provide a valid EBA cell ID
        import random
        error_id = random.randint(100000, 999999)
        return f"EBA_{error_id}"

def build_template_cube_id(row):
    """
    Build CUBE_STRUCTURE_ID from Template information following EBA pattern.
    Enhanced to use Template.csv as source and match technical export ground truth.
    Examples: EBA_FINREP_EBA_F_08.01_FINREP, EBA_AE_EBA_A_00.01_AE_3.2

    Pattern: EBA_{framework}_EBA_{table_code}_{framework}_{version} (matches technical export)
    """
    try:
        # Step 1: Get template information
        template_id = row.get('TemplateID', '')
        template_code = row.get('TemplateCode', '')

        # Step 2: Resolve framework from template
        framework = lookup_framework_from_template(row)
        if framework == 'UNK':
            # Try to derive from template code
            if template_code:
                framework, _ = _extract_framework_version_from_table_code(template_code)

        # Step 3: Extract and normalize table code
        if template_code:
            table_code = extract_table_code_from_template(template_code)
        else:
            table_code = f"TEMPLATE_{template_id}" if template_id else "UNK"

        # Step 4: Determine version based on framework and context
        version = ''
        if framework == 'AE':
            # Asset Encumbrance versions
            version = '3.2'  # Default to latest
        elif framework == 'FINREP':
            # FINREP may or may not have version in cube ID
            version = ''  # Match technical export pattern
        elif framework == 'COREP':
            # COREP versions
            version = ''  # Match technical export pattern

        # Step 5: Build EBA cube structure ID following technical export pattern
        if framework != 'UNK' and table_code != 'UNK':
            if version:
                cube_id = f"EBA_{framework}_EBA_{table_code}_{framework}_{version}"
            else:
                cube_id = f"EBA_{framework}_EBA_{table_code}_{framework}"
        else:
            # Fallback with template info
            cube_id = f"EBA_CUBE_{template_id}" if template_id else "EBA_CUBE_UNK"

        # Sanitize final ID
        cube_id = sanitize_id_component(cube_id, 255)

        return cube_id

    except Exception as e:
        log_transformation_warning(f"Error building CUBE_STRUCTURE_ID: {e}", row)
        template_id = row.get('TemplateID', 'UNK')
        return f"EBA_CUBE_{template_id}"

def build_member_id(row):
    """
    Build MEMBER_ID following EBA technical export pattern.
    Pattern: EBA_{domain_code}_EBA_{member_code}
    Example: EBA_CS_EBA_x1, EBA_BT_EBA_x5
    """
    domain_id = row.get('DomainID', '')
    member_code = row.get('MemberCode', '')

    # Get domain code - need to look up domain table
    domain_code = 'UNK'
    if domain_id:
        try:
            # Convert domain_id to int for lookup
            domain_id_int = int(domain_id)
            if domain_id_int in LOOKUP_TABLES.get('domains', {}):
                domain_info = LOOKUP_TABLES['domains'][domain_id_int]
                domain_code = domain_info.get('DomainCode', str(domain_id))
            else:
                domain_code = str(domain_id)  # Fallback to domain ID
        except (ValueError, TypeError):
            domain_code = str(domain_id)  # Fallback to original domain ID

    # Clean member code
    if not member_code:
        member_id = row.get('MemberID', 'UNK')
        member_code = f"x{member_id}"

    return f"EBA_{domain_code}_EBA_{member_code}"

def build_variable_id(row):
    """
    Build VARIABLE_ID following EBA technical export pattern.
    Pattern: EBA_{variable_code}
    Example: EBA_DPS, EBA_MCF
    """
    variable_code = row.get('VariableCode', row.get('DimensionCode', ''))

    if not variable_code:
        variable_id = row.get('VariableID', row.get('DimensionID', 'UNK'))
        variable_code = f"VAR_{variable_id}"

    return f"EBA_{variable_code}"

def extract_variable_from_context(row):
    """
    Extract the first variable from context key.
    Context format: "ALO1799APL2563BAS1506MCY2059"
    Returns first variable code (e.g., "EBA_ALO")
    """
    context_key = row.get('ContextKey', '')
    if not context_key:
        return None

    # Extract first variable code (typically 3 letters at start)
    import re
    match = re.match(r'^([A-Z]{2,4})', context_key)
    if match:
        var_code = match.group(1)
        return f"EBA_{var_code}"

    return None

def extract_member_from_context(row):
    """
    Extract the first member from XBRL context.
    XBRL format: "ALO=eba_IM:x1,APL=eba_PL:x4,BAS=eba_BA:x6,MCY=eba_MC:x143"
    Returns first member ID (e.g., "EBA_IM_EBA_x1")
    """
    xbrl_context = row.get('XbrlContextKey', '')
    if not xbrl_context:
        return None

    # Extract first member from XBRL context
    import re
    match = re.search(r'eba_([A-Z]+):([^,]+)', xbrl_context)
    if match:
        domain_code = match.group(1)
        member_code = match.group(2)
        return f"EBA_{domain_code}_EBA_{member_code}"

    return None

def build_cube_id_from_module(row):
    """
    Build CUBE_ID from module information.
    Uses the existing cube ID building logic but with module data.
    """
    module_id = row.get('ModuleID', '')
    # Use module ID to generate cube ID following EBA pattern
    if module_id and module_id in LOOKUP_TABLES.get('modules', {}):
        module_info = LOOKUP_TABLES['modules'][module_id]
        return module_info.get('CubeID', f"EBA_MODULE_{module_id}")
    return f"EBA_MODULE_{module_id}"

def build_table_id_from_table_vid(row):
    """
    Build TABLE_ID from TableVID.
    Looks up the table version to get proper table ID.
    """
    table_vid = row.get('TableVID', '')
    # Use table VID to look up proper table ID
    if table_vid and table_vid in LOOKUP_TABLES.get('table_versions', {}):
        table_info = LOOKUP_TABLES['table_versions'][table_vid]
        return table_info.get('TableID', f"EBA_TABLE_{table_vid}")
    return f"EBA_TABLE_{table_vid}"

def build_template_id(row, owner_prefix=None, framework_code=None, template_code=None):
    """
    Build CUBE_STRUCTURE_ID following pattern: {OWNER_PREFIX}_{FRAMEWORK}_{TEMPLATE_CODE}
    Example: EBA_FINREP_F_05.01
    """
    owner = (owner_prefix or lookup_owner_from_template(row) or 'NODE').upper()
    framework = (framework_code or lookup_framework_from_template(row) or 'UNK').upper()
    template = normalize_template_code(template_code or row.get('TemplateCode', f"T_{row.get('TemplateID', 'UNK')}"))

    return f"{owner}_{framework}_{template}"

# ==================== LOOKUP HELPER FUNCTIONS ====================

def lookup_owner_prefix_from_table(row):
    """Lookup owner prefix through Table -> Template -> Framework -> Owner chain"""
    template_id = row.get('TemplateID')
    if template_id:
        try:
            template_id_int = int(template_id)
            if template_id_int in LOOKUP_TABLES['template_framework']:
                framework_id = LOOKUP_TABLES['template_framework'][template_id_int]
                framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
                framework_code = framework_info.get('FrameworkCode', '')
                if 'FINREP' in framework_code or 'COREP' in framework_code:
                    return 'EBA'
        except (ValueError, TypeError):
            if template_id in LOOKUP_TABLES['template_framework']:
                framework_id = LOOKUP_TABLES['template_framework'][template_id]
                framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
                framework_code = framework_info.get('FrameworkCode', '')
                if 'FINREP' in framework_code or 'COREP' in framework_code:
                    return 'EBA'
    return 'NODE'

def lookup_framework_from_table(row):
    """Lookup framework code through Table -> Template -> Framework chain"""
    template_id = row.get('TemplateID')
    if template_id:
        try:
            template_id_int = int(template_id)
            if template_id_int in LOOKUP_TABLES['template_framework']:
                framework_id = LOOKUP_TABLES['template_framework'][template_id_int]
                framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
                return framework_info.get('FrameworkCode', 'UNK')
        except (ValueError, TypeError):
            if template_id in LOOKUP_TABLES['template_framework']:
                framework_id = LOOKUP_TABLES['template_framework'][template_id]
                framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
                return framework_info.get('FrameworkCode', 'UNK')
    return 'UNK'

def lookup_template_code(row):
    """Lookup template code from Template table"""
    template_id = row.get('TemplateID')
    if template_id:
        # Handle string/int conversion
        try:
            template_id_int = int(template_id)
            template_info = LOOKUP_TABLES['templates'].get(template_id_int, {})
            return template_info.get('TemplateCode', '')
        except (ValueError, TypeError):
            template_info = LOOKUP_TABLES['templates'].get(template_id, {})
            return template_info.get('TemplateCode', '')
    return ''

def lookup_version_from_table(row):
    """
    Enhanced version lookup using TableVersion data.
    Returns framework version string like 'FINREP 2.9', 'AE 3.2', 'COREP 3.2'
    """
    # Strategy 1: Use TableVID if available (for TableVersion exports)
    table_vid = row.get('TableVID')
    if table_vid:
        try:
            table_vid_int = int(table_vid)
            if table_vid_int in LOOKUP_TABLES['table_versions']:
                table_version_info = LOOKUP_TABLES['table_versions'][table_vid_int]
                framework = table_version_info.get('Framework', 'UNK')
                version = table_version_info.get('Version', '1.0')
                return f"{framework} {version}"
        except (ValueError, TypeError):
            pass

    # Strategy 2: Use TableVersionCode directly
    table_version_code = row.get('TableVersionCode', '').strip('"')
    if table_version_code:
        framework, version = _extract_framework_version_from_table_code(table_version_code)
        return f"{framework} {version}"

    # Strategy 3: Try framework -> taxonomy lookup
    template_id = row.get('TemplateID')
    if template_id and template_id in LOOKUP_TABLES['template_framework']:
        framework_id = LOOKUP_TABLES['template_framework'][template_id]
        # Find taxonomy for this framework
        for tax_id, tax_info in LOOKUP_TABLES['taxonomies'].items():
            if tax_info.get('FrameworkID') == framework_id:
                taxonomy_code = tax_info.get('TaxonomyCode', '')
                # Extract meaningful version from taxonomy code
                if 'AE' in taxonomy_code:
                    return 'AE 3.2'  # Standard AE version from technical export
                elif 'FINREP' in taxonomy_code:
                    return 'FINREP 2.9'  # Standard FINREP version
                elif 'COREP' in taxonomy_code:
                    return 'COREP 3.2'  # Standard COREP version
                else:
                    return tax_info.get('DpmPackageCode', '1.0')

    # Strategy 4: Fallback to table code pattern analysis
    table_code = row.get('OriginalTableCode', row.get('Code', ''))
    if table_code:
        framework, version = _extract_framework_version_from_table_code(table_code)
        return f"{framework} {version}"

    return '1.0'

def lookup_owner_prefix_from_module(row):
    """Lookup owner prefix from module through framework"""
    framework_id = row.get('FrameworkID')
    if framework_id:
        framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
        framework_code = framework_info.get('FrameworkCode', '')
        if 'FINREP' in framework_code or 'COREP' in framework_code:
            return 'EBA'
    return 'NODE'

def lookup_framework_from_module(row):
    """Lookup framework code from module"""
    framework_id = row.get('FrameworkID')
    if framework_id:
        framework_info = LOOKUP_TABLES['frameworks'].get(framework_id, {})
        return framework_info.get('FrameworkCode', 'UNK')
    return 'UNK'

def lookup_version_from_module(row):
    """Lookup version from module through framework -> taxonomy"""
    framework_id = row.get('FrameworkID')
    taxonomy_id = row.get('TaxonomyID')

    # First try direct taxonomy lookup if available
    if taxonomy_id:
        # Handle both string and integer taxonomy IDs
        try:
            tax_id_int = int(taxonomy_id)
            if tax_id_int in LOOKUP_TABLES['taxonomies']:
                tax_info = LOOKUP_TABLES['taxonomies'][tax_id_int]
                taxonomy_code = tax_info.get('TaxonomyCode', '')
                if taxonomy_code:
                    return taxonomy_code  # This gives us "AE 3.2", "FINREP", "COREP", etc.
        except (ValueError, TypeError):
            if taxonomy_id in LOOKUP_TABLES['taxonomies']:
                tax_info = LOOKUP_TABLES['taxonomies'][taxonomy_id]
                taxonomy_code = tax_info.get('TaxonomyCode', '')
                if taxonomy_code:
                    return taxonomy_code

    # Fallback to framework-based lookup (find latest taxonomy for framework)
    if framework_id:
        latest_version = '1.0'
        latest_tax_code = ''
        for tax_id, tax_info in LOOKUP_TABLES['taxonomies'].items():
            if tax_info.get('FrameworkID') == framework_id:
                tax_code = tax_info.get('TaxonomyCode', '')
                dpm_code = tax_info.get('DpmPackageCode', '1.0')
                if tax_code and (not latest_tax_code or dpm_code > latest_version):
                    latest_version = dpm_code
                    latest_tax_code = tax_code
        if latest_tax_code:
            return latest_tax_code

    return '1.0'

def build_module_code(row):
    """Build module code from module information"""
    module_id = row.get('ModuleID')
    module_code = row.get('ModuleCode', row.get('Code', ''))  # Use ModuleCode first, then fallback to Code
    if module_code:
        return normalize_template_code(module_code)
    return f"MOD_{module_id}" if module_id else 'MOD_UNK'

def lookup_owner_from_template(row):
    """Lookup owner from template (default to NODE for now)"""
    return 'NODE'

def lookup_framework_from_template(row):
    """
    Lookup framework from template context using multiple resolution methods.

    Args:
        row (dict): Data row containing template/framework information

    Returns:
        str: Framework code (e.g., 'FINREP', 'COREP', 'AE')
    """
    # Method 1: Direct template lookup via TemplateID
    template_id = row.get('TemplateID')
    if template_id and template_id in LOOKUP_TABLES['templates']:
        template_code = LOOKUP_TABLES['templates'][template_id].get('TemplateCode', '')
        if template_code:
            framework, _ = _extract_framework_version_from_table_code(template_code)
            return framework

    # Method 2: Direct template code analysis
    template_code = row.get('TemplateCode', '')
    if template_code:
        framework, _ = _extract_framework_version_from_table_code(template_code)
        return framework

    # Method 3: Framework ID lookup
    framework_id = row.get('FrameworkID')
    if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
        framework_code = LOOKUP_TABLES['frameworks'][framework_id].get('FrameworkCode', '')
        if framework_code:
            return framework_code

    # Method 4: Taxonomy-based framework resolution
    taxonomy_id = row.get('TaxonomyID')
    if taxonomy_id and taxonomy_id in LOOKUP_TABLES['taxonomies']:
        taxonomy_info = LOOKUP_TABLES['taxonomies'][taxonomy_id]
        tax_framework_id = taxonomy_info.get('FrameworkID')
        if tax_framework_id and tax_framework_id in LOOKUP_TABLES['frameworks']:
            framework_code = LOOKUP_TABLES['frameworks'][tax_framework_id].get('FrameworkCode', '')
            if framework_code:
                return framework_code

    # Method 5: Module-based framework resolution
    module_id = row.get('ModuleID')
    if module_id and module_id in LOOKUP_TABLES['modules']:
        module_info = LOOKUP_TABLES['modules'][module_id]
        framework_code = module_info.get('FrameworkCode', '')
        if framework_code:
            return framework_code

    # Method 6: Code field analysis
    for field in ['Code', 'TemplateCode', 'TableCode', 'ModuleCode']:
        code = row.get(field, '')
        if code:
            framework, _ = _extract_framework_version_from_table_code(code)
            if framework != 'UNK':
                return framework

    return 'UNK'

def populate_lookup_tables(owner_data=None, framework_data=None, template_data=None, taxonomy_data=None):
    """
    Populate global lookup tables with reference data.
    This should be called before processing dependent mappings.
    """
    if owner_data:
        for row in owner_data:
            owner_id = row.get('OwnerID')
            if owner_id:
                LOOKUP_TABLES['owners'][owner_id] = {
                    'OwnerPrefix': row.get('OwnerPrefix', 'NODE').upper(),
                    'OwnerName': row.get('OwnerName', '')
                }

    if framework_data:
        for row in framework_data:
            framework_id = row.get('FrameworkID')
            if framework_id:
                LOOKUP_TABLES['frameworks'][framework_id] = {
                    'FrameworkCode': row.get('FrameworkCode', ''),
                    'FrameworkLabel': row.get('FrameworkLabel', '')
                }

    if template_data:
        for row in template_data:
            template_id = row.get('TemplateID')
            if template_id:
                LOOKUP_TABLES['templates'][template_id] = {
                    'TemplateCode': row.get('TemplateCode', ''),
                    'TemplateLabel': row.get('TemplateLabel', '')
                }

    if taxonomy_data:
        for row in taxonomy_data:
            taxonomy_id = row.get('TaxonomyID')
            if taxonomy_id:
                LOOKUP_TABLES['taxonomies'][taxonomy_id] = {
                    'DpmPackageCode': row.get('DpmPackageCode', '1.0'),
                    'FrameworkID': row.get('FrameworkID'),
                    'TaxonomyCode': row.get('TaxonomyCode', '')
                }

def extract_table_code_from_template(template_code):
    """
    Extract proper table code from template code for EBA ID generation.

    Args:
        template_code (str): Template code like 'F 08.01', 'C 01.00', 'A 00.01'

    Returns:
        str: Normalized table code like 'F_08.01', 'C_01.00', 'A_00.01'
    """
    if not template_code:
        return 'UNK'

    # Clean and normalize the template code
    code = str(template_code).strip().strip('"').strip("'")

    # Replace spaces with underscores and normalize format
    normalized = code.replace(' ', '_')

    # Ensure proper format for EBA IDs
    if normalized and not normalized.startswith(('F_', 'C_', 'A_', 'B_', 'D_', 'G_')):
        # Handle cases where template code might not have standard prefix
        if code.upper().startswith('F '):
            normalized = code.replace(' ', '_')
        elif code.upper().startswith('C '):
            normalized = code.replace(' ', '_')
        elif code.upper().startswith('A '):
            normalized = code.replace(' ', '_')

    return normalized

@lru_cache(maxsize=1000)
def _extract_framework_version_from_table_code(table_code):
    """
    Extract framework and version information from table code.

    Args:
        table_code (str): Table code like 'F 01.01', 'C 01.00', 'A 00.01'

    Returns:
        tuple: (framework, version) like ('FINREP', '2.9'), ('COREP', '3.2'), ('AE', '3.2')
    """
    if not table_code:
        return 'UNK', '1.0'

    # Clean the table code
    code = table_code.strip().upper()

    # Determine framework based on first character
    if code.startswith('F '):
        framework = 'FINREP'
        version = '2.9'  # Standard FINREP version
    elif code.startswith('C '):
        framework = 'COREP'
        version = '3.2'  # Standard COREP version
    elif code.startswith('A '):
        framework = 'AE'
        version = '3.2'  # Standard AE version
    elif code.startswith('B '):
        framework = 'COREP'  # B tables are part of COREP
        version = '3.2'
    elif code.startswith('D '):
        framework = 'FINREP'  # D tables are part of FINREP
        version = '2.9'
    elif code.startswith('G '):
        framework = 'FINREP'  # G tables are part of FINREP
        version = '2.9'
    elif code.startswith('I '):
        framework = 'FINREP'  # I tables are part of FINREP
        version = '2.9'
    elif code.startswith('J '):
        framework = 'FINREP'  # J tables are part of FINREP
        version = '2.9'
    elif code.startswith('K '):
        framework = 'COREP'  # K tables are part of COREP
        version = '3.2'
    elif code.startswith('L '):
        framework = 'COREP'  # L tables are part of COREP
        version = '3.2'
    elif code.startswith('M '):
        framework = 'COREP'  # M tables are part of COREP
        version = '3.2'
    elif code.startswith('N '):
        framework = 'COREP'  # N tables are part of COREP
        version = '3.2'
    elif code.startswith('P '):
        framework = 'COREP'  # P tables are part of COREP
        version = '3.2'
    elif code.startswith('Q '):
        framework = 'COREP'  # Q tables are part of COREP
        version = '3.2'
    elif code.startswith('R '):
        framework = 'COREP'  # R tables are part of COREP
        version = '3.2'
    elif code.startswith('S '):
        framework = 'SHS'  # Securities holdings statistics
        version = '2.0'
    elif code.startswith('T '):
        framework = 'COREP'  # T tables are part of COREP
        version = '3.2'
    elif code.startswith('U '):
        framework = 'COREP'  # U tables are part of COREP
        version = '3.2'
    elif code.startswith('Y '):
        framework = 'COREP'  # Y tables are part of COREP
        version = '3.2'
    elif code.startswith('Z '):
        framework = 'COREP'  # Z tables are part of COREP
        version = '3.2'
    else:
        framework = 'UNK'
        version = '1.0'

    return framework, version

def auto_populate_lookup_tables(target_directory):
    """
    Automatically populate lookup tables by reading CSV files from target directory.
    This provides a more convenient way to initialize lookup tables.

    Args:
        target_directory (str): Path to directory containing DPM CSV files

    Returns:
        dict: Summary of loaded records
    """
    import csv
    import os

    summary = {'loaded': 0, 'errors': []}

    # Clear existing lookup tables
    for table in LOOKUP_TABLES.values():
        table.clear()

    try:
        # Load Owner data
        owner_file = os.path.join(target_directory, 'Owner.csv')
        if os.path.exists(owner_file):
            with open(owner_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        owner_id = int(row.get('OwnerID', 0))
                        if owner_id:
                            LOOKUP_TABLES['owners'][owner_id] = {
                                'OwnerPrefix': (row.get('OwnerPrefix') or 'NODE').upper(),
                                'OwnerName': row.get('OwnerName', '')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Owner row error: {e}")

        # Load Framework data
        framework_file = os.path.join(target_directory, 'ReportingFramework.csv')
        if os.path.exists(framework_file):
            with open(framework_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        framework_id = int(row.get('FrameworkID', 0))
                        if framework_id:
                            LOOKUP_TABLES['frameworks'][framework_id] = {
                                'FrameworkCode': row.get('FrameworkCode', ''),
                                'FrameworkLabel': row.get('FrameworkLabel', '')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Framework row error: {e}")

        # Load Template data
        template_file = os.path.join(target_directory, 'Template.csv')
        if os.path.exists(template_file):
            with open(template_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        template_id = int(row.get('TemplateID', 0))
                        if template_id:
                            LOOKUP_TABLES['templates'][template_id] = {
                                'TemplateCode': row.get('TemplateCode', ''),
                                'TemplateLabel': row.get('TemplateLabel', '')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Template row error: {e}")

        # Load Taxonomy data
        taxonomy_file = os.path.join(target_directory, 'Taxonomy.csv')
        if os.path.exists(taxonomy_file):
            with open(taxonomy_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        taxonomy_id = int(row.get('TaxonomyID', 0))
                        framework_id = int(row.get('FrameworkID', 0)) if row.get('FrameworkID') else None
                        if taxonomy_id:
                            LOOKUP_TABLES['taxonomies'][taxonomy_id] = {
                                'DpmPackageCode': row.get('DpmPackageCode', '1.0'),
                                'FrameworkID': framework_id,
                                'TaxonomyCode': row.get('TaxonomyCode', '')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Taxonomy row error: {e}")

        # Load Domain data
        domain_file = os.path.join(target_directory, 'Domain.csv')
        if os.path.exists(domain_file):
            with open(domain_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        domain_id = int(row.get('DomainID', 0))
                        if domain_id:
                            LOOKUP_TABLES['domains'][domain_id] = {
                                'DomainCode': row.get('DomainCode', ''),
                                'DomainLabel': row.get('DomainLabel', '')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Domain row error: {e}")

        # Load TableVersion data
        table_version_file = os.path.join(target_directory, 'TableVersion.csv')
        if os.path.exists(table_version_file):
            with open(table_version_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        table_vid = int(row.get('TableVID', 0))
                        if table_vid:
                            # Parse dates from MM/DD/YY HH:MM:SS format
                            from_date = transform_date(row.get('FromDate', ''),row)
                            to_date = transform_date(row.get('ToDate', ''),row)

                            # Extract framework and version from table code
                            table_code = row.get('TableVersionCode', '').strip('"')
                            framework, version = _extract_framework_version_from_table_code(table_code)

                            LOOKUP_TABLES['table_versions'][table_vid] = {
                                'TableVersionCode': table_code,
                                'TableVersionLabel': row.get('TableVersionLabel', '').strip('"'),
                                'FromDate': from_date,
                                'ToDate': to_date,
                                'Framework': framework,
                                'Version': version,
                                'XbrlTableCode': row.get('XbrlTableCode', '').strip('"'),
                                'XbrlFilingIndicatorCode': row.get('XbrlFilingIndicatorCode', '').strip('"')
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"TableVersion row error: {e}")

        # Load Module data
        module_file = os.path.join(target_directory, 'Module.csv')
        if os.path.exists(module_file):
            with open(module_file, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    try:
                        module_id = int(row.get('ModuleID', 0))
                        taxonomy_id = int(row.get('TaxonomyID', 0)) if row.get('TaxonomyID') else None
                        conceptual_module_id = int(row.get('ConceptualModuleID', 0)) if row.get('ConceptualModuleID') else None

                        if module_id:
                            # Get framework info through taxonomy relationship
                            framework_id = None
                            framework_code = ''

                            if taxonomy_id and taxonomy_id in LOOKUP_TABLES['taxonomies']:
                                taxonomy_info = LOOKUP_TABLES['taxonomies'][taxonomy_id]
                                framework_id = taxonomy_info.get('FrameworkID')

                                if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
                                    framework_info = LOOKUP_TABLES['frameworks'][framework_id]
                                    framework_code = framework_info.get('FrameworkCode', '')

                            LOOKUP_TABLES['modules'][module_id] = {
                                'ModuleCode': row.get('ModuleCode', '').strip('"'),
                                'ModuleLabel': row.get('ModuleLabel', '').strip('"'),
                                'TaxonomyID': taxonomy_id,
                                'ConceptualModuleID': conceptual_module_id,
                                'Version': row.get('Version', '').strip('"'),
                                'XbrlSchemaRef': row.get('XbrlSchemaRef', '').strip('"'),
                                'FromDate': transform_date(row.get('FromDate', ''),row),
                                'ToDate': transform_date(row.get('ToDate', ''),row),
                                'isDocumentModule': transform_boolean(row.get('isDocumentModule', '0')),
                                'FrameworkID': framework_id,
                                'FrameworkCode': framework_code
                            }
                            summary['loaded'] += 1
                    except (ValueError, TypeError) as e:
                        summary['errors'].append(f"Module row error: {e}")

        # Enhanced template->framework relationships with comprehensive mapping
        _build_enhanced_template_framework_relationships()

    except Exception as e:
        summary['errors'].append(f"General error: {e}")

    return summary

def _build_enhanced_template_framework_relationships():
    """
    Build enhanced template-framework relationships using multiple data sources.
    This provides more accurate mapping than simple heuristics.
    """
    # Clear existing relationships
    LOOKUP_TABLES['template_framework'].clear()

    # Strategy 1: Use actual taxonomy data to link templates to frameworks
    for taxonomy_id, taxonomy_info in LOOKUP_TABLES['taxonomies'].items():
        framework_id = taxonomy_info.get('FrameworkID')
        if framework_id and framework_id in LOOKUP_TABLES['frameworks']:
            # Find templates that belong to this taxonomy/framework
            # This would be enhanced with actual TaxonomyTableVersion data
            pass

    # Strategy 2: Enhanced pattern matching with edge cases
    framework_patterns = {
        'FINREP': ['F ', 'FINREP'],
        'COREP': ['C ', 'COREP'],
        'AE': ['AE ', 'ASSET'],
        'SHS': ['SHS', 'SECURITIES'],
        'REM': ['REM', 'REMUNERATION'],
        'ESG': ['ESG', 'ENVIRONMENTAL'],
        'MREL': ['MREL', 'TLAC'],
        'SBP': ['SBP', 'BENCHMARKING'],
        'DORA': ['DORA', 'OPERATIONAL'],
        'IRRBB': ['IRRBB', 'INTEREST_RATE']
    }

    # Build reverse lookup from framework code to ID
    framework_code_to_id = {}
    for fw_id, fw_info in LOOKUP_TABLES['frameworks'].items():
        fw_code = fw_info.get('FrameworkCode', '')
        if fw_code:
            framework_code_to_id[fw_code] = fw_id

    # Map templates to frameworks
    for template_id, template_info in LOOKUP_TABLES['templates'].items():
        template_code = template_info.get('TemplateCode', '').upper()

        # Try to find matching framework
        for framework_name, patterns in framework_patterns.items():
            if any(pattern in template_code for pattern in patterns):
                # Find the framework ID for this framework name
                for fw_code, fw_id in framework_code_to_id.items():
                    if framework_name in fw_code:
                        LOOKUP_TABLES['template_framework'][template_id] = fw_id
                        break
                break

    # Strategy 3: Handle special cases and edge cases
    _handle_template_framework_edge_cases()

def _handle_template_framework_edge_cases():
    """
    Handle special cases in template-framework mapping.
    """
    # Add specific mappings for known edge cases
    edge_case_mappings = {
        # Template patterns that don't follow standard rules
        'PILLAR3': 'PILLAR3',
        'IPU': 'IPU',
        'PAY': 'PAYMENTS',
        'IF': 'INVESTMENT_FIRMS',
        'FC': 'FICO',
        'GSII': 'GSII'
    }

    framework_code_to_id = {}
    for fw_id, fw_info in LOOKUP_TABLES['frameworks'].items():
        fw_code = fw_info.get('FrameworkCode', '')
        if fw_code:
            framework_code_to_id[fw_code] = fw_id

    for template_id, template_info in LOOKUP_TABLES['templates'].items():
        if template_id not in LOOKUP_TABLES['template_framework']:
            template_code = template_info.get('TemplateCode', '').upper()

            for pattern, framework_name in edge_case_mappings.items():
                if pattern in template_code:
                    # Find framework ID
                    for fw_code, fw_id in framework_code_to_id.items():
                        if framework_name in fw_code:
                            LOOKUP_TABLES['template_framework'][template_id] = fw_id
                            break
                    break
