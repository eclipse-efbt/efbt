"""
SQL to CSV Converter for BIRD test fixtures.

Converts existing sql_inserts.sql files to the new CSV format.
Parses INSERT statements and outputs one CSV file per table.
"""

import csv
import logging
import os
import re
from typing import Dict, List, Tuple, Optional

logger = logging.getLogger(__name__)


class SQLToCSVConverter:
    """
    Converts SQL INSERT statements to CSV files.

    Handles:
    - INSERT INTO table(columns) VALUES(values) statements
    - CAST('date' AS DATETIME) expressions
    - NULL values -> empty strings
    - String values with quotes
    - Numeric values
    """

    # Regex patterns for parsing
    # Match INSERT INTO table_name( - we'll parse the rest manually
    INSERT_START_PATTERN = re.compile(
        r"INSERT\s+INTO\s+(\w+)\s*\(",
        re.IGNORECASE
    )

    CAST_DATETIME_PATTERN = re.compile(
        r"CAST\s*\(\s*'([^']+)'\s+AS\s+DATETIME\s*\)",
        re.IGNORECASE
    )

    def __init__(self):
        pass

    def _find_matching_paren(self, text: str, start: int) -> int:
        """
        Find the index of the closing parenthesis matching the one at start.

        Args:
            text: The text to search
            start: Index of the opening parenthesis

        Returns:
            Index of matching closing parenthesis, or -1 if not found
        """
        if start >= len(text) or text[start] != '(':
            return -1

        depth = 0
        in_quotes = False
        quote_char = None
        i = start

        while i < len(text):
            char = text[i]

            if not in_quotes:
                if char in ("'", '"'):
                    in_quotes = True
                    quote_char = char
                elif char == '(':
                    depth += 1
                elif char == ')':
                    depth -= 1
                    if depth == 0:
                        return i
            else:
                if char == quote_char:
                    # Check for escaped quote
                    if i + 1 < len(text) and text[i + 1] == quote_char:
                        i += 1  # Skip escaped quote
                    else:
                        in_quotes = False
                        quote_char = None
            i += 1

        return -1

    def _extract_table_name(self, full_table_name: str) -> str:
        """
        Extract short table name from full table name.

        Args:
            full_table_name: Full table name like 'pybirdai_prty'

        Returns:
            Short table name like 'prty'
        """
        if full_table_name.lower().startswith('pybirdai_'):
            return full_table_name[9:]  # Remove 'pybirdai_' prefix
        return full_table_name

    def _parse_columns(self, columns_str: str) -> List[str]:
        """
        Parse column names from INSERT statement.

        Args:
            columns_str: Comma-separated column names

        Returns:
            List of column names
        """
        columns = []
        for col in columns_str.split(','):
            col = col.strip()
            if col:
                columns.append(col)
        return columns

    def _convert_sql_value(self, value: str) -> str:
        """
        Convert a SQL value to CSV format.

        Handles:
        - CAST('date' AS DATETIME) -> date string
        - NULL -> empty string
        - 'string' -> string (without quotes)
        - numeric -> numeric string

        Args:
            value: Raw SQL value

        Returns:
            CSV-friendly string value
        """
        value = value.strip()

        # Handle CAST(...) datetime expressions
        cast_match = self.CAST_DATETIME_PATTERN.search(value)
        if cast_match:
            return cast_match.group(1)  # Return just the date string

        # Handle NULL
        if value.upper() == 'NULL':
            return ''

        # Handle quoted strings
        if (value.startswith("'") and value.endswith("'")) or \
           (value.startswith('"') and value.endswith('"')):
            # Remove quotes and unescape
            inner = value[1:-1]
            # Handle escaped quotes
            inner = inner.replace("''", "'").replace('""', '"')
            return inner

        # Handle numeric and other values
        return value

    def _parse_values(self, values_str: str) -> List[str]:
        """
        Parse values from INSERT statement, handling nested quotes and CAST().

        Args:
            values_str: Values portion of INSERT statement

        Returns:
            List of parsed values
        """
        values = []
        current_value = ''
        in_quotes = False
        quote_char = None
        paren_depth = 0
        i = 0

        while i < len(values_str):
            char = values_str[i]

            if not in_quotes:
                if char in ("'", '"'):
                    in_quotes = True
                    quote_char = char
                    current_value += char
                elif char == '(':
                    paren_depth += 1
                    current_value += char
                elif char == ')':
                    paren_depth -= 1
                    current_value += char
                elif char == ',' and paren_depth == 0:
                    # End of value
                    values.append(self._convert_sql_value(current_value))
                    current_value = ''
                else:
                    current_value += char
            else:
                # Inside quotes
                current_value += char
                if char == quote_char:
                    # Check for escaped quote
                    if i + 1 < len(values_str) and values_str[i + 1] == quote_char:
                        current_value += quote_char
                        i += 1
                    else:
                        in_quotes = False
                        quote_char = None

            i += 1

        # Add last value
        if current_value.strip():
            values.append(self._convert_sql_value(current_value))

        return values

    def parse_insert_statement(self, sql: str) -> Optional[Tuple[str, List[str], List[str]]]:
        """
        Parse a single INSERT statement.

        Args:
            sql: SQL INSERT statement

        Returns:
            Tuple of (table_name, columns, values) or None if parsing fails
        """
        # Find INSERT INTO table_name(
        match = self.INSERT_START_PATTERN.search(sql)
        if not match:
            return None

        table_name = self._extract_table_name(match.group(1))

        # Find the opening parenthesis for columns
        col_start = match.end() - 1  # Position of '('

        # Find matching closing parenthesis for columns
        col_end = self._find_matching_paren(sql, col_start)
        if col_end == -1:
            return None

        # Extract columns
        columns_str = sql[col_start + 1:col_end]
        columns = self._parse_columns(columns_str)

        # Find VALUES keyword after columns
        rest = sql[col_end + 1:]
        values_match = re.search(r'\s*VALUES\s*\(', rest, re.IGNORECASE)
        if not values_match:
            return None

        # Find the opening parenthesis for values
        values_start_in_rest = values_match.end() - 1  # Position of '(' in rest
        values_start = col_end + 1 + values_start_in_rest

        # Find matching closing parenthesis for values
        values_end = self._find_matching_paren(sql, values_start)
        if values_end == -1:
            return None

        # Extract values
        values_str = sql[values_start + 1:values_end]
        values = self._parse_values(values_str)

        # Validate column/value count
        if len(columns) != len(values):
            logger.warning(
                f"Column/value count mismatch for {table_name}: "
                f"{len(columns)} columns, {len(values)} values"
            )
            # Try to handle minor mismatches
            if len(values) > len(columns):
                values = values[:len(columns)]
            elif len(values) < len(columns):
                values.extend([''] * (len(columns) - len(values)))

        return (table_name, columns, values)

    def _remove_sql_comments(self, sql_content: str) -> str:
        """
        Remove SQL comments from content.

        Args:
            sql_content: Raw SQL content

        Returns:
            SQL content with comments removed
        """
        lines = sql_content.split('\n')
        cleaned_lines = []
        for line in lines:
            # Remove single-line comments (-- ...)
            # But keep the line if it has SQL before the comment
            comment_idx = line.find('--')
            if comment_idx != -1:
                line = line[:comment_idx]
            cleaned_lines.append(line)
        return '\n'.join(cleaned_lines)

    def convert_sql_file(self, sql_path: str, output_dir: str) -> Dict[str, str]:
        """
        Convert an SQL file to CSV files.

        Args:
            sql_path: Path to sql_inserts.sql file
            output_dir: Directory to write CSV files

        Returns:
            Dict mapping table name to output CSV path
        """
        if not os.path.exists(sql_path):
            raise FileNotFoundError(f"SQL file not found: {sql_path}")

        os.makedirs(output_dir, exist_ok=True)

        # Read SQL file
        with open(sql_path, 'r', encoding='utf-8') as f:
            sql_content = f.read()

        # Remove comments before parsing
        sql_content = self._remove_sql_comments(sql_content)

        # Parse all INSERT statements
        # Group by table name
        tables_data: Dict[str, Dict] = {}  # table_name -> {columns: [], rows: []}

        # Find all INSERT statements
        # Split by INSERT (case-insensitive) to handle multiple statements
        insert_statements = re.split(r'(?=INSERT\s+INTO)', sql_content, flags=re.IGNORECASE)

        for statement in insert_statements:
            statement = statement.strip()
            if not statement:
                continue

            result = self.parse_insert_statement(statement)
            if result is None:
                continue

            table_name, columns, values = result

            if table_name not in tables_data:
                tables_data[table_name] = {
                    'columns': columns,
                    'rows': []
                }

            # Verify columns match (should be same for all rows)
            if tables_data[table_name]['columns'] != columns:
                logger.warning(
                    f"Column mismatch in {table_name}: "
                    f"expected {tables_data[table_name]['columns']}, got {columns}"
                )

            tables_data[table_name]['rows'].append(values)

        # Write CSV files
        output_files = {}

        for table_name, data in tables_data.items():
            csv_filename = f"{table_name.lower()}.csv"
            csv_path = os.path.join(output_dir, csv_filename)

            with open(csv_path, 'w', encoding='utf-8', newline='') as f:
                writer = csv.writer(f)
                writer.writerow(data['columns'])
                for row in data['rows']:
                    writer.writerow(row)

            output_files[table_name] = csv_path
            logger.info(f"Wrote {len(data['rows'])} rows to {csv_path}")

        return output_files

    def convert_scenario_in_place(self, scenario_path: str) -> Dict[str, str]:
        """
        Convert sql_inserts.sql in a scenario directory to CSV files in the same directory.

        Args:
            scenario_path: Path to scenario directory containing sql_inserts.sql

        Returns:
            Dict mapping table name to output CSV path
        """
        sql_path = os.path.join(scenario_path, 'sql_inserts.sql')
        return self.convert_sql_file(sql_path, scenario_path)


def convert_sql_to_csv(sql_path: str, output_dir: str) -> Dict[str, str]:
    """
    Convenience function to convert SQL file to CSV files.

    Args:
        sql_path: Path to SQL file
        output_dir: Output directory for CSV files

    Returns:
        Dict mapping table name to output CSV path
    """
    converter = SQLToCSVConverter()
    return converter.convert_sql_file(sql_path, output_dir)


def convert_all_fixtures(fixtures_root: str, dry_run: bool = False) -> Dict[str, Dict[str, str]]:
    """
    Convert all sql_inserts.sql files under a fixtures root directory.

    Walks the directory tree looking for sql_inserts.sql files and
    converts each to CSV files in the same directory.

    Args:
        fixtures_root: Root directory of fixtures (e.g., tests/fixtures/templates)
        dry_run: If True, only report what would be converted

    Returns:
        Dict mapping scenario path to {table_name: csv_path}
    """
    converter = SQLToCSVConverter()
    results = {}

    for root, dirs, files in os.walk(fixtures_root):
        if 'sql_inserts.sql' in files:
            sql_path = os.path.join(root, 'sql_inserts.sql')

            if dry_run:
                logger.info(f"Would convert: {sql_path}")
                results[root] = {'dry_run': True}
            else:
                try:
                    output_files = converter.convert_sql_file(sql_path, root)
                    results[root] = output_files
                    logger.info(f"Converted: {sql_path}")
                except Exception as e:
                    logger.error(f"Failed to convert {sql_path}: {e}")
                    results[root] = {'error': str(e)}

    return results


if __name__ == '__main__':
    # Command-line usage example
    import sys

    if len(sys.argv) < 2:
        print("Usage: python sql_to_csv_converter.py <sql_file_or_fixtures_dir> [output_dir]")
        print("       python sql_to_csv_converter.py --convert-all <fixtures_root>")
        sys.exit(1)

    logging.basicConfig(level=logging.INFO)

    if sys.argv[1] == '--convert-all':
        if len(sys.argv) < 3:
            print("Please specify fixtures root directory")
            sys.exit(1)
        results = convert_all_fixtures(sys.argv[2])
        for path, files in results.items():
            print(f"{path}: {files}")
    else:
        sql_path = sys.argv[1]
        output_dir = sys.argv[2] if len(sys.argv) > 2 else os.path.dirname(sql_path)
        results = convert_sql_to_csv(sql_path, output_dir)
        for table, path in results.items():
            print(f"{table}: {path}")
