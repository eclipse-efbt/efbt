#!/usr/bin/env python3
"""
Alternative Access database export script using Python libraries.
This script can export Access databases without requiring Microsoft Access to be installed.
"""

import os
import sys
import argparse
import pandas as pd
from pathlib import Path
import logging

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

def export_access_to_csv_python(database_path, output_dir="target"):
    """
    Export Access database tables to CSV using Python libraries.
    
    This function attempts multiple methods to read Access databases:
    1. Using pandas with pyodbc (if available)
    2. Using mdb-tools (if available on Linux/Mac)
    3. Fallback message for manual conversion
    
    Args:
        database_path (str): Path to the Access database file
        output_dir (str): Output directory for CSV files
    """
    
    # Ensure output directory exists
    os.makedirs(output_dir, exist_ok=True)
    
    database_path = os.path.abspath(database_path)
    logger.info(f"Attempting to export Access database: {database_path}")
    
    if not os.path.exists(database_path):
        logger.error(f"Database file not found: {database_path}")
        return False
    
    # Method 1: Try using pyodbc with Access driver
    try:
        import pyodbc
        
        # Try different Access drivers
        drivers = [
            'Microsoft Access Driver (*.mdb, *.accdb)',
            'Microsoft Access Driver (*.mdb)',
            'Microsoft Access Driver (*.accdb)',
        ]
        
        connection = None
        for driver in drivers:
            try:
                connection_string = f'DRIVER={{{driver}}};DBQ={database_path};'
                connection = pyodbc.connect(connection_string)
                logger.info(f"Successfully connected using driver: {driver}")
                break
            except pyodbc.Error as e:
                logger.debug(f"Failed to connect with driver '{driver}': {e}")
                continue
        
        if connection:
            return export_with_pyodbc(connection, output_dir)
        else:
            logger.warning("No suitable Access driver found for pyodbc")
            
    except ImportError:
        logger.info("pyodbc not available")
    except Exception as e:
        logger.error(f"Error with pyodbc method: {e}")
    
    # Method 2: Try using mdb-tools (Linux/Mac)
    if os.name != 'nt':  # Not Windows
        try:
            return export_with_mdb_tools(database_path, output_dir)
        except Exception as e:
            logger.error(f"Error with mdb-tools method: {e}")
    
    # Method 3: Fallback - provide instructions
    logger.error("Unable to export Access database automatically.")
    logger.error("Automatic Access database export requires one of the following:")
    logger.error("1. Microsoft Access or Access Runtime installed (Windows)")
    logger.error("2. pyodbc with Access drivers (Windows)")
    logger.error("3. mdb-tools (Linux/Mac)")
    logger.error("")
    logger.error("Alternative solutions:")
    logger.error("1. Manually export tables from Access to CSV files")
    logger.error("2. Convert .accdb to .mdb format and use mdb-tools")
    logger.error("3. Install Microsoft Access Runtime (free from Microsoft)")
    
    return False

def export_with_pyodbc(connection, output_dir):
    """Export using pyodbc connection."""
    try:
        cursor = connection.cursor()
        
        # Get list of tables (excluding system tables)
        cursor.execute("""
            SELECT Name FROM MSysObjects 
            WHERE Type=1 AND Flags=0 AND Name NOT LIKE 'MSys%' AND Name NOT LIKE '~%'
        """)
        
        tables = [row[0] for row in cursor.fetchall()]
        logger.info(f"Found {len(tables)} tables to export")
        
        exported_count = 0
        for table_name in tables:
            try:
                logger.info(f"Exporting table: {table_name}")
                df = pd.read_sql(f"SELECT * FROM [{table_name}]", connection)
                
                csv_path = os.path.join(output_dir, f"{table_name}.csv")
                df.to_csv(csv_path, index=False)
                
                logger.info(f"Successfully exported {table_name} to {csv_path}")
                exported_count += 1
                
            except Exception as e:
                logger.error(f"Error exporting table {table_name}: {e}")
        
        logger.info(f"Export complete. Successfully exported {exported_count} of {len(tables)} tables.")
        return exported_count > 0
        
    except Exception as e:
        logger.error(f"Error during pyodbc export: {e}")
        return False
    finally:
        connection.close()

def export_with_mdb_tools(database_path, output_dir):
    """Export using mdb-tools (Linux/Mac)."""
    import subprocess
    
    try:
        # Check if mdb-tools is available
        subprocess.run(['mdb-ver'], check=True, capture_output=True)
        logger.info("mdb-tools is available")
        
        # Get list of tables
        result = subprocess.run(['mdb-tables', '-1', database_path], 
                              capture_output=True, text=True, check=True)
        tables = [t.strip() for t in result.stdout.strip().split('\n') if t.strip()]
        
        logger.info(f"Found {len(tables)} tables to export")
        
        exported_count = 0
        for table_name in tables:
            try:
                logger.info(f"Exporting table: {table_name}")
                csv_path = os.path.join(output_dir, f"{table_name}.csv")
                
                with open(csv_path, 'w') as f:
                    subprocess.run(['mdb-export', database_path, table_name], 
                                 stdout=f, check=True)
                
                logger.info(f"Successfully exported {table_name} to {csv_path}")
                exported_count += 1
                
            except Exception as e:
                logger.error(f"Error exporting table {table_name}: {e}")
        
        logger.info(f"Export complete. Successfully exported {exported_count} of {len(tables)} tables.")
        return exported_count > 0
        
    except subprocess.CalledProcessError:
        logger.info("mdb-tools not available or failed")
        return False
    except FileNotFoundError:
        logger.info("mdb-tools not installed")
        return False

def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description='Export Access database tables to CSV files using Python'
    )
    parser.add_argument('database', help='Path to Access database file (.accdb or .mdb)')
    parser.add_argument('--output-dir', default='target', 
                       help='Output directory for CSV files (default: target)')
    
    args = parser.parse_args()
    
    success = export_access_to_csv_python(args.database, args.output_dir)
    
    if success:
        logger.info("Export completed successfully")
        sys.exit(0)
    else:
        logger.error("Export failed")
        sys.exit(1)

if __name__ == '__main__':
    main()