#!/usr/bin/env python3
# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

"""
Script to add EPL-2.0 license headers to Python files that are missing them.

Usage:
    python3 add_license_headers.py [--dry-run] [--include-tests]

Options:
    --dry-run       Show what files would be modified without making changes
    --include-tests Include test files (normally skipped)
"""

import os
import re
import argparse

LICENSE_HEADER = '''# coding=UTF-8
# Copyright (c) 2025 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Neil Mackenzie - initial API and implementation

'''

def has_license_header(file_content):
    """Check if file already has a license header"""
    first_30_lines = '\n'.join(file_content.split('\n')[:30])
    return bool(re.search(r'(copyright|license|SPDX)', first_30_lines, re.IGNORECASE))

def should_skip_file(filepath, include_tests=False):
    """Determine if we should skip this file"""
    skip_patterns = [
        '/.venv/',
        '/dpm_database/',
        '/target/',
        '__pycache__',
        '.pyc',
        'add_license_headers.py',  # Don't modify this script
    ]
    
    # Conditionally skip test files
    if not include_tests:
        skip_patterns.extend([
            '/tests/',
            '/migrations/',
            'test_github_token.py',
            'test_aorta_implementation.py'
        ])
    
    for pattern in skip_patterns:
        if pattern in filepath:
            return True
    return False

def add_license_header_to_file(filepath, dry_run=False):
    """Add license header to a Python file if it doesn't have one"""
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
        
        if has_license_header(content):
            return False, "Already has header"
        
        # Handle shebang lines
        lines = content.split('\n')
        shebang = None
        start_idx = 0
        
        if lines and lines[0].startswith('#!'):
            shebang = lines[0]
            start_idx = 1
        
        # Build new content
        new_content = ''
        if shebang:
            new_content += shebang + '\n'
        
        new_content += LICENSE_HEADER
        new_content += '\n'.join(lines[start_idx:])
        
        # Write back to file (unless dry run)
        if not dry_run:
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write(new_content)
        
        return True, "Header added"
    
    except Exception as e:
        return False, f"Error: {e}"

def main():
    """Main function to process all Python files"""
    parser = argparse.ArgumentParser(description='Add EPL-2.0 license headers to Python files')
    parser.add_argument('--dry-run', action='store_true', 
                       help='Show what would be changed without making modifications')
    parser.add_argument('--include-tests', action='store_true', 
                       help='Include test files (normally skipped)')
    
    args = parser.parse_args()
    
    processed_count = 0
    skipped_count = 0
    error_count = 0
    
    if args.dry_run:
        print("DRY RUN MODE - No files will be modified")
        print("=" * 50)
    
    for root, dirs, files in os.walk('.'):
        # Skip certain directories entirely
        dirs[:] = [d for d in dirs if not d.startswith('.') and d not in ['__pycache__', 'target', 'dpm_database']]
        
        for file in files:
            if file.endswith('.py'):
                filepath = os.path.join(root, file)
                
                if should_skip_file(filepath, args.include_tests):
                    continue
                
                success, message = add_license_header_to_file(filepath, args.dry_run)
                
                if success:
                    processed_count += 1
                    action = "Would add" if args.dry_run else "Added"
                    print(f"{action} license header to: {filepath}")
                elif "Already has header" in message:
                    skipped_count += 1
                    if args.dry_run:
                        print(f"Skipping (has header): {filepath}")
                else:
                    error_count += 1
                    print(f"Error processing {filepath}: {message}")
    
    print("\n" + "=" * 50)
    print(f"Summary:")
    print(f"  Files processed: {processed_count}")
    print(f"  Files skipped (already licensed): {skipped_count}")
    print(f"  Errors: {error_count}")
    
    if args.dry_run and processed_count > 0:
        print(f"\nTo actually apply changes, run without --dry-run")

if __name__ == "__main__":
    main()