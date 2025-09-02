# coding=UTF-8
# Copyright (c) 2024 Bird Software Solutions Ltd
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Security fixes for path traversal prevention
#

import os
import re
from pathlib import Path
from typing import Optional, Set

class SecureFileValidator:
    """
    Utility class for secure file operations with path traversal protection.
    """
    
    # Default allowed file extensions for different use cases
    DOCUMENT_EXTENSIONS = {'.csv', '.xml', '.xsd', '.txt', '.json', '.dmd', '.html', '.md'}
    IMAGE_EXTENSIONS = {'.jpg', '.jpeg', '.png', '.gif', '.svg', '.webp'}
    ALL_SAFE_EXTENSIONS = DOCUMENT_EXTENSIONS | IMAGE_EXTENSIONS
    
    # Maximum file size (50MB by default)
    DEFAULT_MAX_FILE_SIZE = 50 * 1024 * 1024
    
    @staticmethod
    def sanitize_filename(filename: str, allowed_extensions: Optional[Set[str]] = None) -> Optional[str]:
        """
        Sanitize filename to prevent path traversal attacks and validate extension.
        
        Args:
            filename: Original filename from user input
            allowed_extensions: Set of allowed file extensions (default: DOCUMENT_EXTENSIONS)
            
        Returns:
            Safe filename or None if invalid
        """
        if not filename:
            return None
        
        if allowed_extensions is None:
            allowed_extensions = SecureFileValidator.DOCUMENT_EXTENSIONS
            
        # Get just the filename part (remove any path components)
        safe_name = os.path.basename(filename)
        
        # Remove path traversal sequences
        safe_name = safe_name.replace('..', '').replace('/', '').replace('\\', '')
        
        # Remove or replace dangerous characters
        safe_name = re.sub(r'[<>:"|?*\x00-\x1f]', '', safe_name)
        
        # Ensure filename is not empty and has reasonable length
        if not safe_name or len(safe_name) > 255:
            return None
        
        # Validate filename format (alphanumeric, dots, hyphens, underscores only)
        if not re.match(r'^[a-zA-Z0-9._-]+$', safe_name):
            return None
            
        # Check file extension
        _, ext = os.path.splitext(safe_name.lower())
        if ext not in allowed_extensions:
            return None
            
        return safe_name
    
    @staticmethod
    def validate_path_safety(file_path: Path, base_directory: Path) -> bool:
        """
        Validate that a file path is safe and within the expected directory boundaries.
        
        Args:
            file_path: The file path to validate
            base_directory: The base directory that should contain the file
            
        Returns:
            True if path is safe, False otherwise
        """
        try:
            # Resolve both paths to handle any symlinks or relative components
            resolved_file_path = file_path.resolve()
            resolved_base_dir = base_directory.resolve()
            
            # Check if the file path is within the base directory
            return resolved_file_path.is_relative_to(resolved_base_dir)
            
        except (OSError, ValueError):
            return False
    
    @staticmethod
    def get_safe_file_path(base_directory: str | Path, filename: str, 
                          allowed_extensions: Optional[Set[str]] = None) -> Optional[Path]:
        """
        Construct a safe file path by validating the filename and ensuring it's within bounds.
        
        Args:
            base_directory: Base directory path
            filename: User-provided filename
            allowed_extensions: Set of allowed file extensions
            
        Returns:
            Safe Path object or None if invalid
        """
        # Sanitize the filename
        safe_filename = SecureFileValidator.sanitize_filename(filename, allowed_extensions)
        if not safe_filename:
            return None
        
        # Create base directory path
        base_path = Path(base_directory)
        
        # Construct the file path
        file_path = base_path / safe_filename
        
        # Validate the path is safe
        if not SecureFileValidator.validate_path_safety(file_path, base_path):
            return None
            
        return file_path
    
    @staticmethod
    def validate_file_size(file_size: int, max_size: int = DEFAULT_MAX_FILE_SIZE) -> bool:
        """
        Validate file size to prevent DoS attacks.
        
        Args:
            file_size: Size of the file in bytes
            max_size: Maximum allowed file size in bytes
            
        Returns:
            True if file size is acceptable
        """
        return 0 < file_size <= max_size
    
    @staticmethod
    def create_secure_upload_path(base_directory: str | Path, original_filename: str,
                                 allowed_extensions: Optional[Set[str]] = None) -> tuple[Optional[Path], Optional[str]]:
        """
        Create a secure upload path with automatic filename collision handling.
        
        Args:
            base_directory: Base directory for uploads
            original_filename: Original filename from user
            allowed_extensions: Set of allowed file extensions
            
        Returns:
            Tuple of (safe_file_path, safe_filename) or (None, None) if invalid
        """
        safe_filename = SecureFileValidator.sanitize_filename(original_filename, allowed_extensions)
        if not safe_filename:
            return None, None
        
        base_path = Path(base_directory)
        
        # Handle filename collisions by adding counter
        original_safe_name = safe_filename
        counter = 1
        while True:
            file_path = base_path / safe_filename
            
            # Validate the path is safe
            if not SecureFileValidator.validate_path_safety(file_path, base_path):
                return None, None
            
            # If file doesn't exist, we can use this name
            if not file_path.exists():
                return file_path, safe_filename
            
            # Generate new filename with counter
            name, ext = os.path.splitext(original_safe_name)
            safe_filename = f"{name}_{counter}{ext}"
            counter += 1
            
            # Prevent infinite loop
            if counter > 1000:
                return None, None