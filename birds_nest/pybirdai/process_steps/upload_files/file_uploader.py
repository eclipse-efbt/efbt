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
#    Neil Mackenzie - initial API and implementation
#

import os
import re
import uuid
from pathlib import Path

class FileUploader:
    """
    A class for secure file upload handling with path traversal protection.
    """
    
    # Define allowed file extensions for security
    ALLOWED_EXTENSIONS = {
        '.csv', '.xml', '.xsd', '.txt', '.json', '.dmd'
    }
    
    # Maximum file size (50MB)
    MAX_FILE_SIZE = 50 * 1024 * 1024
    
    def _sanitize_filename(self, filename):
        """
        Sanitize filename to prevent path traversal attacks.
        
        Args:
            filename: Original filename from user input
            
        Returns:
            Safe filename or None if invalid
        """
        if not filename:
            return None
            
        # Get just the filename part (remove any path components)
        safe_name = os.path.basename(filename)
        
        # Remove path traversal sequences
        safe_name = safe_name.replace('..', '').replace('/', '').replace('\\', '')
        
        # Remove or replace dangerous characters
        safe_name = re.sub(r'[<>:"|?*\x00-\x1f]', '', safe_name)
        
        # Ensure filename is not empty and has reasonable length
        if not safe_name or len(safe_name) > 255:
            return None
            
        # Check file extension
        _, ext = os.path.splitext(safe_name.lower())
        if ext not in self.ALLOWED_EXTENSIONS:
            return None
            
        return safe_name
    
    def _validate_file_size(self, file):
        """
        Validate file size to prevent DoS attacks.
        
        Args:
            file: Django UploadedFile object
            
        Returns:
            bool: True if file size is acceptable
        """
        return file.size <= self.MAX_FILE_SIZE
    
    def _ensure_safe_directory(self, directory):
        """
        Ensure directory is safe and within expected bounds.
        
        Args:
            directory: Target directory path
            
        Returns:
            Absolute path if safe, None if unsafe
        """
        try:
            # Get absolute path
            abs_directory = os.path.abspath(directory)
            
            # Ensure the directory exists
            os.makedirs(abs_directory, exist_ok=True)
            
            return abs_directory
        except (OSError, ValueError):
            return None

    # Define allowed file extensions for security
    ALLOWED_EXTENSIONS = {
        '.csv', '.xml', '.xsd', '.txt', '.json', '.dmd'
    }

    # Maximum file size (50MB)
    MAX_FILE_SIZE = 50 * 1024 * 1024

    def _sanitize_filename(self, filename):
        """
        Sanitize filename to prevent path traversal attacks.

        Args:
            filename: Original filename from user input

        Returns:
            Safe filename or None if invalid
        """
        if not filename:
            return None

        # Get just the filename part (remove any path components)
        safe_name = os.path.basename(filename)

        # Remove path traversal sequences
        safe_name = safe_name.replace('..', '').replace('/', '').replace('\\', '')

        # Remove or replace dangerous characters
        safe_name = re.sub(r'[<>:"|?*\x00-\x1f]', '', safe_name)

        # Ensure filename is not empty and has reasonable length
        if not safe_name or len(safe_name) > 255:
            return None

        # Check file extension
        _, ext = os.path.splitext(safe_name.lower())
        if ext not in self.ALLOWED_EXTENSIONS:
            return None

        return safe_name

    def _validate_file_size(self, file):
        """
        Validate file size to prevent DoS attacks.

        Args:
            file: Django UploadedFile object

        Returns:
            bool: True if file size is acceptable
        """
        return file.size <= self.MAX_FILE_SIZE

    def _ensure_safe_directory(self, directory):
        """
        Ensure directory is safe and within expected bounds.

        Args:
            directory: Target directory path

        Returns:
            Absolute path if safe, None if unsafe
        """
        try:
            # Get absolute path
            abs_directory = os.path.abspath(directory)

            # Ensure the directory exists
            os.makedirs(abs_directory, exist_ok=True)

            return abs_directory
        except (OSError, ValueError):
            return None

    def upload_sqldev_eil_files(self, sdd_context, request=None):
        """
        Handle the upload of SQLDeveloper EIL files.

        Args:
            sdd_context: The context for the upload
            request: The Django HTTP request object containing the files

        Returns:
            dict: Status of the upload operation
        """
        print("Uploading SQLDeveloper EIL files")


        if not request or not request.FILES:
            return {
                'status': 'error',
                'message': 'No files were uploaded'
            }

        uploaded_files = []
        resource_directory = sdd_context.file_directory
        eil_directory = os.path.join(resource_directory, 'il')
        # delete all files in the directory
        for file in os.listdir(eil_directory):
            os.remove(os.path.join(eil_directory, file))

        for file in request.FILES.getlist('eil_files'):
            try:
                # Secure file upload with validation
                file_info = self._save_file(file, eil_directory)
                uploaded_files.append({
                    'original_name': file_info['original_name'],
                    'safe_name': file_info['safe_name'],
                    'path': file_info['path'],
                    'size': file_info['size']
                })

            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }
<<<<<<< HEAD

=======
>>>>>>> 07358bcc (security check #1656)
            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Error uploading file {file.name}: {str(e)}'
                }

        return {
            'status': 'success',
            'files': uploaded_files
        }

    def upload_sqldev_eldm_files(self, sdd_context, request=None):
        """
        Handle the upload of SQLDeveloper ELDM files.
        """
        print("Uploading SQLDeveloper ELDM files")


        if not request or not request.FILES:
            return {
                'status': 'error',
                'message': 'No files were uploaded'
            }

        uploaded_files = []
        resource_directory = sdd_context.file_directory
        eldm_directory = os.path.join(resource_directory, 'ldm')
        # delete all files in the directory
        for file in os.listdir(eldm_directory):
            os.remove(os.path.join(eldm_directory, file))

        for file in request.FILES.getlist('eldm_files'):
            try:
                # Secure file upload with validation
                file_info = self._save_file(file, eldm_directory)
                uploaded_files.append({
                    'original_name': file_info['original_name'],
                    'safe_name': file_info['safe_name'],
                    'path': file_info['path'],
                    'size': file_info['size']
                })
   
            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }

            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }
            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Error uploading file {file.name}: {str(e)}'
                }

        return {
            'status': 'success',
            'files': uploaded_files
        }

    def upload_technical_export_files(self, sdd_context, request=None):
        """
        Handle the upload of Technical Export files.
        """
        print("Uploading Technical Export files")


        if not request or not request.FILES:
            return {
                'status': 'error',
                'message': 'No files were uploaded'
            }

        uploaded_files = []
        resource_directory = sdd_context.file_directory
        technical_export_directory = os.path.join(resource_directory, 'technical_export')

        # delete all files in the directory
        for file in os.listdir(technical_export_directory):
            os.remove(os.path.join(technical_export_directory, file))

        for file in request.FILES.getlist('technical_export_files'):
            try:
                # Secure file upload with validation
                file_info = self._save_file(file, technical_export_directory)
                uploaded_files.append({
                    'original_name': file_info['original_name'],
                    'safe_name': file_info['safe_name'],
                    'path': file_info['path'],
                    'size': file_info['size']
                })
    
            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }

            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }
            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Error uploading file {file.name}: {str(e)}'
                }

        return {
            'status': 'success',
            'files': uploaded_files
        }

    def upload_joins_configuration(self, sdd_context, request=None):
        """
        Handle the upload of Joins Configuration files.
        """
        print("Uploading Joins Configuration files")


        if not request or not request.FILES:
            return {
                'status': 'error',
                'message': 'No files were uploaded'
            }

        uploaded_files = []
        resource_directory = sdd_context.file_directory
        joins_configuration_directory = os.path.join(resource_directory, 'joins_configuration')

        # delete all files in the directory
        for file in os.listdir(joins_configuration_directory):
            os.remove(os.path.join(joins_configuration_directory, file))

        for file in request.FILES.getlist('joins_configuration_files'):
            try:
                # Secure file upload with validation
                file_info = self._save_file(file, joins_configuration_directory)
                uploaded_files.append({
                    'original_name': file_info['original_name'],
                    'safe_name': file_info['safe_name'],
                    'path': file_info['path'],
                    'size': file_info['size']
                })

<<<<<<< HEAD
                
=======
>>>>>>> 07358bcc (security check #1656)
            except ValueError as e:
                return {
                    'status': 'error',
                    'message': f'Security error uploading file {file.name}: {str(e)}'
                }
            except Exception as e:
                return {
                    'status': 'error',
                    'message': f'Error uploading file {file.name}: {str(e)}'
                }

        return {
            'status': 'success',
            'files': uploaded_files
        }

    def _save_file(self, file, directory):
        """
        Secure method to save the uploaded file with path traversal protection.

        Secure method to save the uploaded file with path traversal protection.
        
        Args:
            file: Django UploadedFile object
            directory: Target directory path

        Returns:
            dict: Result with safe filename and status
        """
        # Validate file size
        if not self._validate_file_size(file):
            raise ValueError(f"File size ({file.size} bytes) exceeds maximum allowed size ({self.MAX_FILE_SIZE} bytes)")

        # Sanitize filename to prevent path traversal
        safe_filename = self._sanitize_filename(file.name)
        if not safe_filename:
            raise ValueError(f"Invalid or unsafe filename: {file.name}")

        # Ensure directory is safe
        safe_directory = self._ensure_safe_directory(directory)
        if not safe_directory:
            raise ValueError(f"Invalid or unsafe directory: {directory}")

        # Generate unique filename if file already exists
        original_name = safe_filename
        counter = 1
        while os.path.exists(os.path.join(safe_directory, safe_filename)):
            name, ext = os.path.splitext(original_name)
            safe_filename = f"{name}_{counter}{ext}"
            counter += 1

        # Validate file size
        if not self._validate_file_size(file):
            raise ValueError(f"File size ({file.size} bytes) exceeds maximum allowed size ({self.MAX_FILE_SIZE} bytes)")
        
        # Sanitize filename to prevent path traversal
        safe_filename = self._sanitize_filename(file.name)
        if not safe_filename:
            raise ValueError(f"Invalid or unsafe filename: {file.name}")
        
        # Ensure directory is safe
        safe_directory = self._ensure_safe_directory(directory)
        if not safe_directory:
            raise ValueError(f"Invalid or unsafe directory: {directory}")
        
        # Generate unique filename if file already exists
        original_name = safe_filename
        counter = 1
        while os.path.exists(os.path.join(safe_directory, safe_filename)):
            name, ext = os.path.splitext(original_name)
            safe_filename = f"{name}_{counter}{ext}"
            counter += 1
        
        # Create the full file path
        file_path = os.path.join(safe_directory, safe_filename)

        # Verify the final path is still within the target directory
        if not os.path.abspath(file_path).startswith(os.path.abspath(safe_directory)):
            raise ValueError("Path traversal attempt detected")

        # Save the file using chunks to handle large files efficiently
        try:
            with open(file_path, 'wb+') as destination:
                for chunk in file.chunks():
                    destination.write(chunk)

            return {
                'original_name': file.name,
                'safe_name': safe_filename,
                'path': file_path,
                'size': file.size
            }
        except IOError as e:
            raise ValueError(f"Failed to save file: {str(e)}")
