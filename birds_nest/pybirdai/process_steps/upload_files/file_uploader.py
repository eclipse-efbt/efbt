# coding=UTF-8#
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

class FileUploader:
    """
    A class for creating generation rules for reports and tables.
    """

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
        for file in request.FILES.getlist('eil_files'):
            try:
                # You might want to add file validation here
                # For example, check file extension, size, etc.
                
                # Handle the file upload
                # Example: save to a specific directory
                resource_directory = sdd_context.file_directory
                eil_directory = os.path.join(resource_directory, 'il')
                self._save_file(file, eil_directory)
                uploaded_files.append({
                    'name': file.name,
                    'path': eil_directory,
                    'size': file.size
                })
                
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
        Helper method to save the uploaded file.
        
        Args:
            file: Django UploadedFile object
            directory: Target directory path
        """
        # Ensure the directory exists
        os.makedirs(directory, exist_ok=True)
        
        # Create the full file path
        file_path = os.path.join(directory, file.name)
        
        # Save the file using chunks to handle large files efficiently
        with open(file_path, 'wb+') as destination:
            for chunk in file.chunks():
                destination.write(chunk)