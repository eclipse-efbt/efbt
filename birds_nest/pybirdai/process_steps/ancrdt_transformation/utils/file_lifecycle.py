# coding=UTF-8
# Copyright (c) 2025 Arfa Digital Consulting
# This program and the accompanying materials
# are made available under the terms of the Eclipse Public License 2.0
# which accompanies this distribution, and is available at
# https://www.eclipse.org/legal/epl-2.0/
#
# SPDX-License-Identifier: EPL-2.0
#
# Contributors:
#    Benjamin Arfa - initial API and implementation
#
"""
File lifecycle management for generated ANCRDT transformation code.

This module provides utilities for managing the lifecycle of generated Python files,
including:
- Detecting manual edits by comparing .py and .generated files
- Preserving manually edited files during regeneration
- Creating .generated base copies for comparison
- Writing generation metadata headers

This ensures that manual edits to generated code are not lost during regeneration.
"""

import shutil
from pathlib import Path
from datetime import datetime
from typing import Optional
import logging

from .constants import GENERATED_FILE_SUFFIX, BACKUP_FILE_SUFFIX

logger = logging.getLogger(__name__)


class GeneratedFileLifecycle:
    """
    Manages the lifecycle of generated Python transformation files.

    This class handles the common pattern of generated code that may be
    manually edited. It uses a .generated file as a baseline to detect
    when manual edits have been made.

    Workflow:
        1. prepare_generated_file() - Check if file has manual edits
        2. Generate new file content (if not manually edited)
        3. finalize_generated_file() - Create .generated base copy

    Example:
        >>> lifecycle = GeneratedFileLifecycle()
        >>> preserve = lifecycle.prepare_generated_file("output.py")
        >>> if not preserve:
        ...     # Generate new file
        ...     with open("output.py", "w") as f:
        ...         lifecycle.write_generation_metadata(f, "output.py")
        ...         f.write("# Generated code here")
        >>> lifecycle.finalize_generated_file("output.py", preserve)
    """

    @staticmethod
    def prepare_generated_file(
        file_path_str: str,
        logger_instance: Optional[logging.Logger] = None
    ) -> bool:
        """
        Prepare a file for generation by checking if it has been manually edited.

        This method compares the existing .py file with its .generated baseline
        to determine if manual edits have been made. If edits are detected,
        the existing file is preserved.

        Lifecycle logic:
        - If .py exists and .generated exists: compare them
          - If different: .py has manual edits, preserve it, delete .generated
          - If same: safe to regenerate
        - If .py exists but no .generated: first time generating, preserve current .py
        - If .py doesn't exist: nothing to preserve

        Args:
            file_path_str (str): Path to the .py file to generate
            logger_instance (logging.Logger, optional): Logger to use.
                If None, uses module logger.

        Returns:
            bool: True if existing .py file should be preserved,
                  False if safe to overwrite

        Raises:
            None: Errors are logged but not raised to allow generation to continue
        """
        log = logger_instance or logger
        file_path = Path(file_path_str)
        generated_path = Path(str(file_path_str) + GENERATED_FILE_SUFFIX)

        # If .py doesn't exist, nothing to preserve
        if not file_path.exists():
            return False

        # If .generated doesn't exist, this is first generation - preserve .py
        if not generated_path.exists():
            log.info(
                f"Preserving existing {file_path.name} (no .generated base found)"
            )
            return True

        # Both exist - compare them
        try:
            with open(file_path, 'r', encoding='utf-8') as f1:
                current_content = f1.read()
            with open(generated_path, 'r', encoding='utf-8') as f2:
                generated_content = f2.read()

            if current_content != generated_content:
                log.info(f"Preserving manually edited {file_path.name}")
                # Delete old .generated, we'll create new one
                generated_path.unlink()
                return True
            else:
                # Files are identical, safe to regenerate
                log.info(f"No edits detected in {file_path.name}, regenerating")
                return False

        except Exception as e:
            log.error(f"Error comparing files: {e}")
            return True  # Preserve on error to be safe

    @staticmethod
    def finalize_generated_file(
        file_path_str: str,
        preserve_original: bool,
        logger_instance: Optional[logging.Logger] = None
    ):
        """
        Finalize generation by creating .generated base copy.

        After generating a new file, this method creates a .generated copy
        that will be used in future comparisons to detect manual edits.

        Args:
            file_path_str (str): Path to the generated .py file
            preserve_original (bool): If True, original .py was preserved,
                so no new file was generated
            logger_instance (logging.Logger, optional): Logger to use

        Raises:
            None: Errors are logged but not raised
        """
        log = logger_instance or logger
        file_path = Path(file_path_str)
        generated_path = Path(str(file_path_str) + GENERATED_FILE_SUFFIX)

        try:
            if preserve_original:
                # Original .py was preserved with edits, new generation was skipped
                log.info(
                    f"Skipped generation of {file_path.name} (manual edits preserved)"
                )
            else:
                # New file was generated, save as .generated base
                if file_path.exists():
                    shutil.copy2(file_path, generated_path)
                    log.info(f"Created base reference: {generated_path.name}")
                else:
                    log.warning(
                        f"Cannot create .generated for {file_path.name}: file not found"
                    )

        except Exception as e:
            log.error(f"Error finalizing generated file: {e}")

    @staticmethod
    def write_generation_metadata(
        file,
        filename: str,
        generator_name: str = "CreatePythonTransformations (ANCRDT Lifecycle)"
    ):
        """
        Write generation metadata header to file.

        This header provides information about when and how the file was
        generated, and warns against direct edits.

        Args:
            file: Open file handle (must be in write mode)
            filename (str): Name of the file being generated
            generator_name (str): Name of the generator tool.
                Defaults to "CreatePythonTransformations (ANCRDT Lifecycle)"

        Example:
            >>> with open("output.py", "w") as f:
            ...     GeneratedFileLifecycle.write_generation_metadata(f, "output.py")
            ...     f.write("# Your code here")
        """
        timestamp = datetime.now().isoformat()
        file.write(f"# Generated: {timestamp}\n")
        file.write(f"# Generator: {generator_name}\n")
        file.write(f"# File: {filename}\n")
        file.write(f"# DO NOT EDIT THIS FILE DIRECTLY - Edit via web UI to preserve changes\n")
        file.write(f"# Base version saved as {filename}{GENERATED_FILE_SUFFIX}\n\n")

    @staticmethod
    def create_backup(
        file_path_str: str,
        logger_instance: Optional[logging.Logger] = None
    ) -> bool:
        """
        Create a backup copy of a file before modifying it.

        This creates a .backup copy of the file for safety.

        Args:
            file_path_str (str): Path to file to backup
            logger_instance (logging.Logger, optional): Logger to use

        Returns:
            bool: True if backup successful, False otherwise
        """
        log = logger_instance or logger
        file_path = Path(file_path_str)
        backup_path = Path(str(file_path_str) + BACKUP_FILE_SUFFIX)

        try:
            if file_path.exists():
                shutil.copy2(file_path, backup_path)
                log.info(f"Created backup: {backup_path.name}")
                return True
            else:
                log.warning(f"Cannot backup {file_path.name}: file not found")
                return False

        except Exception as e:
            log.error(f"Error creating backup: {e}")
            return False

    @staticmethod
    def has_manual_edits(file_path_str: str) -> bool:
        """
        Check if a file has manual edits compared to its .generated baseline.

        Args:
            file_path_str (str): Path to the .py file to check

        Returns:
            bool: True if file has manual edits or .generated doesn't exist,
                  False if files are identical
        """
        file_path = Path(file_path_str)
        generated_path = Path(str(file_path_str) + GENERATED_FILE_SUFFIX)

        if not file_path.exists():
            return False

        if not generated_path.exists():
            return True  # Treat as manually edited if no baseline

        try:
            with open(file_path, 'r', encoding='utf-8') as f1:
                current_content = f1.read()
            with open(generated_path, 'r', encoding='utf-8') as f2:
                generated_content = f2.read()

            return current_content != generated_content

        except Exception:
            return True  # Assume edited on error
