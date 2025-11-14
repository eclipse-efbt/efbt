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
Standard logging configuration for ANCRDT transformation processes.

This module provides consistent logging setup across all ANCRDT transformation
modules, ensuring uniform log formatting and output handling.
"""

import logging
import os


def setup_ancrdt_logger(
    name=__name__,
    log_file="ancrdt.log",
    level=logging.INFO,
    format_string='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
):
    """
    Configure and return a logger with standard ANCRDT settings.

    This function sets up a logger with both file and console handlers,
    using a consistent format across all ANCRDT transformation modules.

    Args:
        name (str): Logger name, typically __name__ of the calling module.
                   Defaults to __name__ of this module.
        log_file (str): Path to log file. Can be absolute or relative.
                       Defaults to 'ancrdt.log' in current directory.
        level (int): Logging level (e.g., logging.INFO, logging.DEBUG).
                    Defaults to logging.INFO.
        format_string (str): Log message format string.
                            Defaults to standard format with timestamp, name, level, message.

    Returns:
        logging.Logger: Configured logger instance

    Example:
        >>> logger = setup_ancrdt_logger(__name__, "my_process.log")
        >>> logger.info("Processing started")
        >>> logger.error("An error occurred")

    Note:
        If the logger already has handlers, this function will not add
        duplicate handlers.
    """
    logger = logging.getLogger(name)

    # Only configure if logger doesn't already have handlers
    # (prevents duplicate handlers if called multiple times)
    if not logger.handlers:
        logger.setLevel(level)

        # Create formatter
        formatter = logging.Formatter(format_string)

        # File handler - writes to log file
        file_handler = logging.FileHandler(log_file)
        file_handler.setLevel(level)
        file_handler.setFormatter(formatter)

        # Console handler - writes to stdout
        console_handler = logging.StreamHandler()
        console_handler.setLevel(level)
        console_handler.setFormatter(formatter)

        # Add handlers to logger
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)

    return logger


def setup_ancrdt_logger_basic(name=__name__):
    """
    Quick logger setup with default settings.

    Convenience function for simple logging needs. Uses default
    settings: INFO level, ancrdt.log file, standard format.

    Args:
        name (str): Logger name, typically __name__ of the calling module

    Returns:
        logging.Logger: Configured logger instance

    Example:
        >>> logger = setup_ancrdt_logger_basic(__name__)
        >>> logger.info("Simple logging setup")
    """
    return setup_ancrdt_logger(name)


def configure_root_logger(log_file="ancrdt.log", level=logging.INFO):
    """
    Configure the root logger with ANCRDT settings.

    This function is useful when you want to capture logs from all modules,
    not just the current one.

    Args:
        log_file (str): Path to log file
        level (int): Logging level

    Example:
        >>> configure_root_logger("ancrdt_all.log", logging.DEBUG)
        >>> # Now all modules will log to this file
    """
    logging.basicConfig(
        level=level,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler(log_file),
            logging.StreamHandler()
        ]
    )


def get_logger_for_module(module_name=__name__, log_dir="logs"):
    """
    Get a logger for a specific module with its own log file.

    Creates a logger that writes to a module-specific log file in the logs directory.
    Log file name is based on the module name.

    Args:
        module_name (str): Name of the module (typically __name__)
        log_dir (str): Directory to store log files. Will be created if it doesn't exist.

    Returns:
        logging.Logger: Configured logger instance

    Example:
        >>> logger = get_logger_for_module(__name__)
        >>> # Creates logs/my_module.log
    """
    # Create logs directory if it doesn't exist
    os.makedirs(log_dir, exist_ok=True)

    # Generate log filename from module name
    # e.g., "pybirdai.process_steps.ancrdt_transformation.ancrdt_importer"
    # becomes "ancrdt_importer.log"
    module_basename = module_name.split('.')[-1]
    log_file = os.path.join(log_dir, f"{module_basename}.log")

    return setup_ancrdt_logger(module_name, log_file)
