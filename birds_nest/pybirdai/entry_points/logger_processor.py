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

from django.apps import AppConfig
import logging
from pathlib import Path

logger = logging.getLogger(__name__)


class LoggerProcessorConfig(AppConfig):
    """
    Django AppConfig for Logger Processing operations.
    Provides centralized logging configuration and management functionality.
    """
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'pybirdai.entry_points.logger_processor'
    verbose_name = 'Logger Processor'

    def ready(self):
        """Initialize the logger processor when Django starts."""
        logger.info("Logger Processor initialized")


def return_logger(file_name: str):
    """
    Get a logger instance for a specific file.
    
    Args:
        file_name (str): Name of the file requesting the logger
        
    Returns:
        logging.Logger: Configured logger instance
    """
    return logging.getLogger(file_name)


def configure_logging(level=logging.INFO, format_string=None, log_file=None, **kwargs):
    """
    Configure logging for the application.
    
    Args:
        level: Logging level (default: INFO)
        format_string (str): Custom format string for log messages
        log_file (str): Optional file to write logs to
        **kwargs: Additional logging configuration parameters
        
    Returns:
        dict: Configuration results
    """
    try:
        if format_string is None:
            format_string = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
        
        # Configure basic logging
        logging.basicConfig(
            level=level,
            format=format_string,
            **kwargs
        )
        
        # Add file handler if specified
        if log_file:
            file_handler = logging.FileHandler(log_file)
            file_handler.setLevel(level)
            formatter = logging.Formatter(format_string)
            file_handler.setFormatter(formatter)
            
            # Add to root logger
            root_logger = logging.getLogger()
            root_logger.addHandler(file_handler)
        
        logger.info(f"Logging configured with level: {logging.getLevelName(level)}")
        
        return {
            'success': True,
            'level': logging.getLevelName(level),
            'format': format_string,
            'log_file': log_file,
            'message': 'Logging configuration completed successfully'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Logging configuration failed'
        }


def get_logger_for_module(module_name: str, level=None):
    """
    Get a configured logger for a specific module.
    
    Args:
        module_name (str): Name of the module
        level: Optional logging level for this specific logger
        
    Returns:
        logging.Logger: Configured logger for the module
    """
    module_logger = logging.getLogger(module_name)
    
    if level is not None:
        module_logger.setLevel(level)
    
    return module_logger


def set_logger_level(logger_name: str, level):
    """
    Set logging level for a specific logger.
    
    Args:
        logger_name (str): Name of the logger
        level: New logging level
        
    Returns:
        dict: Result with success status
    """
    try:
        target_logger = logging.getLogger(logger_name)
        target_logger.setLevel(level)
        
        logger.info(f"Logger '{logger_name}' level set to {logging.getLevelName(level)}")
        
        return {
            'success': True,
            'logger_name': logger_name,
            'new_level': logging.getLevelName(level),
            'message': f'Logger level updated successfully'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to set logger level'
        }


def add_log_handler(logger_name: str, handler_type="file", handler_config=None):
    """
    Add a handler to a specific logger.
    
    Args:
        logger_name (str): Name of the logger
        handler_type (str): Type of handler - "file", "console", "rotating"
        handler_config (dict): Handler configuration parameters
        
    Returns:
        dict: Result with success status
    """
    try:
        target_logger = logging.getLogger(logger_name)
        
        if handler_config is None:
            handler_config = {}
        
        if handler_type == "file":
            filename = handler_config.get('filename', f'{logger_name}.log')
            handler = logging.FileHandler(filename)
        elif handler_type == "console":
            handler = logging.StreamHandler()
        elif handler_type == "rotating":
            from logging.handlers import RotatingFileHandler
            filename = handler_config.get('filename', f'{logger_name}.log')
            max_bytes = handler_config.get('max_bytes', 10485760)  # 10MB
            backup_count = handler_config.get('backup_count', 5)
            handler = RotatingFileHandler(filename, maxBytes=max_bytes, backupCount=backup_count)
        else:
            raise ValueError(f"Unknown handler type: {handler_type}")
        
        # Configure handler
        level = handler_config.get('level', logging.INFO)
        handler.setLevel(level)
        
        format_string = handler_config.get('format', '%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        formatter = logging.Formatter(format_string)
        handler.setFormatter(formatter)
        
        target_logger.addHandler(handler)
        
        logger.info(f"Added {handler_type} handler to logger '{logger_name}'")
        
        return {
            'success': True,
            'logger_name': logger_name,
            'handler_type': handler_type,
            'handler_config': handler_config,
            'message': f'{handler_type.title()} handler added successfully'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to add log handler'
        }


def log_system_info():
    """
    Log system and application information.
    
    Returns:
        dict: Result with logged information
    """
    try:
        import sys
        import platform
        from django.conf import settings
        
        system_logger = get_logger_for_module('system_info')
        
        system_logger.info(f"Python version: {sys.version}")
        system_logger.info(f"Platform: {platform.platform()}")
        system_logger.info(f"Django version: {getattr(settings, 'DJANGO_VERSION', 'Unknown')}")
        system_logger.info(f"Application: PyBIRD AI")
        
        return {
            'success': True,
            'message': 'System information logged successfully'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to log system information'
        }


def create_logger_context(context_name: str, base_path=None):
    """
    Create a logging context for a specific operation or module.
    
    Args:
        context_name (str): Name of the logging context
        base_path (str): Base path for log files
        
    Returns:
        dict: Context information and logger instance
    """
    try:
        if base_path is None:
            base_path = Path.cwd() / 'logs'
        else:
            base_path = Path(base_path)
        
        # Ensure log directory exists
        base_path.mkdir(parents=True, exist_ok=True)
        
        # Create context logger
        context_logger = get_logger_for_module(f"context.{context_name}")
        
        # Add file handler for this context
        log_file = base_path / f"{context_name}.log"
        add_log_handler(
            f"context.{context_name}",
            "rotating",
            {
                'filename': str(log_file),
                'max_bytes': 5242880,  # 5MB
                'backup_count': 3
            }
        )
        
        context_logger.info(f"Logging context '{context_name}' created")
        
        return {
            'success': True,
            'context_name': context_name,
            'logger': context_logger,
            'log_file': str(log_file),
            'base_path': str(base_path),
            'message': f'Logging context created successfully'
        }
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to create logging context'
        }


def get_logger_status(logger_name=None):
    """
    Get status information about loggers.
    
    Args:
        logger_name (str): Optional specific logger name, or None for all loggers
        
    Returns:
        dict: Logger status information
    """
    try:
        status_info = {
            'success': True,
            'loggers': {}
        }
        
        if logger_name:
            # Get specific logger info
            target_logger = logging.getLogger(logger_name)
            status_info['loggers'][logger_name] = {
                'level': logging.getLevelName(target_logger.level),
                'handlers': len(target_logger.handlers),
                'effective_level': logging.getLevelName(target_logger.getEffectiveLevel()),
                'disabled': target_logger.disabled
            }
        else:
            # Get all loggers info
            for name in logging.Logger.manager.loggerDict:
                if isinstance(logging.Logger.manager.loggerDict[name], logging.Logger):
                    target_logger = logging.Logger.manager.loggerDict[name]
                    status_info['loggers'][name] = {
                        'level': logging.getLevelName(target_logger.level),
                        'handlers': len(target_logger.handlers),
                        'effective_level': logging.getLevelName(target_logger.getEffectiveLevel()),
                        'disabled': target_logger.disabled
                    }
        
        status_info['total_loggers'] = len(status_info['loggers'])
        
        return status_info
        
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Failed to get logger status'
        }


class LoggerProcessor:
    """
    Main logger processor class providing high-level logging management interface.
    """
    
    def __init__(self):
        """Initialize the logger processor."""
        self.contexts = {}
        logger.info("LoggerProcessor initialized")
    
    def setup_application_logging(self, app_config=None):
        """
        Set up comprehensive logging for the application.
        
        Args:
            app_config (dict): Application logging configuration
            
        Returns:
            dict: Setup results
        """
        if app_config is None:
            app_config = {
                'level': logging.INFO,
                'file_logging': True,
                'console_logging': True,
                'rotating_logs': True
            }
        
        setup_results = {
            'success': True,
            'configurations_applied': [],
            'errors': []
        }
        
        try:
            # Configure basic logging
            config_result = configure_logging(
                level=app_config.get('level', logging.INFO),
                format_string=app_config.get('format')
            )
            
            if config_result.get('success'):
                setup_results['configurations_applied'].append('basic_config')
            else:
                setup_results['errors'].append(f"Basic config failed: {config_result.get('error')}")
            
            # Set up application contexts
            contexts = app_config.get('contexts', ['main', 'database', 'api', 'processing'])
            for context_name in contexts:
                context_result = create_logger_context(context_name)
                if context_result.get('success'):
                    self.contexts[context_name] = context_result
                    setup_results['configurations_applied'].append(f'context_{context_name}')
                else:
                    setup_results['errors'].append(f"Context {context_name} failed: {context_result.get('error')}")
            
            # Log system information
            if app_config.get('log_system_info', True):
                log_system_info()
                setup_results['configurations_applied'].append('system_info')
            
        except Exception as e:
            setup_results['success'] = False
            setup_results['errors'].append(f"Setup error: {str(e)}")
            logger.error(f"Application logging setup error: {e}")
        
        setup_results['success'] = len(setup_results['errors']) == 0
        
        return setup_results
    
    def get_context_logger(self, context_name: str):
        """
        Get logger for a specific context.
        
        Args:
            context_name (str): Name of the context
            
        Returns:
            logging.Logger: Context logger
        """
        if context_name in self.contexts:
            return self.contexts[context_name]['logger']
        else:
            # Create context if it doesn't exist
            context_result = create_logger_context(context_name)
            if context_result.get('success'):
                self.contexts[context_name] = context_result
                return context_result['logger']
        
        return get_logger_for_module(f"context.{context_name}")
    
    def rotate_logs(self, context_name=None):
        """
        Manually rotate logs for contexts.
        
        Args:
            context_name (str): Optional specific context, or None for all contexts
            
        Returns:
            dict: Rotation results
        """
        rotation_results = {
            'success': True,
            'rotated_contexts': [],
            'errors': []
        }
        
        contexts_to_rotate = [context_name] if context_name else list(self.contexts.keys())
        
        for ctx_name in contexts_to_rotate:
            try:
                if ctx_name in self.contexts:
                    ctx_logger = self.contexts[ctx_name]['logger']
                    
                    # Find rotating handlers and trigger rotation
                    for handler in ctx_logger.handlers:
                        if hasattr(handler, 'doRollover'):
                            handler.doRollover()
                            rotation_results['rotated_contexts'].append(ctx_name)
                            break
                
            except Exception as e:
                rotation_results['errors'].append(f"Failed to rotate {ctx_name}: {str(e)}")
        
        rotation_results['success'] = len(rotation_results['errors']) == 0
        
        return rotation_results


# Convenience function for backwards compatibility
def run_logger_operations():
    """Get a configured logger processor instance."""
    return LoggerProcessor()


# Export main functions for easy access
__all__ = [
    'LoggerProcessorConfig',
    'return_logger',
    'configure_logging',
    'get_logger_for_module',
    'set_logger_level',
    'add_log_handler',
    'log_system_info',
    'create_logger_context',
    'get_logger_status',
    'LoggerProcessor',
    'run_logger_operations'
]