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
#    Benjamin Arfa - refactored to standalone functions
"""
Automode database setup orchestrator.

This module provides the core business logic for database setup operations.
All functions are standalone and called by the RunApplicationSetup entry point.
"""

import os
import sys
import ast
import glob
import hashlib
import json
import logging
import shutil
import subprocess
import time

from django.conf import settings

logger = logging.getLogger(__name__)

SETUP_FINGERPRINT_PATHS = [
    "automode_config.json",
    os.path.join("artefacts", "derivation_files"),
    os.path.join("artefacts", "joins_configuration"),
    os.path.join("artefacts", "smcubes_artefacts"),
    os.path.join("resources", "derivation_files"),
    os.path.join("resources", "il"),
    os.path.join("resources", "ldm"),
]


# =============================================================================
# Public API Functions
# =============================================================================

def run_automode_setup(app_name: str, app_module: str, token: str = "") -> dict:
    """
    Execute Step 1: Environment cleanup and derivation file generation.

    This step cleans the environment and generates derivation files from
    transformation rules. User can then configure derived fields before
    running step 2.

    Args:
        app_name: Django app name (e.g., 'pybirdai')
        app_module: Django module name (e.g., 'birds_nest')
        token: Optional GitHub token for artifact fetching

    Returns:
        dict with success status and completion details
    """
    try:
        logger.info("Starting automode database setup - Step 1...")
        base_dir = settings.BASE_DIR
        config = _load_temp_config()
        _remove_setup_ready_marker(base_dir)

        # Step 1a: Clean migrations
        _clean_migrations(base_dir)

        # Step 1b: Clean admin.py and bird_data_model.py
        _clean_admin_and_models(base_dir)
        logger.info("Step 1a: Environment cleaned successfully.")

        # Step 1c: Generate derivation files from transformation rules for EIL only.
        derivation_steps = _prepare_derivation_files(base_dir, config)

        # Step 1d: Handle full execution mode if configured
        if config and config.get("when_to_stop") == "FULL_EXECUTION":
            _transfer_generated_python_files()

        # Create migration ready marker
        _create_migration_ready_marker(base_dir)

        logger.info("Step 1 completed successfully!")
        return {
            "success": True,
            "message": "Environment cleaned and derivation files generated.",
            "server_restart_required": False,
            "next_step": "Configure derivation rules and click 'Setup Database'",
            "steps_completed": ["Environment cleaned"] + derivation_steps,
        }

    except Exception as e:
        logger.error(f"Automode setup step 1 failed: {str(e)}")
        raise


def run_post_setup(app_name: str, app_module: str, token: str = "") -> dict:
    """
    Execute Step 2: Generate models, merge derivations, update admin.py.

    This step generates Django models from LDM, merges derived fields,
    and updates admin.py. Requires server restart after completion.

    Args:
        app_name: Django app name
        app_module: Django module name
        token: Optional GitHub token

    Returns:
        dict with success status and completion details
    """
    from pybirdai.entry_points.create_django_models import RunCreateDjangoModels
    from pybirdai.process_steps.database_setup.derived_fields_merger import merge_all_derived_fields_into_model

    try:
        logger.info("Starting Step 2: Model generation and admin update...")
        base_dir = settings.BASE_DIR
        _remove_setup_ready_marker(base_dir)

        # Paths
        pybirdai_admin_path = os.path.join(base_dir, "pybirdai", "admin.py")
        pybirdai_meta_data_model_path = os.path.join(base_dir, "pybirdai", "models", "bird_meta_data_model.py")
        pybirdai_models_path = os.path.join(base_dir, "pybirdai", "models", "bird_data_model.py")
        results_models_path = os.path.join(base_dir, "results", "database_configuration_files", "models.py")

        # Step 2a: Generate Django models from LDM
        logger.info("Step 2a: Generating Django models from LDM...")
        try:
            django_models = RunCreateDjangoModels(app_name, app_module)
            django_models.ready()
            logger.info("Django models generated successfully.")
        except Exception as e:
            import traceback
            logger.error(f"Failed to create Django models: {str(e)}")
            logger.error(f"Full traceback:\n{traceback.format_exc()}")
            raise RuntimeError(f"Django model creation failed: {str(e)}") from e

        if not os.path.exists(results_models_path):
            raise RuntimeError(f"Generated models file not found: {results_models_path}")

        # Step 2b: Copy models to bird_data_model.py
        logger.info("Step 2b: Copying generated models to bird_data_model.py...")
        _update_models_file(pybirdai_models_path, results_models_path)

        # Step 2c: Merge derived fields
        logger.info("Step 2c: Merging derived fields into model...")
        manual_derivation_dir = os.path.join(base_dir, "resources", "derivation_files", "manually_generated")
        member_link_derivation_dir = os.path.join(base_dir, "resources", "derivation_files", "generated_from_member_links")
        generated_derivation_dir = os.path.join(base_dir, "resources", "derivation_files", "generated_from_logical_transformation_rules")
        derivation_config_file = os.path.join(base_dir, "resources", "derivation_files", "derivation_config.csv")

        os.makedirs(manual_derivation_dir, exist_ok=True)
        os.makedirs(member_link_derivation_dir, exist_ok=True)
        os.makedirs(generated_derivation_dir, exist_ok=True)

        merge_all_derived_fields_into_model(
            pybirdai_models_path,
            manual_dir=manual_derivation_dir,
            member_link_dir=member_link_derivation_dir,
            generated_dir=generated_derivation_dir,
            config_file=derivation_config_file,
        )
        logger.info("Derived fields merged successfully.")

        # Step 2d: Update admin.py
        logger.info("Step 2d: Updating admin.py...")
        _update_admin_file(pybirdai_admin_path, pybirdai_meta_data_model_path, pybirdai_models_path)

        _create_migration_ready_marker(base_dir)

        logger.info("Step 2 completed successfully!")
        return {
            "success": True,
            "step": 2,
            "message": "Models generated and admin files updated. Server will restart.",
            "next_action": "run_migrations",
            "server_restart_required": True,
            "steps_completed": [
                "Django models generated from LDM",
                "Models copied to bird_data_model.py",
                "Derived fields merged",
                "Admin.py updated",
            ],
        }

    except Exception as e:
        logger.error(f"Step 2 failed: {str(e)}")
        raise


def run_migrations(app_name: str, app_module: str, token: str = "") -> dict:
    """
    Execute Step 3: Run Django migrations.

    This step runs makemigrations and migrate to create the database.
    Should be called after server restart from step 2.

    Args:
        app_name: Django app name
        app_module: Django module name
        token: Optional GitHub token

    Returns:
        dict with success status and completion details
    """
    try:
        logger.info("Starting Step 3: Running migrations...")
        base_dir = settings.BASE_DIR

        if not _check_migration_ready_marker(base_dir):
            raise RuntimeError("Migration ready marker not found. Please run Step 1 first.")

        _run_migrations_in_subprocess(base_dir, token)
        _remove_migration_ready_marker(base_dir)

        logger.info("Step 3 completed successfully!")
        return {
            "success": True,
            "step": 3,
            "message": "Database migrations completed successfully",
            "database_ready": True,
        }

    except Exception as e:
        logger.error(f"Step 3 failed: {str(e)}")
        raise


# =============================================================================
# Private Helper Functions
# =============================================================================

def _clean_migrations(base_dir):
    """Remove existing migration files."""
    migration_path = os.path.join(base_dir, "pybirdai", "migrations")
    for file in os.listdir(migration_path):
        if file.endswith(".py") and not file.startswith("__"):
            logger.info(f"Removing migration: {file}")
            os.remove(os.path.join(migration_path, file))


def _clean_admin_and_models(base_dir):
    """Clean admin.py and bird_data_model.py."""
    admin_path = os.path.join(base_dir, "pybirdai", "admin.py")
    models_path = os.path.join(base_dir, "pybirdai", "models", "bird_data_model.py")

    if os.path.exists(admin_path):
        with open(admin_path, "r") as rf:
            admin_header = rf.read().split("\n\n")[0]
        with open(admin_path, "w") as wf:
            wf.write(admin_header)

    if os.path.exists(models_path):
        with open(models_path, "w") as wf:
            wf.write("")


def _load_temp_config():
    """Load temporary configuration file."""
    try:
        base_dir = str(settings.BASE_DIR)
        config_path = os.path.join(base_dir, "automode_config.json")

        if os.path.exists(config_path):
            with open(config_path, "r") as f:
                return json.load(f)
        return None
    except Exception as e:
        logger.error(f"Error loading config: {e}")
        return None


def _is_eldm_config(config):
    """Return True when the active setup is for the ELDM logical model."""
    return (config or {}).get("data_model_type", "").upper() == "ELDM"


def _remove_file_if_exists(path, description):
    try:
        if os.path.exists(path):
            os.remove(path)
            logger.info(f"Removed stale {description}: {path}")
    except OSError as e:
        logger.warning(f"Could not remove stale {description} {path}: {e}")


def _clear_generated_logical_derivations(generated_output_dir):
    """Remove generated logical-transformation derivations that only apply to EIL."""
    os.makedirs(generated_output_dir, exist_ok=True)
    removed_count = 0

    for file_name in os.listdir(generated_output_dir):
        file_path = os.path.join(generated_output_dir, file_name)
        if file_name == "tmp":
            continue
        try:
            if os.path.isdir(file_path):
                if file_name == "__pycache__":
                    shutil.rmtree(file_path)
                    removed_count += 1
            elif os.path.isfile(file_path):
                os.remove(file_path)
                removed_count += 1
        except OSError as e:
            logger.warning(f"Could not remove generated derivation artefact {file_path}: {e}")

    tmp_file = os.path.join(generated_output_dir, "tmp")
    if not os.path.exists(tmp_file):
        with open(tmp_file, "w"):
            pass

    if removed_count:
        logger.info(f"Removed {removed_count} generated logical derivation artefact(s)")


def _prepare_derivation_files(base_dir, config):
    """Create EIL derivation config, or remove stale EIL derivations for ELDM."""
    generated_output_dir = os.path.join(
        base_dir,
        "resources",
        "derivation_files",
        "generated_from_logical_transformation_rules",
    )
    config_csv = os.path.join(
        base_dir,
        "resources",
        "derivation_files",
        "derivation_config.csv",
    )

    if _is_eldm_config(config):
        logger.info("Skipping logical transformation derivation config for ELDM setup.")
        _remove_file_if_exists(config_csv, "derivation config")
        _clear_generated_logical_derivations(generated_output_dir)
        return ["EIL logical derivation config skipped for ELDM"]

    from pybirdai.process_steps.database_setup.derivation_pipeline import (
        run_generate_derivation_files,
        export_available_rules_to_config,
    )

    logger.info("Step 1b: Generating derivation Python files from transformation rules...")
    transformation_rules_csv = os.path.join(
        base_dir, "artefacts", "smcubes_artefacts", "logical_transformation_rule.csv"
    )

    if os.path.exists(transformation_rules_csv):
        try:
            generated_files = run_generate_derivation_files(
                transformation_rules_csv=transformation_rules_csv,
                output_dir=generated_output_dir,
            )
            logger.info(f"Generated {len(generated_files)} derivation file(s)")

            export_available_rules_to_config(
                transformation_rules_csv=transformation_rules_csv,
                config_csv=config_csv,
                enabled_by_default=False,
            )
            logger.info(f"Exported available rules to config: {config_csv}")
            return [
                "Derivation Python files generated",
                "Derivation config CSV exported",
            ]
        except Exception as e:
            logger.warning(f"Could not generate derivation files: {e}")
    else:
        logger.info(f"Transformation rules CSV not found: {transformation_rules_csv}")

    return ["Derivation setup checked"]


def _transfer_generated_python_files():
    """Transfer generated Python files to filter_code directory."""
    source_dir = os.path.join(".", "resources", "generated_python")
    target_dir = os.path.join(".", "pybirdai", "process_steps", "filter_code")

    if not os.path.exists(source_dir):
        return

    os.makedirs(target_dir, exist_ok=True)

    for file_name in os.listdir(source_dir):
        if file_name.endswith(".py"):
            shutil.copy2(os.path.join(source_dir, file_name), os.path.join(target_dir, file_name))
            logger.info(f"Transferred {file_name}")


def _update_models_file(pybirdai_models_path, results_models_path):
    """Copy generated models to bird_data_model.py."""
    if not os.path.exists(results_models_path):
        logger.warning(f"Results models file not found: {results_models_path}")
        return

    with open(results_models_path, "r") as f:
        content = f.read()

    with open(pybirdai_models_path, "w") as f:
        f.write("\n")
        f.write(content)

    logger.info(f"{pybirdai_models_path} updated successfully.")


def _update_admin_file(pybirdai_admin_path, pybirdai_meta_data_model_path, pybirdai_data_model_path):
    """Update admin.py with model registrations."""
    registered_models = set()
    models_dir = os.path.join(os.path.dirname(pybirdai_admin_path), "models")

    with open(pybirdai_admin_path, "w") as f:
        f.write("from django.contrib import admin\n\n")

        for model_file_path in glob.glob(os.path.join(models_dir, "*.py")):
            if model_file_path.endswith("__init__.py"):
                continue

            model_module_name = os.path.basename(model_file_path)[:-3]

            try:
                with open(model_file_path, "r") as rf:
                    tree = ast.parse(rf.read())

                    for node in ast.walk(tree):
                        if isinstance(node, ast.ClassDef) and node.name not in ["Meta", "Admin"]:
                            if not _is_abstract_model(node) and node.name not in registered_models:
                                f.write(f"from .models.{model_module_name} import {node.name}\n")
                                f.write(f"admin.site.register({node.name})\n")
                                registered_models.add(node.name)
            except Exception as e:
                logger.warning(f"Error processing {model_file_path}: {e}")


def _is_abstract_model(class_node):
    """Check if a Django model class is abstract."""
    for node in class_node.body:
        if isinstance(node, ast.ClassDef) and node.name == "Meta":
            for meta_node in node.body:
                if isinstance(meta_node, ast.Assign):
                    for target in meta_node.targets:
                        if isinstance(target, ast.Name) and target.id == "abstract":
                            if isinstance(meta_node.value, ast.Constant) and meta_node.value.value is True:
                                return True
    return False


def _run_migrations_in_subprocess(base_dir, token: str = ""):
    """Run Django migrations in a subprocess."""
    from pybirdai.process_steps.database_setup.migration_generator import AdvancedMigrationGenerator

    original_dir = os.getcwd()
    start_time = time.time()

    try:
        # Remove existing database
        db_file = "db.sqlite3"
        if os.path.exists(db_file):
            os.remove(db_file)

        python_executable = _get_python_executable()

        # Generate initial migration
        generator = AdvancedMigrationGenerator()
        models = generator.parse_files([
            f"pybirdai{os.sep}models{os.sep}bird_data_model.py",
            f"pybirdai{os.sep}models{os.sep}bird_meta_data_model.py"
        ])
        generator.save_migration_file(models, f"pybirdai{os.sep}migrations{os.sep}0001_initial.py")

        # Run makemigrations
        logger.info("Running makemigrations...")
        result = subprocess.run(
            [python_executable, "manage.py", "makemigrations", "pybirdai"],
            capture_output=True, text=True, timeout=900, shell=(os.name == 'nt')
        )
        if result.returncode != 0:
            raise RuntimeError(f"Makemigrations failed: {result.stderr}")
        logger.info(f"Makemigrations completed in {time.time() - start_time:.2f}s")

        # Remove database again before migrate
        if os.path.exists(db_file):
            os.remove(db_file)

        # Run migrate
        logger.info("Running migrate...")
        result = subprocess.run(
            [python_executable, "manage.py", "migrate"],
            capture_output=True, text=True, timeout=600, shell=(os.name == 'nt')
        )
        if result.returncode != 0:
            raise RuntimeError(f"Migrate failed: {result.stderr}")

        # Recreate superuser after database reset
        logger.info("Creating superuser...")
        result = subprocess.run(
            [python_executable, "manage.py", "ensure_superuser"],
            capture_output=True, text=True, timeout=60, shell=(os.name == 'nt')
        )
        if result.returncode != 0:
            logger.warning(f"Superuser creation warning: {result.stderr}")
        else:
            logger.info("Superuser created successfully")

        total_time = time.time() - start_time
        logger.info(f"Migrations completed in {total_time:.2f}s")

        _create_setup_ready_marker(base_dir)

    except subprocess.TimeoutExpired as e:
        raise RuntimeError(f"Migration timed out after {e.timeout}s")
    finally:
        os.chdir(original_dir)


def _get_python_executable():
    """Get the correct Python executable."""
    python_executable = sys.executable
    venv_path = os.environ.get("VIRTUAL_ENV")

    if venv_path:
        if os.name == 'nt':
            python_executable = os.path.join(venv_path, "Scripts", "python.exe")
        else:
            python_executable = os.path.join(venv_path, "bin", "python")

    os.chdir(settings.BASE_DIR)
    return python_executable


def _create_migration_ready_marker(base_dir):
    """Create marker file indicating ready for migrations."""
    marker_path = os.path.join(str(base_dir), ".migration_ready_marker")
    try:
        with open(marker_path, "w") as f:
            json.dump({"step": 1, "timestamp": time.time(), "status": "ready"}, f)
        logger.info(f"Created migration ready marker: {marker_path}")
    except Exception as e:
        logger.error(f"Failed to create migration marker: {e}")


def _check_migration_ready_marker(base_dir):
    """Check if migration ready marker exists."""
    marker_path = os.path.join(str(base_dir), ".migration_ready_marker")
    return os.path.exists(marker_path)


def _remove_migration_ready_marker(base_dir):
    """Remove the migration ready marker file."""
    marker_path = os.path.join(str(base_dir), ".migration_ready_marker")
    try:
        if os.path.exists(marker_path):
            os.remove(marker_path)
            logger.info(f"Removed migration ready marker: {marker_path}")
    except Exception as e:
        logger.error(f"Failed to remove migration marker: {e}")


def _remove_setup_ready_marker(base_dir):
    """Remove the setup ready marker because generated models need to be refreshed."""
    marker_path = os.path.join(str(base_dir), ".setup_ready_marker")
    try:
        if os.path.exists(marker_path):
            os.remove(marker_path)
            logger.info(f"Removed setup ready marker: {marker_path}")
    except Exception as e:
        logger.error(f"Failed to remove setup ready marker: {e}")


def _iter_setup_input_files(base_dir):
    """Yield files that affect generated bird data models."""
    base_dir = str(base_dir)
    for relative_path in SETUP_FINGERPRINT_PATHS:
        absolute_path = os.path.join(base_dir, relative_path)
        if os.path.isfile(absolute_path):
            yield relative_path, absolute_path
            continue

        if not os.path.isdir(absolute_path):
            continue

        for root, dirs, files in os.walk(absolute_path):
            dirs[:] = sorted(d for d in dirs if d != "__pycache__")
            for file_name in sorted(files):
                if file_name == "tmp" or file_name.endswith(".pyc"):
                    continue
                file_path = os.path.join(root, file_name)
                yield os.path.relpath(file_path, base_dir), file_path


def _compute_setup_input_fingerprint(base_dir):
    """Create a cheap fingerprint of inputs used to generate bird_data_model.py."""
    digest = hashlib.sha256()
    for relative_path, file_path in sorted(_iter_setup_input_files(base_dir)):
        try:
            stat = os.stat(file_path)
        except OSError:
            continue

        digest.update(relative_path.replace(os.sep, "/").encode("utf-8"))
        digest.update(b"\0")
        digest.update(str(stat.st_size).encode("ascii"))
        digest.update(b"\0")
        digest.update(str(stat.st_mtime_ns).encode("ascii"))
        digest.update(b"\0")

    return digest.hexdigest()


def is_setup_ready(base_dir):
    """Return True only when setup marker matches the current artefact inputs."""
    marker_path = os.path.join(str(base_dir), ".setup_ready_marker")
    if not os.path.exists(marker_path):
        return False

    try:
        with open(marker_path, "r") as f:
            marker_data = json.load(f)
    except (OSError, json.JSONDecodeError) as e:
        logger.warning(f"Setup ready marker could not be read: {e}")
        return False

    stored_fingerprint = marker_data.get("input_fingerprint")
    if not stored_fingerprint:
        logger.info("Setup ready marker has no input fingerprint and will be treated as stale.")
        return False

    current_fingerprint = _compute_setup_input_fingerprint(base_dir)
    if stored_fingerprint != current_fingerprint:
        logger.info("Setup ready marker is stale because artefact inputs changed.")
        return False

    return True


def _create_setup_ready_marker(base_dir):
    """Create marker file indicating setup is complete."""
    marker_path = os.path.join(str(base_dir), ".setup_ready_marker")
    try:
        with open(marker_path, "w") as f:
            json.dump(
                {
                    "step": 2,
                    "timestamp": time.time(),
                    "status": "complete",
                    "input_fingerprint": _compute_setup_input_fingerprint(base_dir),
                },
                f,
            )
        logger.info(f"Created setup ready marker: {marker_path}")
    except Exception as e:
        logger.error(f"Failed to create setup ready marker: {e}")
