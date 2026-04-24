# coding=UTF-8
# Copyright (c) 2026 Bird Software Solutions Ltd
#
# SPDX-License-Identifier: EPL-2.0

"""Helpers for safely extracting ZIP archives from external sources."""

import os
import shutil
from pathlib import Path, PurePosixPath
from zipfile import ZipFile, ZipInfo


def _safe_zip_target(destination: str, member_name: str) -> Path:
    """Resolve a ZIP member path and ensure it stays under destination."""
    if not member_name:
        raise ValueError("ZIP member has an empty path")

    normalized_name = member_name.replace("\\", "/")
    pure_path = PurePosixPath(normalized_name)
    if pure_path.is_absolute() or any(part in ("", ".", "..") for part in pure_path.parts):
        raise ValueError(f"Unsafe ZIP member path: {member_name!r}")

    destination_path = Path(destination).resolve()
    target_path = (destination_path / Path(*pure_path.parts)).resolve()

    if os.path.commonpath([str(destination_path), str(target_path)]) != str(destination_path):
        raise ValueError(f"Unsafe ZIP member path: {member_name!r}")

    return target_path


def safe_extract(zip_file: ZipFile, destination: str) -> None:
    """Extract ZIP contents without allowing path traversal outside destination."""
    destination_path = Path(destination).resolve()
    destination_path.mkdir(parents=True, exist_ok=True)

    for member in zip_file.infolist():
        safe_extract_member(zip_file, member, destination)


def safe_extract_member(zip_file: ZipFile, member: ZipInfo, destination: str) -> Path:
    """Safely extract a single ZIP member and return its destination path."""
    target_path = _safe_zip_target(destination, member.filename)

    if member.is_dir():
        target_path.mkdir(parents=True, exist_ok=True)
        return target_path

    target_path.parent.mkdir(parents=True, exist_ok=True)
    with zip_file.open(member) as source, open(target_path, "wb") as target:
        shutil.copyfileobj(source, target)

    return target_path
