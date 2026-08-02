# coding=UTF-8
# Copyright (c) 2026 Bird Software Solutions Ltd
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
"""Migrate and lint ``__bird_annotations__`` in a generated Django LDM model.

Migrating folds the legacy ``sql_developer`` namespace into ``ldm``, normalizes
``"Y"``/``"N"`` flags to booleans, and drops the SQLDeveloper export keys that
``specs/BIRD_LDM_ANNOTATIONS_SPEC.md`` places out of contract.

Linting (``--check``) reports contract violations without writing anything, so
hand edits can be validated in the same way generated annotations are.
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from pybirdai.process_steps.forward_engineering import ldm_annotations  # noqa: E402


def migrate_ldm_annotations(
    model_path: str | Path,
    output_path: str | Path | None = None,
) -> dict[str, int]:
    """Rewrite one Django LDM model's annotations into the canonical form."""

    model_path = Path(model_path)
    output_path = Path(output_path) if output_path is not None else model_path

    migrated_source, changed_class_count = ldm_annotations.rewrite_annotations(
        model_path.read_text(encoding="utf-8"),
        lambda _class_name, annotations: ldm_annotations.canonical_annotations(annotations),
    )
    output_path.write_text(migrated_source, encoding="utf-8")

    return {
        "changed_class_count": changed_class_count,
        "remaining_issue_count": len(check_ldm_annotations(output_path)),
    }


def check_ldm_annotations(model_path: str | Path) -> list[str]:
    """Return every contract violation found in a Django LDM model."""

    issues: list[str] = []

    def collect(class_name: str, annotations: dict) -> dict:
        issues.extend(ldm_annotations.validate_annotations(annotations, class_name))
        return annotations

    ldm_annotations.rewrite_annotations(Path(model_path).read_text(encoding="utf-8"), collect)
    return issues


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--model", required=True, type=Path, help="Django LDM model to migrate")
    parser.add_argument("--output", type=Path, help="Output path. Defaults to overwriting --model")
    parser.add_argument(
        "--check",
        action="store_true",
        help="Only report contract violations, without rewriting the model",
    )
    args = parser.parse_args(argv)

    if args.check:
        issues = check_ldm_annotations(args.model)
        for issue in issues:
            print(issue)
        print(f"{len(issues)} annotation contract issues in {args.model}")
        return 1 if issues else 0

    summary = migrate_ldm_annotations(args.model, args.output)
    print(f"Migrated annotations on {summary['changed_class_count']} classes.")
    if summary["remaining_issue_count"]:
        print(f"{summary['remaining_issue_count']} annotation contract issues remain. Run with --check.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
