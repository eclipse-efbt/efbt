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
#

"""
Condition Parser for Flexible Gap Analysis.

Parses product breakdown condition strings in formats:
- Single variable: TYP_INSTRMNT=TYP_INSTRMNT_970
- Multi-variable: TYP_INSTRMNT=TYP_INSTRMNT_970:TYP_CLLRL=TYP_CLLRL_1
- No breakdown: empty string
- Legacy format: TYP_INSTRMNT_970 (variable inferred from prefix)
"""

import re
from typing import List, Dict, Optional


class BreakdownCondition:
    """
    Represents a product breakdown condition.

    Contains zero, one, or multiple variable=member conditions combined with AND logic.
    """

    # Known variable prefixes for legacy format inference
    KNOWN_PREFIXES = {
        'TYP_INSTRMNT_': 'TYP_INSTRMNT',
        'TYP_ACCNTNG_ITM_': 'TYP_ACCNTNG_ITM',
        'TYP_CLLRL_': 'TYP_CLLRL',
        'TYP_SCRTY_': 'TYP_SCRTY',
        'TYP_PRTCTN_': 'TYP_PRTCTN',
        'TYP_ENTTY_': 'TYP_ENTTY',
        'INSTRMNT_TYP_PRDCT_': 'INSTRMNT_TYP_PRDCT',
        'SCRTY_EXCHNG_TRDBL_DRVTV_TYP_': 'SCRTY_EXCHNG_TRDBL_DRVTV_TYP',
        'TYP_INSTRMNT_': 'TYP_INSTRMNT',
    }

    def __init__(self, condition_string: str):
        self.original_string = condition_string
        self.conditions = self._parse(condition_string)

    def _parse(self, condition_string: str) -> List[Dict[str, str]]:
        """Parse condition string into list of {variable, member} dicts."""
        if not condition_string or condition_string.strip() == "":
            return []

        conditions = []
        for part in condition_string.split(":"):
            part = part.strip()
            if not part:
                continue

            if "=" in part:
                variable, member = part.split("=", 1)
                conditions.append({"variable": variable.strip(), "member": member.strip()})
            else:
                inferred = self._infer_variable(part)
                if inferred:
                    conditions.append(inferred)

        return conditions

    def _infer_variable(self, member: str) -> Optional[Dict[str, str]]:
        """Infer variable name from member ID prefix."""
        member = member.strip()

        for prefix, variable in self.KNOWN_PREFIXES.items():
            if member.startswith(prefix):
                return {"variable": variable, "member": member}

        # Fallback: extract variable by finding last underscore followed by digits
        match = re.match(r'^(.+)_(\d+)$', member)
        if match:
            return {"variable": match.group(1), "member": member}

        raise ValueError(f"Cannot infer variable from member: {member}. Use 'VARIABLE=MEMBER' format.")

    def is_empty(self) -> bool:
        """True if no conditions (no breakdown needed)."""
        return len(self.conditions) == 0

    def get_variables(self) -> List[str]:
        """List of variable names."""
        return [c["variable"] for c in self.conditions]

    def get_members(self) -> List[str]:
        """List of member IDs."""
        return [c["member"] for c in self.conditions]

    def get_condition_for_variable(self, variable: str) -> Optional[str]:
        """Get member ID for a specific variable."""
        for c in self.conditions:
            if c["variable"] == variable:
                return c["member"]
        return None

    def get_filter_expression(self) -> str:
        """Generate pandas DataFrame filter expression."""
        if self.is_empty():
            return "True"

        expressions = [f"(df['{c['variable']}'] == '{c['member']}')" for c in self.conditions]
        return " & ".join(expressions)

    def get_row_filter_expression(self) -> str:
        """Generate filter expression for a single row dict."""
        if self.is_empty():
            return "True"

        expressions = [f"row['{c['variable']}'] == '{c['member']}'" for c in self.conditions]
        return " and ".join(expressions)

    def get_id(self) -> str:
        """Unique identifier for this condition."""
        if self.is_empty():
            return "ALL"
        return "_".join(c["member"] for c in self.conditions)

    def get_description(self) -> str:
        """Human-readable description."""
        if self.is_empty():
            return "No breakdown (all data)"
        return " AND ".join(f"{c['variable']}={c['member']}" for c in self.conditions)

    def to_canonical_string(self) -> str:
        """Convert to VAR1=MEM1:VAR2=MEM2 format."""
        if self.is_empty():
            return ""
        return ":".join(f"{c['variable']}={c['member']}" for c in self.conditions)

    def __str__(self) -> str:
        return self.get_description()

    def __repr__(self) -> str:
        return f"BreakdownCondition({self.original_string!r})"

    def __eq__(self, other):
        if not isinstance(other, BreakdownCondition):
            return False
        return self.conditions == other.conditions

    def __hash__(self):
        return hash(tuple((c["variable"], c["member"]) for c in self.conditions))
