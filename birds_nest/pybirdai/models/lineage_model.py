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

from django.db import models
from django.contrib.contenttypes.fields import GenericForeignKey, GenericRelation
from django.contrib.contenttypes.models import ContentType
# Models defined in bird_data_model.py

# AORTA Models for Lineage Tracking

# Base Abstract Models
class AortaTable(models.Model):
    """Base class for all table types in AORTA lineage tracking"""
    class Meta:
        abstract = True

class AortaColumn(models.Model):
    """Base class for all column types in AORTA lineage tracking"""
    class Meta:
        abstract = True

class AortaRow(models.Model):
    """Base class for all row types in AORTA lineage tracking"""
    class Meta:
        abstract = True

class AortaActualValue(models.Model):
    """Base class for all value types in AORTA lineage tracking"""
    class Meta:
        abstract = True

# Table Models
class DatabaseTable(AortaTable):
    """Represents source data tables with fields"""
    name = models.CharField(max_length=255)
    
    def __str__(self):
        return f"DatabaseTable: {self.name}"

class DerivedTable(AortaTable):
    """Represents computed tables with functions"""
    name = models.CharField(max_length=255)
    table_creation_function = models.ForeignKey('TableCreationFunction', on_delete=models.CASCADE, null=True, blank=True)
    
    def __str__(self):
        return f"DerivedTable: {self.name}"

# Column Models
class DatabaseField(AortaColumn):
    """Physical database columns"""
    name = models.CharField(max_length=255)
    table = models.ForeignKey('DatabaseTable', related_name='database_fields', on_delete=models.CASCADE)
    
    def __str__(self):
        return f"DatabaseField: {self.table.name}.{self.name}"

class Function(AortaColumn):
    """Computed columns that reference other columns"""
    name = models.CharField(max_length=255)
    function_text = models.OneToOneField('FunctionText', on_delete=models.CASCADE)
    # We'll track column references separately since they can be of different types
    table = models.ForeignKey('DerivedTable', related_name='derived_functions', on_delete=models.CASCADE)
    
    def __str__(self):
        return f"Function: {self.name}"

# Row Models
class DatabaseRow(AortaRow):
    """Contains DatabaseColumnValues"""
    populated_table = models.ForeignKey('PopulatedDataBaseTable', on_delete=models.CASCADE)
    row_identifier = models.CharField(max_length=255, null=True, blank=True)
    
    def __str__(self):
        return f"DatabaseRow: {self.id}"

class DerivedTableRow(AortaRow):
    """Contains EvaluatedFunctions and references to source rows"""
    populated_table = models.ForeignKey('EvaluatedDerivedTable', on_delete=models.CASCADE)
    # Source rows will be tracked through a separate model
    row_identifier = models.CharField(max_length=255, null=True, blank=True)
    
    def __str__(self):
        return f"DerivedTableRow: {self.id}"

# Value Models
class DatabaseColumnValue(AortaActualValue):
    """Stores actual data values"""
    value = models.FloatField(null=True, blank=True)
    string_value = models.TextField(null=True, blank=True)  # For non-numeric values
    column = models.ForeignKey('DatabaseField', on_delete=models.CASCADE)
    row = models.ForeignKey('DatabaseRow', related_name='column_values', on_delete=models.CASCADE)
    
    def __str__(self):
        return f"DatabaseColumnValue: {self.column.name}={self.value or self.string_value}"

class EvaluatedFunction(AortaActualValue):
    """Stores computed values with lineage"""
    value = models.FloatField(null=True, blank=True)
    string_value = models.TextField(null=True, blank=True)  # For non-numeric values
    function = models.ForeignKey('Function', on_delete=models.CASCADE)
    row = models.ForeignKey('DerivedTableRow', related_name='evaluated_functions', on_delete=models.CASCADE)
    # Source values will be tracked through a separate model
    
    def __str__(self):
        return f"EvaluatedFunction: {self.function.name}={self.value or self.string_value}"

# Lineage Container Models
class Trail(models.Model):
    """Top-level container for execution traces"""
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    metadata_trail = models.OneToOneField('MetaDataTrail', on_delete=models.CASCADE)
    execution_context = models.JSONField(default=dict, blank=True)  # Store additional context
    
    def __str__(self):
        return f"Trail: {self.name} ({self.created_at})"

class MetaDataTrail(models.Model):
    """Contains table definitions (schema)"""
    # Tables are linked via foreign keys in the table models
    
    def __str__(self):
        return f"MetaDataTrail: {self.id}"

class PopulatedTable(models.Model):
    """Base class linking table definitions to actual data"""
    class Meta:
        abstract = True
    trail = models.ForeignKey('Trail', on_delete=models.CASCADE)

class PopulatedDataBaseTable(PopulatedTable):
    """Links DatabaseTable to its rows"""
    table = models.ForeignKey('DatabaseTable', on_delete=models.CASCADE)
    # rows are linked via foreign key in DatabaseRow
    
    class Meta:
        # Fix the reverse accessor clash
        default_related_name = 'populated_database_tables'
    
    def __str__(self):
        return f"PopulatedDataBaseTable: {self.table.name}"

class EvaluatedDerivedTable(PopulatedTable):
    """Links DerivedTable to its rows"""
    table = models.ForeignKey('DerivedTable', on_delete=models.CASCADE)
    # rows are linked via foreign key in DerivedTableRow
    
    class Meta:
        # Fix the reverse accessor clash
        default_related_name = 'evaluated_derived_tables'
    
    def __str__(self):
        return f"EvaluatedDerivedTable: {self.table.name}"

# Supporting Models
class FunctionText(models.Model):
    """Stores the actual function code/text"""
    text = models.TextField()
    language = models.CharField(max_length=50, default='python')
    
    def __str__(self):
        return f"FunctionText: {self.text[:50]}..."

class TableCreationFunction(models.Model):
    """Functions that create entire tables"""
    name = models.CharField(max_length=255)
    function_text = models.ForeignKey('FunctionText', on_delete=models.CASCADE)
    # Source tables will be tracked through a separate model
    
    def __str__(self):
        return f"TableCreationFunction: {self.name}"

# Additional models for AORTA metadata tracking
class AortaTableReference(models.Model):
    """Track table references in metadata trail"""
    metadata_trail = models.ForeignKey('MetaDataTrail', related_name='table_references', on_delete=models.CASCADE)
    table_content_type = models.CharField(max_length=50)  # 'DatabaseTable' or 'DerivedTable'
    table_id = models.PositiveIntegerField()
    
    class Meta:
        unique_together = ('metadata_trail', 'table_content_type', 'table_id')

# Relationship tracking models
class FunctionColumnReference(models.Model):
    """Track which columns a function references - scoped to a specific trail execution"""
    function = models.ForeignKey('Function', related_name='column_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseField and Function columns
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    referenced_column = GenericForeignKey('content_type', 'object_id')

    # Original dependency string from @lineage decorator (e.g., "Other_loans.GRSS_CRRYNG_AMNT")
    # This preserves the logical dependency even when the resolved object is on a base table
    dependency_string = models.CharField(max_length=255, blank=True, null=True)

    # Trail this reference belongs to - ensures each execution has isolated lineage
    trail = models.ForeignKey('Trail', related_name='function_column_references', on_delete=models.CASCADE, null=True, blank=True)

    def __str__(self):
        return f"FunctionColumnReference: {self.function.name} -> {self.dependency_string or self.referenced_column}"

class DerivedRowSourceReference(models.Model):
    """Track source rows for derived rows"""
    derived_row = models.ForeignKey('DerivedTableRow', related_name='source_row_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseRow and DerivedTableRow
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    source_row = GenericForeignKey('content_type', 'object_id')
    
    def __str__(self):
        return f"DerivedRowSourceReference: {self.derived_row} <- {self.source_row}"

class EvaluatedFunctionSourceValue(models.Model):
    """Track source values for evaluated functions"""
    evaluated_function = models.ForeignKey('EvaluatedFunction', related_name='source_value_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseColumnValue and EvaluatedFunction
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    source_value = GenericForeignKey('content_type', 'object_id')
    
    def __str__(self):
        return f"EvaluatedFunctionSourceValue: {self.evaluated_function} <- {self.source_value}"

class TableCreationSourceTable(models.Model):
    """Track source tables for table creation functions"""
    table_creation_function = models.ForeignKey('TableCreationFunction', related_name='source_table_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseTable and DerivedTable
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    source_table = GenericForeignKey('content_type', 'object_id')
    
    def __str__(self):
        return f"TableCreationSourceTable: {self.table_creation_function.name} <- {self.source_table}"

class TableCreationFunctionColumn(models.Model):
    """Track which columns a table creation function references in its lineage"""
    table_creation_function = models.ForeignKey('TableCreationFunction', related_name='column_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseField and Function (both inherit from AortaColumn)
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    column = GenericForeignKey('content_type', 'object_id')
    
    # Additional context about how this column is referenced
    reference_text = models.TextField(blank=True, help_text="The specific lineage text that references this column")
    
    def __str__(self):
        return f"TableCreationFunctionColumn: {self.table_creation_function.name} -> {self.column}"

# New models for tracking rows and fields used in calculations
class CalculationUsedRow(models.Model):
    """Track which rows were actually used in a calculation (passed filters)"""
    trail = models.ForeignKey('Trail', related_name='calculation_used_rows', on_delete=models.CASCADE)
    calculation_name = models.CharField(max_length=255, help_text="Name of the calculation cell (e.g., Cell_F_01_01_REF_FINREP_3_0_45749_REF)")
    # Generic relation to handle both DatabaseRow and DerivedTableRow
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    used_row = GenericForeignKey('content_type', 'object_id')
    
    class Meta:
        indexes = [
            models.Index(fields=['trail', 'calculation_name']),
        ]
    
    def __str__(self):
        return f"CalculationUsedRow: {self.calculation_name} used {self.used_row}"

class CalculationUsedField(models.Model):
    """Track which fields were actually accessed during a calculation"""
    trail = models.ForeignKey('Trail', related_name='calculation_used_fields', on_delete=models.CASCADE)
    calculation_name = models.CharField(max_length=255, help_text="Name of the calculation cell")
    # Generic relation to handle both DatabaseField and Function
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    used_field = GenericForeignKey('content_type', 'object_id')
    # Track which row this field was accessed from (optional)
    row_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, related_name='field_row_refs', null=True, blank=True)
    row_object_id = models.PositiveIntegerField(null=True, blank=True)
    row = GenericForeignKey('row_content_type', 'row_object_id')

    class Meta:
        indexes = [
            models.Index(fields=['trail', 'calculation_name']),
        ]

    def __str__(self):
        return f"CalculationUsedField: {self.calculation_name} used {self.used_field}"


# ============================================================================
# ENHANCED LINEAGE MODELS
# These models provide richer tracking of data transformation pipelines
# ============================================================================

class TransformationStep(models.Model):
    """
    Represents a single step in the data transformation pipeline.
    Tracks the sequence of operations from source data to final output.
    """
    STEP_TYPES = [
        ('SOURCE_LOAD', 'Source Data Load'),
        ('FILTER', 'Filter Operation'),
        ('JOIN', 'Join Operation'),
        ('AGGREGATE', 'Aggregation'),
        ('TRANSFORM', 'Transformation'),
        ('UNION', 'Union Operation'),
        ('OUTPUT', 'Output Generation'),
    ]

    trail = models.ForeignKey('Trail', related_name='transformation_steps', on_delete=models.CASCADE)
    step_number = models.PositiveIntegerField(help_text="Order of this step in the transformation sequence")
    step_type = models.CharField(max_length=20, choices=STEP_TYPES)
    step_name = models.CharField(max_length=255, help_text="Human-readable name for this step")
    description = models.TextField(blank=True, help_text="Detailed description of what this step does")

    # Link to the table/function that performs this step
    table_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, null=True, blank=True)
    table_object_id = models.PositiveIntegerField(null=True, blank=True)
    table = GenericForeignKey('table_content_type', 'table_object_id')

    # Statistics
    input_row_count = models.PositiveIntegerField(default=0, help_text="Number of rows coming into this step")
    output_row_count = models.PositiveIntegerField(default=0, help_text="Number of rows produced by this step")
    execution_time_ms = models.PositiveIntegerField(default=0, help_text="Time taken to execute this step in milliseconds")

    # Timestamps
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        ordering = ['trail', 'step_number']
        indexes = [
            models.Index(fields=['trail', 'step_number']),
            models.Index(fields=['step_type']),
        ]

    def __str__(self):
        return f"Step {self.step_number}: {self.step_name} ({self.step_type})"


class TransformationStepInput(models.Model):
    """Links a transformation step to its input tables/data sources"""
    step = models.ForeignKey('TransformationStep', related_name='inputs', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseTable and DerivedTable
    source_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    source_object_id = models.PositiveIntegerField()
    source = GenericForeignKey('source_content_type', 'source_object_id')

    # Optional: specific rows from this source
    row_filter = models.TextField(blank=True, help_text="Filter criteria applied to input rows")

    def __str__(self):
        return f"Input to {self.step.step_name}: {self.source}"


class TransformationStepOutput(models.Model):
    """Links a transformation step to its output tables/data"""
    step = models.ForeignKey('TransformationStep', related_name='outputs', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseTable and DerivedTable
    target_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    target_object_id = models.PositiveIntegerField()
    target = GenericForeignKey('target_content_type', 'target_object_id')

    def __str__(self):
        return f"Output from {self.step.step_name}: {self.target}"


class CalculationChain(models.Model):
    """
    Represents a complete calculation chain from source to output.
    Groups related transformation steps into a logical chain.
    """
    trail = models.ForeignKey('Trail', related_name='calculation_chains', on_delete=models.CASCADE)
    chain_name = models.CharField(max_length=255, help_text="Name of the calculation chain (e.g., Cell_F_01_01_REF_FINREP_3_0_45749_REF)")

    # Final output value
    final_value = models.FloatField(null=True, blank=True)
    final_string_value = models.TextField(null=True, blank=True)

    # Link to the output cell/metric
    output_cell_name = models.CharField(max_length=255, blank=True, help_text="Name of the output cell in the report")
    output_table = models.CharField(max_length=255, blank=True, help_text="Name of the output table/report")
    output_row_key = models.CharField(max_length=255, blank=True, help_text="Row identifier in the output")
    output_column = models.CharField(max_length=255, blank=True, help_text="Column identifier in the output")

    # Statistics
    total_steps = models.PositiveIntegerField(default=0)
    total_source_rows = models.PositiveIntegerField(default=0, help_text="Total rows from all source tables")
    total_contributing_rows = models.PositiveIntegerField(default=0, help_text="Rows that actually contributed to the output")

    # Timestamps
    started_at = models.DateTimeField(null=True, blank=True)
    completed_at = models.DateTimeField(null=True, blank=True)

    class Meta:
        indexes = [
            models.Index(fields=['trail', 'chain_name']),
            models.Index(fields=['output_table', 'output_cell_name']),
        ]

    def __str__(self):
        return f"Chain: {self.chain_name} = {self.final_value}"


class CalculationChainStep(models.Model):
    """Links a transformation step to a calculation chain"""
    chain = models.ForeignKey('CalculationChain', related_name='chain_steps', on_delete=models.CASCADE)
    step = models.ForeignKey('TransformationStep', on_delete=models.CASCADE)
    order_in_chain = models.PositiveIntegerField(help_text="Order of this step within the chain")

    class Meta:
        ordering = ['chain', 'order_in_chain']
        unique_together = ('chain', 'step')


class DataFlowEdge(models.Model):
    """
    Explicit representation of data flow between tables/functions.
    Used for generating Sankey diagrams and data flow visualizations.
    """
    FLOW_TYPES = [
        ('DATA', 'Data Flow'),
        ('FILTER', 'Filter Application'),
        ('JOIN', 'Join Relation'),
        ('AGGREGATE', 'Aggregation'),
        ('TRANSFORM', 'Transformation'),
    ]

    trail = models.ForeignKey('Trail', related_name='data_flow_edges', on_delete=models.CASCADE)

    # Source node
    source_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, related_name='flow_sources')
    source_object_id = models.PositiveIntegerField()
    source = GenericForeignKey('source_content_type', 'source_object_id')
    source_label = models.CharField(max_length=255, blank=True, help_text="Human-readable label for source")

    # Target node
    target_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE, related_name='flow_targets')
    target_object_id = models.PositiveIntegerField()
    target = GenericForeignKey('target_content_type', 'target_object_id')
    target_label = models.CharField(max_length=255, blank=True, help_text="Human-readable label for target")

    flow_type = models.CharField(max_length=20, choices=FLOW_TYPES, default='DATA')

    # Flow statistics (for Sankey diagram widths)
    row_count = models.PositiveIntegerField(default=0, help_text="Number of rows flowing through this edge")
    value_sum = models.FloatField(null=True, blank=True, help_text="Sum of values flowing through (for numeric data)")

    class Meta:
        indexes = [
            models.Index(fields=['trail', 'flow_type']),
        ]

    def __str__(self):
        return f"{self.source_label} -> {self.target_label} ({self.row_count} rows)"


class CellLineage(models.Model):
    """
    Output-centric lineage tracking for FINREP/COREP report cells.
    Provides a direct mapping from output cell to all contributing data.
    """
    trail = models.ForeignKey('Trail', related_name='cell_lineages', on_delete=models.CASCADE)

    # Output cell identification
    report_template = models.CharField(max_length=50, help_text="Report template (e.g., F_01.01)")
    framework = models.CharField(max_length=50, default='FINREP', help_text="Framework (FINREP, COREP, etc.)")
    cell_code = models.CharField(max_length=255, help_text="Cell code/identifier")
    row_key = models.CharField(max_length=255, blank=True)
    column_key = models.CharField(max_length=255, blank=True)

    # Output value
    computed_value = models.FloatField(null=True, blank=True)
    computed_string_value = models.TextField(null=True, blank=True)

    # Link to the calculation chain that produced this cell
    calculation_chain = models.ForeignKey('CalculationChain', on_delete=models.SET_NULL, null=True, blank=True)

    # Summary statistics
    source_table_count = models.PositiveIntegerField(default=0)
    source_row_count = models.PositiveIntegerField(default=0)
    transformation_count = models.PositiveIntegerField(default=0)

    class Meta:
        indexes = [
            models.Index(fields=['trail', 'report_template', 'cell_code']),
            models.Index(fields=['framework', 'report_template']),
        ]
        unique_together = ('trail', 'report_template', 'cell_code', 'row_key', 'column_key')

    def __str__(self):
        return f"{self.framework} {self.report_template} [{self.cell_code}] = {self.computed_value}"


class CellSourceRow(models.Model):
    """Links a cell to its contributing source rows"""
    cell = models.ForeignKey('CellLineage', related_name='source_rows', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseRow and DerivedTableRow
    row_content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    row_object_id = models.PositiveIntegerField()
    source_row = GenericForeignKey('row_content_type', 'row_object_id')

    # Contribution info
    contribution_type = models.CharField(max_length=50, blank=True, help_text="How this row contributes (e.g., SUM, COUNT)")
    contributed_value = models.FloatField(null=True, blank=True, help_text="Value contributed by this row")

    def __str__(self):
        return f"Source for {self.cell.cell_code}: {self.source_row}"


class LineageSummaryCache(models.Model):
    """
    Cached summary statistics for quick access in the UI.
    Regenerated when lineage data changes.
    """
    trail = models.OneToOneField('Trail', related_name='summary_cache', on_delete=models.CASCADE)

    # Counts
    total_database_tables = models.PositiveIntegerField(default=0)
    total_derived_tables = models.PositiveIntegerField(default=0)
    total_database_rows = models.PositiveIntegerField(default=0)
    total_derived_rows = models.PositiveIntegerField(default=0)
    total_transformation_steps = models.PositiveIntegerField(default=0)
    total_calculation_chains = models.PositiveIntegerField(default=0)
    total_output_cells = models.PositiveIntegerField(default=0)

    # Derived statistics
    avg_chain_length = models.FloatField(default=0)
    max_chain_length = models.PositiveIntegerField(default=0)
    total_data_flow_edges = models.PositiveIntegerField(default=0)

    # Cache metadata
    generated_at = models.DateTimeField(auto_now=True)
    is_stale = models.BooleanField(default=False)

    def __str__(self):
        return f"Summary for Trail {self.trail.name}"


