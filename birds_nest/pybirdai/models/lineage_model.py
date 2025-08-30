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
    """Track which columns a function references"""
    function = models.ForeignKey('Function', related_name='column_references', on_delete=models.CASCADE)
    # Generic relation to handle both DatabaseField and Function columns
    content_type = models.ForeignKey(ContentType, on_delete=models.CASCADE)
    object_id = models.PositiveIntegerField()
    referenced_column = GenericForeignKey('content_type', 'object_id')
    
    def __str__(self):
        return f"FunctionColumnReference: {self.function.name} -> {self.referenced_column}"

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


# Metadata Lineage Models for BIRD

class DataItem(models.Model):
    """Represents both columns and tables as data items for metadata lineage"""
    
    TYPE_CHOICES = [
        ('column', 'Column'),
        ('table', 'Table'),
    ]
    
    type = models.CharField(max_length=10, choices=TYPE_CHOICES)
    location = models.CharField(
        max_length=500,
        help_text="For columns: table_name.column_name format. For tables: table name only"
    )
    description = models.TextField(blank=True, null=True)
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        unique_together = ('type', 'location')
        indexes = [
            models.Index(fields=['type', 'location']),
        ]
    
    def __str__(self):
        return f"DataItem({self.type}): {self.location}"


class Process(models.Model):
    """Represents transformations or operations on data items"""
    
    TYPE_CHOICES = [
        ('column_to_column', 'Column to Column'),
        ('table_to_table', 'Table to Table'),
        ('columns_to_table', 'Columns to Table'),
    ]
    
    FUNCTION_TYPE_CHOICES = [
        ('basic', 'Basic Transformation'),
        ('join', 'Join Operation'),
        ('filter', 'Filter Operation'),
        ('aggregate_metric', 'Aggregate to Metric'),
        ('aggregate', 'General Aggregation'),
    ]
    
    name = models.CharField(max_length=500)
    type = models.CharField(max_length=20, choices=TYPE_CHOICES)
    function_type = models.CharField(max_length=20, choices=FUNCTION_TYPE_CHOICES)
    description = models.TextField(blank=True, null=True)
    source_reference = models.CharField(
        max_length=500, 
        blank=True, 
        null=True,
        help_text="Reference to source code or function that implements this process"
    )
    created_at = models.DateTimeField(auto_now_add=True)
    updated_at = models.DateTimeField(auto_now=True)
    
    class Meta:
        indexes = [
            models.Index(fields=['type']),
            models.Index(fields=['function_type']),
            models.Index(fields=['name']),
        ]
    
    def __str__(self):
        return f"Process: {self.name} ({self.type} - {self.function_type})"


class Relationship(models.Model):
    """Links processes with data items to track lineage relationships"""
    
    TYPE_CHOICES = [
        ('produces', 'Produces'),
        ('consumes', 'Consumes'),
    ]
    
    process = models.ForeignKey(
        'Process', 
        related_name='relationships', 
        on_delete=models.CASCADE
    )
    data_item = models.ForeignKey(
        'DataItem', 
        related_name='relationships', 
        on_delete=models.CASCADE
    )
    type = models.CharField(max_length=10, choices=TYPE_CHOICES)
    created_at = models.DateTimeField(auto_now_add=True)
    
    class Meta:
        unique_together = ('process', 'data_item', 'type')
        indexes = [
            models.Index(fields=['type']),
            models.Index(fields=['process', 'type']),
            models.Index(fields=['data_item', 'type']),
        ]
    
    def __str__(self):
        return f"Relationship: {self.process.name} {self.type} {self.data_item.location}"