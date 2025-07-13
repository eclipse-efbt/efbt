# Add COCALIMO (AORTA Model) Feature Specification

## Overview

This specification outlines the integration of the AORTA (Automated Orchestration and Result Tracking Architecture) model into the EFBT PyBIRD-AI application. The AORTA model provides detailed lineage tracking for calculations performed by the orchestrator, recording both the chain of classes/functions used and the resulting object graph.
Consider that there is an example of code produced that is ru by the orchestrator in the 
/home/neil/Desktop/efbt/birds_nest/pybirdai/process_steps/filter_code directory, code that is run by the orchestrator will always have a similar structure, this is because the code itslef is created by this project in the files /home/neil/Desktop/efbt/birds_nest/pybirdai/process_steps/pybird/create_executable_filters.py and /home/neil/Desktop/efbt/birds_nest/pybirdai/process_steps/pybird/create_python_django_transformations.py

Ultimately we want to visualise this results, and visData could be very good for that.
## Objectives

1. Convert the existing Java/Ecore AORTA model to Django models
2. Integrate AORTA lineage tracking into the orchestration system
3. Enable detailed tracking of calculation provenance
4. Provide APIs for querying and visualizing lineage information

## Background

The AORTA model is currently implemented in Java using EMF (Eclipse Modeling Framework) and located at `/home/neil/Desktop/efbt/birds_nest/pybirdai/aorta_model`. It provides a comprehensive framework for tracking data transformations and lineage at multiple levels:

- **Table Level**: Tracks derived tables and their source tables
- **Row Level**: Links derived rows to their source rows
- **Cell Level**: Traces computed values back to their inputs

## Technical Architecture

### 1. Django Model Design

#### Core Models

```python
# Base Abstract Models
class AortaTable(models.Model):
    """Base class for all table types"""
    class Meta:
        abstract = True

class AortaColumn(models.Model):
    """Base class for all column types"""
    class Meta:
        abstract = True

class AortaRow(models.Model):
    """Base class for all row types"""
    class Meta:
        abstract = True

class AortaActualValue(models.Model):
    """Base class for all value types"""
    class Meta:
        abstract = True
```

#### Concrete Models

```python
# Table Models
class DatabaseTable(AortaTable):
    name = models.CharField(max_length=255)
    fields = models.ManyToManyField('DatabaseField')

class DerivedTable(AortaTable):
    name = models.CharField(max_length=255)
    table_creation_function = models.ForeignKey('TableCreationFunction')
    functions = models.ManyToManyField('Function')

# Column Models
class DatabaseField(AortaColumn):
    name = models.CharField(max_length=255)
    table = models.ForeignKey('DatabaseTable', related_name='database_fields')

class Function(AortaColumn):
    name = models.CharField(max_length=255)
    function_text = models.OneToOneField('FunctionText')
    columns = models.ManyToManyField(AortaColumn, related_name='used_by_functions')
    table = models.ForeignKey('DerivedTable', related_name='derived_functions')

# Row Models
class DatabaseRow(AortaRow):
    populated_table = models.ForeignKey('PopulatedDataBaseTable')
    values = models.ManyToManyField('DatabaseColumnValue')

class DerivedTableRow(AortaRow):
    populated_table = models.ForeignKey('EvaluatedDerivedTable')
    evaluations = models.ManyToManyField('EvaluatedFunction')
    source_rows = models.ManyToManyField(AortaRow, related_name='derived_rows')

# Value Models
class DatabaseColumnValue(AortaActualValue):
    value = models.FloatField()
    column = models.ForeignKey('DatabaseField')
    row = models.ForeignKey('DatabaseRow', related_name='column_values')

class EvaluatedFunction(AortaActualValue):
    value = models.FloatField()
    function = models.ForeignKey('Function')
    row = models.ForeignKey('DerivedTableRow', related_name='evaluated_functions')
    source_values = models.ManyToManyField(AortaActualValue, related_name='used_in_evaluations')

# Lineage Container Models
class Trail(models.Model):
    """Top-level container for execution traces"""
    name = models.CharField(max_length=255)
    created_at = models.DateTimeField(auto_now_add=True)
    metadata_trail = models.OneToOneField('MetaDataTrail')

class MetaDataTrail(models.Model):
    """Contains table definitions (schema)"""
    tables = models.ManyToManyField(AortaTable)

class PopulatedTable(models.Model):
    """Base class linking table definitions to actual data"""
    class Meta:
        abstract = True
    trail = models.ForeignKey('Trail', related_name='populated_tables')

class PopulatedDataBaseTable(PopulatedTable):
    table = models.ForeignKey('DatabaseTable')
    rows = models.ManyToManyField('DatabaseRow')

class EvaluatedDerivedTable(PopulatedTable):
    table = models.ForeignKey('DerivedTable')
    rows = models.ManyToManyField('DerivedTableRow')

# Supporting Models
class FunctionText(models.Model):
    text = models.TextField()

class TableCreationFunction(models.Model):
    name = models.CharField(max_length=255)
    function_text = models.ForeignKey('FunctionText')
```

### 2. Integration with Orchestration System

#### Modification to orchestration.py

```python
class Orchestration:
    def __init__(self):
        self._initialized_objects = {}
        self.trail = None  # AORTA Trail instance
        self.current_metadata_trail = None
        
    def init_with_lineage(self, object_to_init):
        """Initialize object with AORTA lineage tracking"""
        # Create trail if not exists
        if not self.trail:
            self.trail = Trail.objects.create(name=f"Execution_{datetime.now()}")
            self.current_metadata_trail = MetaDataTrail.objects.create()
            self.trail.metadata_trail = self.current_metadata_trail
            self.trail.save()
            
        # Track object initialization
        self._track_initialization(object_to_init)
        
        # Perform standard initialization
        return self.init(object_to_init)
    
    def _track_initialization(self, obj):
        """Track object in AORTA metadata trail"""
        # Implementation details for tracking different object types
        pass
```

#### Lineage Recording Decorators

```python
def track_lineage(func):
    """Decorator to track function execution lineage"""
    def wrapper(self, *args, **kwargs):
        # Record function invocation
        if hasattr(self, '_orchestration') and self._orchestration.trail:
            # Create Function record
            function_record = Function.objects.create(
                name=func.__name__,
                function_text=FunctionText.objects.create(text=inspect.getsource(func))
            )
            # Link to current execution context
            # ...
        
        # Execute original function
        result = func(self, *args, **kwargs)
        
        # Record result lineage
        if hasattr(self, '_orchestration') and self._orchestration.trail:
            # Create EvaluatedFunction record
            # Link source values
            # ...
            
        return result
    return wrapper
```

### 3. Implementation Phases

#### Phase 1: Model Creation (Week 1)
- Create Django models matching AORTA structure
- Write migrations
- Add model tests

#### Phase 2: Basic Integration (Week 2)
- Integrate Trail creation into orchestration.py
- Add lineage tracking to key transformation functions
- Implement basic lineage recording

#### Phase 3: Full Integration (Week 3-4)
- Add comprehensive lineage tracking decorators
- Integrate with all transformation steps
- Implement lineage querying APIs

#### Phase 4: Visualization and Tools (Week 5-6)
- Create lineage visualization views
- Implement lineage query tools
- Add performance optimizations

### 4. API Design

#### Lineage Query API

```python
# urls.py
urlpatterns = [
    path('api/lineage/trail/<int:trail_id>/', TrailDetailView.as_view()),
    path('api/lineage/value/<int:value_id>/ancestors/', ValueLineageView.as_view()),
    path('api/lineage/table/<int:table_id>/dependencies/', TableDependencyView.as_view()),
]

# views.py
class ValueLineageView(APIView):
    """Get complete lineage tree for a computed value"""
    def get(self, request, value_id):
        value = AortaActualValue.objects.get(id=value_id)
        lineage_tree = self._build_lineage_tree(value)
        return Response(lineage_tree)
```

### 5. Performance Considerations

1. **Lazy Loading**: Use Django's select_related/prefetch_related for efficient queries
2. **Bulk Operations**: Use bulk_create for recording multiple values
3. **Indexing**: Add database indexes on foreign keys and frequently queried fields
4. **Archival**: Implement trail archival strategy for old executions

### 6. Testing Strategy

1. **Unit Tests**: Test each AORTA model independently
2. **Integration Tests**: Test lineage recording during orchestration
3. **Performance Tests**: Ensure lineage tracking doesn't significantly impact execution time
4. **End-to-End Tests**: Verify complete lineage chains from input to output

### 7. Migration from Java/Ecore

1. **Data Migration Script**: If existing AORTA data needs to be migrated
2. **Validation Tools**: Compare Django model structure with Ecore model
3. **Compatibility Layer**: Optional adapter for systems expecting Java objects

### 8. Future Enhancements

1. **Lineage Visualization**: Interactive graph visualization of lineage
2. **Impact Analysis**: Determine what outputs are affected by input changes
3. **Debugging Tools**: Step through calculations using lineage data
4. **Compliance Reports**: Generate audit trails for regulatory purposes

## Success Criteria

1. All AORTA model entities successfully converted to Django models
2. Orchestration system records complete lineage for all transformations
3. Lineage queries perform within acceptable time limits (<1s for typical queries)
4. No significant impact on orchestration performance (<10% overhead)
5. Complete test coverage for lineage tracking functionality

## Dependencies

- Django 
- Optional: Graphviz or D3.js for visualization

## Risks and Mitigations

1. **Performance Impact**: Mitigate with bulk operations and async recording
2. **Storage Requirements**: Implement data retention policies and archival
3. **Complexity**: Phase implementation and provide clear documentation
4. **Model Synchronization**: Automated tests to ensure Java/Python model alignment