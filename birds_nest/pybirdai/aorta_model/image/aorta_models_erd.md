# AORTA Models Entity Relationship Diagram

## PyBIRD AI - AORTA Lineage Tracking Models

This diagram shows the complete entity relationship structure for the AORTA (Advanced Object-Relational Tracking Architecture) lineage tracking system in PyBIRD AI.

```mermaid
erDiagram
    %% Core Trail Structure
    Trail {
        int id PK
        string name
        datetime created_at
        json execution_context
        int metadata_trail_id FK
    }
    
    MetaDataTrail {
        int id PK
    }
    
    %% Abstract Base Classes (shown for documentation)
    AortaTable {
        abstract
    }
    
    AortaColumn {
        abstract
    }
    
    AortaRow {
        abstract
    }
    
    AortaActualValue {
        abstract
    }
    
    %% Table Models
    DatabaseTable {
        int id PK
        string name
    }
    
    DerivedTable {
        int id PK
        string name
        int table_creation_function_id FK
    }
    
    %% Column Models
    DatabaseField {
        int id PK
        string name
        int table_id FK
    }
    
    Function {
        int id PK
        string name
        int function_text_id FK
        int table_id FK
    }
    
    %% Row Models
    DatabaseRow {
        int id PK
        string row_identifier
        int populated_table_id FK
    }
    
    DerivedTableRow {
        int id PK
        string row_identifier
        int populated_table_id FK
    }
    
    %% Value Models
    DatabaseColumnValue {
        int id PK
        float value
        text string_value
        int column_id FK
        int row_id FK
    }
    
    EvaluatedFunction {
        int id PK
        float value
        text string_value
        int function_id FK
        int row_id FK
    }
    
    %% Populated Table Models
    PopulatedDataBaseTable {
        int id PK
        int trail_id FK
        int table_id FK
    }
    
    EvaluatedDerivedTable {
        int id PK
        int trail_id FK
        int table_id FK
    }
    
    %% Supporting Models
    FunctionText {
        int id PK
        text text
        string language
    }
    
    TableCreationFunction {
        int id PK
        string name
        int function_text_id FK
    }
    
    AortaTableReference {
        int id PK
        int metadata_trail_id FK
        string table_content_type
        int table_id
    }
    
    %% Relationship Tracking Models
    FunctionColumnReference {
        int id PK
        int function_id FK
        int content_type_id FK
        int object_id
    }
    
    DerivedRowSourceReference {
        int id PK
        int derived_row_id FK
        int content_type_id FK
        int object_id
    }
    
    EvaluatedFunctionSourceValue {
        int id PK
        int evaluated_function_id FK
        int content_type_id FK
        int object_id
    }
    
    TableCreationSourceTable {
        int id PK
        int table_creation_function_id FK
        int content_type_id FK
        int object_id
    }
    
    TableCreationFunctionColumn {
        int id PK
        int table_creation_function_id FK
        int content_type_id FK
        int object_id
        text reference_text
    }
    
    ContentType {
        int id PK
        string model
        string app_label
    }
    
    %% Core Relationships
    Trail ||--|| MetaDataTrail : "has metadata"
    Trail ||--o{ PopulatedDataBaseTable : "contains"
    Trail ||--o{ EvaluatedDerivedTable : "contains"
    
    %% Table Relationships
    DatabaseTable ||--o{ DatabaseField : "has fields"
    DerivedTable ||--o{ Function : "has functions"
    TableCreationFunction ||--|| DerivedTable : "creates"
    
    %% Populated Table Relationships
    PopulatedDataBaseTable }o--|| DatabaseTable : "instances of"
    EvaluatedDerivedTable }o--|| DerivedTable : "instances of"
    PopulatedDataBaseTable ||--o{ DatabaseRow : "contains rows"
    EvaluatedDerivedTable ||--o{ DerivedTableRow : "contains rows"
    
    %% Row-Value Relationships
    DatabaseRow ||--o{ DatabaseColumnValue : "contains values"
    DerivedTableRow ||--o{ EvaluatedFunction : "contains computations"
    
    %% Column-Value Relationships
    DatabaseField ||--o{ DatabaseColumnValue : "typed by"
    Function ||--o{ EvaluatedFunction : "computed by"
    
    %% Function Support
    Function ||--|| FunctionText : "implemented by"
    TableCreationFunction ||--|| FunctionText : "implemented by"
    
    %% Metadata Tracking
    MetaDataTrail ||--o{ AortaTableReference : "references tables"
    
    %% Lineage Relationships (Generic Foreign Keys)
    Function ||--o{ FunctionColumnReference : "references columns"
    DerivedTableRow ||--o{ DerivedRowSourceReference : "derived from rows"
    EvaluatedFunction ||--o{ EvaluatedFunctionSourceValue : "computed from values"
    TableCreationFunction ||--o{ TableCreationSourceTable : "uses source tables"
    TableCreationFunction ||--o{ TableCreationFunctionColumn : "references columns"
    
    %% Generic FK Support
    FunctionColumnReference }o--|| ContentType : "content type"
    DerivedRowSourceReference }o--|| ContentType : "content type"
    EvaluatedFunctionSourceValue }o--|| ContentType : "content type"
    TableCreationSourceTable }o--|| ContentType : "content type"
    TableCreationFunctionColumn }o--|| ContentType : "content type"
```

## Model Categories

### 🗂️ **Core Trail Structure**
- **Trail**: Top-level execution container with timestamp and context
- **MetaDataTrail**: Schema/metadata container for table definitions

### 📊 **Table Models** (inherit from AortaTable)
- **DatabaseTable**: Source data tables with physical fields
- **DerivedTable**: Computed tables created by functions

### 📋 **Column Models** (inherit from AortaColumn)
- **DatabaseField**: Physical database columns in source tables
- **Function**: Computed columns that reference other columns

### 📄 **Row Models** (inherit from AortaRow)
- **DatabaseRow**: Rows containing actual data values
- **DerivedTableRow**: Rows containing computed function results

### 💾 **Value Models** (inherit from AortaActualValue)
- **DatabaseColumnValue**: Actual data values with numeric/string storage
- **EvaluatedFunction**: Computed values with full lineage tracking

### 🔗 **Populated Table Models**
- **PopulatedDataBaseTable**: Links DatabaseTable to actual data rows in a Trail
- **EvaluatedDerivedTable**: Links DerivedTable to computed rows in a Trail

### 🛠️ **Supporting Models**
- **FunctionText**: Stores actual function code/implementations
- **TableCreationFunction**: Functions that generate entire tables
- **AortaTableReference**: Tracks table references in metadata

### 🎯 **Lineage Tracking Models** (use Generic Foreign Keys)
- **FunctionColumnReference**: Tracks column dependencies for functions
- **DerivedRowSourceReference**: Tracks source rows for derived rows
- **EvaluatedFunctionSourceValue**: Tracks source values for computations
- **TableCreationSourceTable**: Tracks source tables for table creation
- **TableCreationFunctionColumn**: Tracks column references in table creation

## Key Features

### ✅ **Complete Lineage Tracking**
- **Table Level**: Which tables are used to create other tables
- **Row Level**: Which source rows contribute to derived rows
- **Column Level**: Which columns/functions depend on other columns
- **Value Level**: Which source values contribute to computed values

### 🔄 **Flexible References**
- Generic Foreign Keys allow referencing different model types
- Functions can reference both DatabaseFields and other Functions
- Derived rows can come from both DatabaseRows and other DerivedTableRows
- Computed values can depend on both DatabaseColumnValues and other EvaluatedFunctions

### 📈 **Hierarchical Structure**
- Trail → PopulatedTables → Rows → Values
- MetaDataTrail → Tables → Columns → Functions
- Full support for multi-level derived computations

### 🎨 **Extensible Architecture**
- Abstract base classes provide consistent interfaces
- Generic relationships support future model types
- JSON execution context allows arbitrary metadata storage