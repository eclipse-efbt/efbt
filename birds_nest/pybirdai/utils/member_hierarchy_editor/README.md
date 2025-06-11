# Member Hierarchy Editor Integration

This module integrates a visual hierarchy editor with the Django BIRD application, allowing users to create and edit member hierarchies through an interactive canvas interface.

## Overview

The integration consists of several key components:

- **Visual Editor**: HTML5 canvas-based interface for creating/editing hierarchies
- **Django Integration**: Utilities to convert between visualization format and Django models
- **API Endpoints**: REST-like endpoints for saving/loading hierarchy data
- **Model Conversion**: Automatic conversion between visualization boxes/arrows and MEMBER_HIERARCHY_NODE instances

## Files Structure

```
member_hierarchy_editor/
├── README.md                           # This documentation
├── django_hierarchy_integration.py     # Main integration class
├── django_model_converter.py          # Conversion utilities
├── from_member_hierarchy_node_to_visualisation.py  # Legacy pandas-based converter
└── from_visualisation_to_member_hierarchy_node.py  # Legacy converter (reverse)
```

## Key Components

### DjangoMemberHierarchyIntegration

Main integration class that provides:
- `get_hierarchy_by_id()`: Convert Django models to visualization format
- `save_hierarchy_from_visualization()`: Save visual changes back to database
- `get_available_hierarchies()`: List all hierarchies
- `get_domain_members()`: Get members for a specific domain

### DjangoModelConverter

Handles the complex conversion logic:
- Calculates hierarchy levels automatically
- Determines node properties (comparator/operator) based on position
- Validates hierarchy structure for logical consistency
- Prevents circular references and other structural issues

### Visual Editor Features

- **Drag & Drop**: Add members from sidebar to canvas
- **Arrow Tool**: Connect concepts to create parent-child relationships
- **Delete Arrow Tool**: Remove connections between concepts
- **Zoom Controls**: Scale the workspace for better navigation
- **Export/Import**: Save/load hierarchy data as JSON
- **Real-time Validation**: Immediate feedback on structural issues

## API Endpoints

| Endpoint | Method | Description |
|----------|--------|-------------|
| `/api/hierarchy/{id}/json/` | GET | Get hierarchy data for visualization |
| `/api/hierarchy/save/` | POST | Save visualization data to database |
| `/api/domain/{id}/members/` | GET | Get available members for domain |
| `/api/hierarchies/` | GET | List all available hierarchies |
| `/api/hierarchy/create/` | POST | Create new hierarchy from visualization |

## Usage

### Basic Usage

1. Navigate to `/pybirdai/member_hierarchy_editor/`
2. Select an existing hierarchy from dropdown, or create new one
3. Drag members from sidebar to canvas
4. Use arrow tool to connect concepts (parent ← child)
5. Changes are automatically validated
6. Use export/import for backup/restore

### Programmatic Usage

```python
from pybirdai.utils.member_hierarchy_editor.django_hierarchy_integration import get_hierarchy_integration

# Get integration instance
integration = get_hierarchy_integration()

# Load hierarchy data
hierarchy_data = integration.get_hierarchy_by_id("MY_HIERARCHY_ID")

# Save changes back to database
success = integration.save_hierarchy_from_visualization("MY_HIERARCHY_ID", modified_data)
```

## Data Formats

### Visualization Format

```json
{
  "boxes": [
    {
      "id": "MEMBER_001",
      "x": 100,
      "y": 200,
      "width": 300,
      "height": 120,
      "name": "Member Name",
      "text": "Description"
    }
  ],
  "arrows": [
    {
      "from": "CHILD_MEMBER",
      "to": "PARENT_MEMBER"
    }
  ],
  "nextId": 2,
  "hierarchy_info": {
    "id": "HIERARCHY_ID",
    "name": "Hierarchy Name",
    "description": "Description",
    "domain": "DOMAIN_ID",
    "allowed_members": {
      "MEMBER_001": "Member Display Name"
    }
  }
}
```

### Django Model Mapping

- **boxes** → MEMBER_HIERARCHY_NODE instances
- **arrows** → parent_member_id relationships
- **levels** → automatically calculated based on hierarchy depth
- **comparator/operator** → determined by node position in hierarchy

## Hierarchy Rules

The system enforces standard BIRD hierarchy rules:

1. **Root Nodes** (no parent): `comparator="="`, `operator=""`
2. **Intermediate Nodes** (has parent & children): `comparator="="`, `operator="+"`
3. **Leaf Nodes** (has parent, no children): `comparator=""`, `operator="+"`

## Validation

The system performs comprehensive validation:

- **Structural**: No circular references, valid parent-child relationships
- **Business Rules**: Correct comparator/operator assignment
- **Data Integrity**: All referenced members exist in domain
- **Uniqueness**: No duplicate parent assignments

## Error Handling

- Invalid structures are rejected with detailed error messages
- Missing members are skipped during import
- Circular references are detected and prevented
- User-friendly feedback for all operations

## Browser Compatibility

- Modern browsers with HTML5 Canvas support
- Keyboard shortcuts for power users
- Touch-friendly for tablet use
- Responsive design for different screen sizes

## Future Enhancements

- Undo/Redo functionality
- Collaborative editing
- Advanced layout algorithms
- Batch operations
- Integration with other BIRD modules