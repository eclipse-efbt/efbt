# Orchestration Class

The `Orchestration` class manages the execution of datapoints by creating instances of classes, running the correct functions, and getting the correct data from the model.

## Initialization Tracking

The Orchestration class now includes functionality to track which objects have been initialized, ensuring that the `init` method is called only once per object. This prevents redundant initialization and improves performance.

## Key Features

### Single Initialization with Reference Handling

Objects are now initialized only once. If the `init` method is called on an object that has already been initialized, the initialization will be skipped, but references will still be properly set to ensure the object can be used correctly.

```python
# First initialization - will proceed with full initialization
Orchestration().init(my_object)

# Second initialization - will skip full initialization but ensure references are set
Orchestration().init(my_object)  # This will print a message and only set necessary references
```

### Checking Initialization Status

You can check if an object has been initialized:

```python
if Orchestration.is_initialized(my_object):
    print("Object is already initialized")
else:
    print("Object needs initialization")
```

### Resetting Initialization Tracking

In some cases, you may want to reset the initialization tracking, for example during testing or when you explicitly want to re-initialize objects:

```python
# Reset all initialization tracking
Orchestration.reset_initialization()
```

## Implementation Details

The initialization tracking is implemented using a class-level set that stores the IDs of initialized objects. This approach ensures that:

1. Tracking persists across multiple instances of the Orchestration class
2. Objects are uniquely identified by their memory address (via `id()`)
3. The tracking has minimal memory overhead

### Reference Handling

When an object is already initialized and the `init` method is called again, the system:
1. Skips the full initialization process
2. Still ensures that all table references are properly set
3. Only sets references that are currently `None`

This ensures that objects can be safely reused without losing their references, preventing `NoneType` errors when accessing attributes.

## Testing

A test script is provided in `test_orchestration.py` to verify the initialization tracking functionality.

To run the tests:

```
python -m pybirdai.process_steps.pybird.test_orchestration
``` 