# Orchestration Class

The `Orchestration` class manages the execution of datapoints by creating instances of classes, running the correct functions, and getting the correct data from the model.

## Initialization Tracking

The Orchestration class now includes functionality to track which objects have been initialized, ensuring that the `init` method is called only once per object. This prevents redundant initialization and improves performance.

## Key Features

### Single Initialization

Objects are now initialized only once. If the `init` method is called on an object that has already been initialized, the initialization will be skipped.

```python
# First initialization - will proceed
Orchestration().init(my_object)

# Second initialization - will be skipped
Orchestration().init(my_object)  # This will print a message and return without re-initializing
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

## Testing

A test script is provided in `test_orchestration.py` to verify the initialization tracking functionality.

To run the tests:

```
python -m pybirdai.process_steps.pybird.test_orchestration
``` 