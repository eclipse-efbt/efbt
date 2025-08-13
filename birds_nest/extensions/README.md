# Extensions Directory

This directory contains all BIRD Bench extensions. Each extension follows a standardized structure for consistent integration with the core PyBIRD AI platform.

## Structure

Extensions are automatically discovered when placed in this directory. Each extension must follow this structure:

```
extension_name/
├── __init__.py                    # Package initialization
├── views.py                       # HTTP endpoints
├── urls.py                        # URL routing
├── templates/                     # HTML templates
├── static/                        # CSS, JS, images
├── process_steps/                 # Business logic
├── entry_point.py                 # Extension registration
├── extension_configuration.py     # License verification
├── extension_models.py            # Django models
└── extension_manifest.yaml        # Extension metadata
```

## Development Workflow

1. Clone the main repository
2. Create your extension in this directory
3. Test locally with `python manage.py runserver`
4. Package with `bird-ext package`
5. Publish to private repository

## Note

All extensions in this directory are ignored by git by default. This allows developers to work on proprietary extensions without accidentally committing them to the public repository.