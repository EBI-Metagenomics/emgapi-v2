from django.core.management.base import BaseCommand
import os
import re

class Command(BaseCommand):
    help = 'Create a new API endpoint for a model'

    def add_arguments(self, parser):
        parser.add_argument('model', type=str, help='Name of the model')
        parser.add_argument('app', type=str, help='App where the model is located')
        parser.add_argument('--fields', nargs='+', help='Fields to include in the API')

    def handle(self, *args, **options):
        model_name = options['model']
        app = options['app']
        fields = options['fields'] or []
        
        # Convert model_name to PascalCase for class name
        model_class_name = ''.join(word.capitalize() for word in re.split(r'[_\-\s]', model_name))
        
        # Create api directory if it doesn't exist
        api_dir = f"emgapiv2/api"
        if not os.path.exists(api_dir):
            os.makedirs(api_dir)
            
        # Create __init__.py if it doesn't exist
        init_file = f"{api_dir}/__init__.py"
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write('')
        
        # Create api file
        file_path = f"{api_dir}/{model_name.lower()}s.py"
        
        # Check if file already exists
        if os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'API file {file_path} already exists'))
            return
        
        # Format fields
        fields_str = ', '.join([f'"{field}"' for field in fields]) if fields else '"id", "created_at", "updated_at"'
        
        # Create api content
        api_content = f'''from ninja import Router, Schema
from ninja.pagination import paginate
from typing import List, Optional

from {app}.models.{model_name.lower()} import {model_class_name}


class {model_class_name}Schema(Schema):
    id: int
    # Add your fields here
    created_at: str
    updated_at: str


class {model_class_name}FilterSchema(Schema):
    # Add your filter fields here
    pass


router = Router()


@router.get("/", response=List[{model_class_name}Schema])
@paginate
def list_{model_name.lower()}s(request, filters: {model_class_name}FilterSchema = None):
    """
    List all {model_name.lower()}s
    """
    qs = {model_class_name}.objects.all()
    # Add your filters here
    return qs


@router.get("/{{id}}", response={model_class_name}Schema)
def get_{model_name.lower()}(request, id: int):
    """
    Get a specific {model_name.lower()} by ID
    """
    return {model_class_name}.objects.get(id=id)
'''
        
        # Write to file
        with open(file_path, 'w') as f:
            f.write(api_content)
        
        # Update __init__.py to import the router
        with open(init_file, 'a') as f:
            f.write(f"from emgapiv2.api.{model_name.lower()}s import router as {model_name.lower()}_router\n")
        
        self.stdout.write(self.style.SUCCESS(f'Successfully created API endpoint for {model_class_name} in {file_path}'))