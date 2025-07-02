from django.core.management.base import BaseCommand
import os
import importlib
import re

class Command(BaseCommand):
    help = 'Create a new admin class for a model'

    def add_arguments(self, parser):
        parser.add_argument('model', type=str, help='Name of the model')
        parser.add_argument('app', type=str, help='App where the model is located')
        parser.add_argument('--list-display', nargs='+', help='Fields to display in list view')
        parser.add_argument('--search-fields', nargs='+', help='Fields to search')
        parser.add_argument('--readonly-fields', nargs='+', help='Read-only fields')

    def handle(self, *args, **options):
        model_name = options['model']
        app = options['app']
        list_display = options['list_display'] or ['id']
        search_fields = options['search_fields'] or []
        readonly_fields = options['readonly_fields'] or ['created_at', 'updated_at']
        
        # Convert model_name to PascalCase for class name
        model_class_name = ''.join(word.capitalize() for word in re.split(r'[_\-\s]', model_name))
        admin_class_name = f"{model_class_name}Admin"
        
        # Create admin directory if it doesn't exist
        admin_dir = f"{app}/admin"
        if not os.path.exists(admin_dir):
            os.makedirs(admin_dir)
            
        # Create __init__.py if it doesn't exist
        init_file = f"{admin_dir}/__init__.py"
        if not os.path.exists(init_file):
            with open(init_file, 'w') as f:
                f.write('')
        
        # Create admin file
        file_path = f"{admin_dir}/{model_name.lower()}.py"
        
        # Check if file already exists
        if os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'Admin file {file_path} already exists'))
            return
        
        # Format list display, search fields, and readonly fields
        list_display_str = ', '.join([f'"{field}"' for field in list_display])
        search_fields_str = ', '.join([f'"{field}"' for field in search_fields])
        readonly_fields_str = ', '.join([f'"{field}"' for field in readonly_fields])
        
        # Create admin content
        admin_content = f'''from django.contrib import admin

from {app}.models.{model_name.lower()} import {model_class_name}


class {admin_class_name}(admin.ModelAdmin):
    list_display = [{list_display_str}]
    search_fields = [{search_fields_str}]
    readonly_fields = [{readonly_fields_str}]

    def get_queryset(self, request):
        return super().get_queryset(request)

admin.site.register({model_class_name}, {admin_class_name})
'''
        
        # Write to file
        with open(file_path, 'w') as f:
            f.write(admin_content)
        
        # Update __init__.py to import the admin
        with open(init_file, 'a') as f:
            f.write(f"from {app}.admin.{model_name.lower()} import {admin_class_name}\n")
        
        self.stdout.write(self.style.SUCCESS(f'Successfully created admin class {admin_class_name} in {file_path}'))