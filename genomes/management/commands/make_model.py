from django.core.management.base import BaseCommand
import os
import re

# TODO: move to separate app and perform tests

class Command(BaseCommand):
    help = "Create a new model with boilerplate code"

    def add_arguments(self, parser):
        parser.add_argument("name", type=str, help="Name of the model")
        parser.add_argument(
            "app", type=str, help="App where the model should be created"
        )
        parser.add_argument(
            "--base", type=str, default="BaseModel", help="Base model to inherit from"
        )
        parser.add_argument("--fields", nargs="+", help="Fields in format name:type")

    def handle(self, *args, **options):
        name = options["name"]
        app = options["app"]
        base_model = options["base"]
        fields = options["fields"] or []

        # Convert name to PascalCase for class name
        class_name = "".join(word.capitalize() for word in re.split(r"[_\-\s]", name))

        # Create model file
        model_dir = f"{app}/models"
        if not os.path.exists(model_dir):
            os.makedirs(model_dir)

        # Create __init__.py if it doesn't exist
        init_file = f"{model_dir}/__init__.py"
        if not os.path.exists(init_file):
            with open(init_file, "w") as f:
                f.write("")

        # Create model file
        file_path = f"{model_dir}/{name.lower()}.py"

        # Check if file already exists
        if os.path.exists(file_path):
            self.stdout.write(
                self.style.ERROR(f"Model file {file_path} already exists")
            )
            return

        # Generate field definitions
        field_definitions = []
        for field in fields:
            if ":" in field:
                field_name, field_type = field.split(":", 1)
                field_definitions.append(f"    {field_name} = models.{field_type}")
            else:
                field_definitions.append(
                    f"    {field} = models.CharField(max_length=255)"
                )

        field_str = (
            "\n".join(field_definitions)
            if field_definitions
            else "    # Add your fields here"
        )

        # Create model content
        model_content = f'''from django.db import models
from {app}.models.base_model import {base_model}

class {class_name}({base_model}):
    """
    {class_name} model.
    """
{field_str}

    class Meta:
        db_table = "{name.lower()}"

    def __str__(self):
        return f"{class_name} #{self.id}"
'''

        # Write to file
        with open(file_path, "w") as f:
            f.write(model_content)

        # Update __init__.py to import the model
        with open(init_file, "a") as f:
            f.write(f"from {app}.models.{name.lower()} import {class_name}\n")

        self.stdout.write(
            self.style.SUCCESS(
                f"Successfully created model {class_name} in {file_path}"
            )
        )
