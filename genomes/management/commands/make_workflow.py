from django.core.management.base import BaseCommand
import os
import re

class Command(BaseCommand):
    help = 'Create a new Prefect workflow'

    def add_arguments(self, parser):
        parser.add_argument('name', type=str, help='Name of the workflow')
        parser.add_argument('--description', type=str, help='Description of the workflow')

    def handle(self, *args, **options):
        name = options['name']
        description = options['description'] or f"Prefect flow for {name}"
        
        # Convert name to snake_case for file name
        file_name = '_'.join(word.lower() for word in re.split(r'[_\-\s]', name))
        
        # Create workflows directory if it doesn't exist
        workflow_dir = "workflows/flows"
        if not os.path.exists(workflow_dir):
            os.makedirs(workflow_dir)
        
        # Create workflow file
        file_path = f"{workflow_dir}/{file_name}.py"
        
        # Check if file already exists
        if os.path.exists(file_path):
            self.stdout.write(self.style.ERROR(f'Workflow file {file_path} already exists'))
            return
        
        # Create workflow content
        workflow_content = f'''"""
{description}
"""
from pathlib import Path
from typing import Dict, List, Optional

from prefect import flow, task, get_run_logger
from prefect.task_runners import SequentialTaskRunner

from activate_django_first import EMG_CONFIG


@task
def prepare_data(input_data: str) -> Dict:
    """
    Prepare data for processing
    """
    logger = get_run_logger()
    logger.info(f"Preparing data from: {{input_data}}")
    
    # Add your data preparation logic here
    
    return {{"prepared_data": "example"}}


@task
def process_data(prepared_data: Dict) -> Dict:
    """
    Process the prepared data
    """
    logger = get_run_logger()
    logger.info(f"Processing data: {{prepared_data}}")
    
    # Add your data processing logic here
    
    return {{"processed_data": "example"}}


@flow(name="{name}", task_runner=SequentialTaskRunner())
def {file_name}(input_data: str) -> Dict:
    """
    Main flow for {name}
    """
    logger = get_run_logger()
    logger.info(f"Starting {name} flow with input: {{input_data}}")
    
    # Prepare data
    prepared_data = prepare_data(input_data)
    
    # Process data
    result = process_data(prepared_data)
    
    logger.info(f"Completed {name} flow with result: {{result}}")
    return result


if __name__ == "__main__":
    # This allows the flow to be run directly for testing
    import sys
    if len(sys.argv) != 2:
        print(f"Usage: python {file_name}.py <input_data>")
        sys.exit(1)
    
    input_data = sys.argv[1]
    result = {file_name}(input_data)
    print(f"Flow completed: {{result}}")
'''
        
        # Write to file
        with open(file_path, 'w') as f:
            f.write(workflow_content)
        
        self.stdout.write(self.style.SUCCESS(f'Successfully created workflow {name} in {file_path}'))
        self.stdout.write(self.style.SUCCESS(f'To deploy this flow, run: FLOW={file_name} task deploy-flow'))