from prefect import flow, task
import subprocess

@task
def run_import_biomes(url: str):
    """
    Task to invoke the Django management command to import biomes from API v1.
    """
    command = [
        "python", "manage.py", "import_biomes_from_api_v1",
        "--url", url
    ]
    subprocess.run(command, check=True)

@flow(name="Import Biomes Flow")
def import_biomes_flow(api_url: str):
    """
    Prefect flow to import biomes from API v1.
    """
    run_import_biomes(api_url)

# Example usage
if __name__ == "__main__":
    import_biomes_flow("https://api.example.com/biomes")