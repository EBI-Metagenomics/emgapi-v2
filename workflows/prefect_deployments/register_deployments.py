import yaml
import sys
import os
import ast
import argparse

from typing import Optional, Dict


def get_flow_info(file_path: str, flow_name_to_find: str) -> Optional[Dict[str, str]]:
    """
    Parses a Python file to find information about a specific function decorated as a flow.

    :param file_path: Path to the Python file to analyze.
    :param flow_name_to_find: Name of the flow function to search for.
    :return: A dictionary with flow name, entrypoint and description, or None if not found.
    """
    with open(file_path, "r") as f:
        tree = ast.parse(f.read())

    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef) and node.name == flow_name_to_find:
            is_flow = False
            for decorator in node.decorator_list:
                # Check for @flow or @flow(...)
                if (isinstance(decorator, ast.Name) and decorator.id == "flow") or (
                    isinstance(decorator, ast.Call)
                    and isinstance(decorator.func, ast.Name)
                    and decorator.func.id == "flow"
                ):
                    is_flow = True
                    break

            if is_flow:
                rel_path = os.path.relpath(file_path, os.getcwd())
                description = ast.get_docstring(node)
                return {
                    "name": node.name,  # Use function name for deployment name suffix usually
                    "entrypoint": f"{rel_path}:{node.name}",
                    "description": description,
                }
    return None


def add_to_prefect_yaml(
    yaml_path: str, flow: Dict[str, str], work_pool_name: str = "slurm"
) -> None:
    """
    Adds or updates a flow deployment in a prefect.yaml file.

    :param yaml_path: Path to the prefect.yaml file.
    :param flow: Dictionary containing flow information (name, entrypoint and description).
    :param work_pool_name: Name of the work pool for the deployment.
    """
    if not os.path.exists(yaml_path):
        print(f"Error: {yaml_path} not found.")
        return

    with open(yaml_path, "r") as f:
        config = yaml.safe_load(f) or {}

    if "deployments" not in config or config["deployments"] is None:
        config["deployments"] = []

    deployment_name = f"{flow['name']}_deployment"

    # Check if deployment already exists
    exists = False
    for i, dep in enumerate(config["deployments"]):
        if (
            dep.get("name") == deployment_name
            and dep.get("entrypoint") == flow["entrypoint"]
        ):
            exists = True
            # Update description if it changed
            if dep.get("description") != flow["description"]:
                dep["description"] = flow["description"]
                print(f"Updated description for {deployment_name} in {yaml_path}")
            else:
                print(
                    f"Deployment {deployment_name} already exists for {flow['entrypoint']}. Skipping."
                )
            break

    if not exists:
        new_deployment = {
            "name": deployment_name,
            "description": flow["description"],
            "entrypoint": flow["entrypoint"],
            "work_pool": {"name": work_pool_name},
        }
        config["deployments"].append(new_deployment)
        print(f"Added {deployment_name} to {yaml_path}")

    # Custom Dumper to use literal block scalar for multiline strings
    class LiteralDumper(yaml.SafeDumper):
        def represent_scalar(self, tag, value, style=None):
            if isinstance(value, str) and "\n" in value:
                style = "|"
            return super().represent_scalar(tag, value, style)

    with open(yaml_path, "w") as f:
        yaml.dump(config, f, Dumper=LiteralDumper, sort_keys=False)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Register a Prefect flow deployment in prefect.yaml"
    )
    parser.add_argument(
        "-file",
        "--file-path",
        dest="file_path",
        required=True,
        help="Path to the Python file containing the flow",
    )
    parser.add_argument(
        "-yaml",
        "--yaml-path",
        dest="yaml_path",
        required=True,
        help="Path to the prefect.yaml file",
    )
    parser.add_argument(
        "-flow",
        "--flow-name",
        dest="flow_name",
        required=True,
        help="Name of the flow function to register",
    )
    parser.add_argument(
        "--pool", default="slurm", help="Name of the work pool (default: slurm)"
    )

    args = parser.parse_args()

    flow = get_flow_info(args.file_path, args.flow_name)
    if not flow:
        print(f"Flow '{args.flow_name}' not found in {args.file_path}")
        sys.exit(1)

    add_to_prefect_yaml(args.yaml_path, flow, args.pool)
