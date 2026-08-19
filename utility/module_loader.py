import importlib.util
import os
import logging
import yaml
from typing import Optional
from yamlpipe.utility.helper import Helper

logger = logging.getLogger("ModuleLoader")


class ModuleLoader:

    @classmethod
    def schema_loader(cls, schema_name: str, project_dir: Optional[str] = None) -> dict:
        """Loads a YAML file from the schema_registry directory defined in project.yml."""
        if not (schema_name.endswith(".yaml") or schema_name.endswith(".yml")):
            file_name_yaml = f"{schema_name}.yaml"
            file_name_yml = f"{schema_name}.yml"
        else:
            file_name_yaml = schema_name
            file_name_yml = schema_name

        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        schema_dir = os.path.join(project_root, "yaml_configs", "schema_registry")

        file_path_yaml = os.path.join(schema_dir, file_name_yaml)
        file_path_yml = os.path.join(schema_dir, file_name_yml)

        if os.path.exists(file_path_yaml):
            file_path = file_path_yaml
        elif os.path.exists(file_path_yml):
            file_path = file_path_yml
        else:
            raise FileNotFoundError(
                f"Could not find configuration file for selector '{schema_name}' in target 'schema_registry'.\n"
                f"Project root from project.yml: {project_root}\n"
                f"Searched paths:\n"
                f"  - {file_path_yaml}\n"
                f"  - {file_path_yml}"
            )

        try:
            with open(file_path, "r", encoding="utf-8") as stream:
                return yaml.safe_load(stream)
        except Exception as e:
            raise RuntimeError(f"Error loading YAML schema '{schema_name}': {str(e)}") from e

    @classmethod
    def functions_loader(cls, func_name: str, *args, project_dir: Optional[str] = None, **kwargs):
        """Loads and executes a Python function from the functions/ directory."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        file_path = os.path.join(project_root, "functions", f"{func_name}.py")

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Could not find the function file: {func_name}.py\n"
                f"Attempted path: {file_path}\n"
                f"Project root from project.yml: {project_root}"
            )

        try:
            spec = importlib.util.spec_from_file_location(func_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            func_to_run = getattr(module, func_name)
            return func_to_run(*args, **kwargs)

        except AttributeError:
            raise AttributeError(
                f"File '{func_name}.py' loaded successfully, but does not contain function '{func_name}()'"
            )
        except Exception as e:
            raise RuntimeError(f"Error executing function '{func_name}': {str(e)}") from e

    @classmethod
    def custom_checks_loader(cls, func_name: str, *args, project_dir: Optional[str] = None, **kwargs):
        """Loads and executes a Python check from the custom_checks/ directory."""
        project_root = Helper.find_project_root(explicit_project_dir=project_dir)
        file_path = os.path.join(project_root, "custom_checks", f"{func_name}.py")

        if not os.path.exists(file_path):
            raise FileNotFoundError(
                f"Could not find the custom check file: {func_name}.py\n"
                f"Attempted path: {file_path}\n"
                f"Project root from project.yml: {project_root}"
            )

        try:
            spec = importlib.util.spec_from_file_location(func_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            func_to_run = getattr(module, func_name)
            return func_to_run(*args, **kwargs)

        except AttributeError:
            raise AttributeError(
                f"File '{func_name}.py' loaded successfully, but does not contain custom check '{func_name}()'"
            )
        except Exception as e:
            raise RuntimeError(f"Error executing custom check '{func_name}': {str(e)}") from e