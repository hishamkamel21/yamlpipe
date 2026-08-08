import importlib.util
import os
import logging
import yaml

logger = logging.getLogger("ModuleLoader") 


class ModuleLoader:

    @staticmethod
    def _find_project_root() -> str:
        """
        Dynamically locates the active project directory by looking for project.yml 
        upward from current working directory and extracting 'project_dir'.
        """
        current_dir = os.path.abspath(os.getcwd())
        search_dir = current_dir

        # Search upward (Current Dir -> Parent -> Root)
        while True:
            candidate_config = os.path.join(search_dir, "project.yml")
            if os.path.exists(candidate_config):
                try:
                    with open(candidate_config, "r", encoding="utf-8") as f:
                        config = yaml.safe_load(f)
                        project_dir = config.get("project", {}).get("project_dir")
                        if project_dir:
                            return project_dir
                        return search_dir
                except Exception as e:
                    logger.warning(f"Error reading {candidate_config}: {str(e)}")

            parent_dir = os.path.dirname(search_dir)
            if parent_dir == search_dir:  # Reached filesystem root
                break
            search_dir = parent_dir

        # Fallback to CWD if project.yml is missing
        logger.warning("project.yml not found in hierarchy. Using CWD as fallback.")
        return current_dir

    @classmethod
    def schema_loader(cls, schema_name: str) -> dict:
        """Loads a YAML file from the schema_registry directory defined in project.yml."""
        if not (schema_name.endswith(".yaml") or schema_name.endswith(".yml")):
            file_name_yaml = f"{schema_name}.yaml"
            file_name_yml = f"{schema_name}.yml"
        else:
            file_name_yaml = schema_name
            file_name_yml = schema_name

        project_root = cls._find_project_root()
        schema_dir = os.path.join(project_root, "yaml_configs", "schema_registry")

        file_path_yaml = os.path.join(schema_dir, file_name_yaml)
        file_path_yml = os.path.join(schema_dir, file_name_yml)

        # Check for .yaml or .yml extensions automatically
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
    def functions_loader(cls, func_name: str, *args, **kwargs):
        """Loads and executes a Python function from the functions/ directory."""
        project_root = cls._find_project_root()
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
            
            # Pass positional and keyword arguments through directly
            return func_to_run(*args, **kwargs)

        except AttributeError:
            raise AttributeError(
                f"File '{func_name}.py' loaded successfully, but does not contain function '{func_name}()'"
            )
        except Exception as e:
            raise RuntimeError(f"Error executing function '{func_name}': {str(e)}") from e 

        
    @classmethod
    def custom_checks_loader(cls, func_name: str, *args, **kwargs):
        """Loads and executes a Python check from the custom_checks/ directory."""
        project_root = cls._find_project_root()
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
            
            # Pass positional (*args) and keyword (**kwargs) arguments through directly
            return func_to_run(*args, **kwargs)

        except AttributeError:
            raise AttributeError(
                f"File '{func_name}.py' loaded successfully, but does not contain custom check '{func_name}()'"
            )
        except Exception as e:
            raise RuntimeError(f"Error executing custom check '{func_name}': {str(e)}") from e 
        