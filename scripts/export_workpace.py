# scripts/export_workspace.py
from scripts.bootstrap import (
    branch,
    config_path,
    pf,
    project_path, 
    root_path,
    workspace_alias,
    workspace_name,   
    workspace_path,   
    workspace_suffix,
)

# Export the workspace to config
pf.export_all_lakehouses(
    workspace_name,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)

pf.export_all_data_pipelines(
    workspace_name,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)


pf.export_all_semantic_models(
    workspace_name,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
    excluded_starts=('Main',)
)

pf.export_all_notebooks(
    workspace_name,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)