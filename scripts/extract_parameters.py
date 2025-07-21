# scripts/deploy_powerbi.py
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

from scripts.utils import (
    export_data_pipeline_variables_to_config, 
    export_notebook_variables,
)

# Export data pipeline variables to config
export_data_pipeline_variables_to_config(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    data_pipeline_name='CopyData',
    config_path=config_path,
    branch=branch,
) 

# Export notebook variables
export_notebook_variables(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    notebook_name='TransformAndLoad',
    config_path=config_path,
    branch=branch,
)


# Export semantic models parameters
pf.extract_semantic_models_parameters(
    project_path=project_path,
    workspace_alias=workspace_alias,
    config_path=config_path,
    branch=branch,
)
