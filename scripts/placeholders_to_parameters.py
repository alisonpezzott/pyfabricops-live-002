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
    replace_data_pipeline_placeholders_with_variables,
    replace_notebook_placeholders_with_variables,
)

# Replace data pipeline variables to config
replace_data_pipeline_placeholders_with_variables(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    data_pipeline_name='CopyData',
    config_path=config_path,
    branch=branch,
) 


# Replace notebook variables
replace_notebook_placeholders_with_variables(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    notebook_name='TransformAndLoad',
    config_path=config_path,
    branch=branch,
)


# Replace semantic models with placeholders
pf.replace_semantic_models_placeholders_with_parameters(
    project_path=project_path,
    workspace_alias=workspace_alias,
    config_path=config_path,
    branch=branch,
)
