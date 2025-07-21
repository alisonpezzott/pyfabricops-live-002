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
    replace_data_pipeline_variables_with_placeholders,
    replace_notebook_variables_with_placeholders,
)

# Replace data pipeline variables to config
replace_data_pipeline_variables_with_placeholders(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    data_pipeline_name='CopyData',
    config_path=config_path,
    branch=branch,
) 


# Replace notebook variables
replace_notebook_variables_with_placeholders(
    project_path=project_path,
    workspace_alias=workspace_alias,
    workspace_path=workspace_alias + '/Engineering',
    notebook_name='TransformAndLoad',
    config_path=config_path,
    branch=branch,
)


# Replace semantic models with placeholders
pf.replace_semantic_models_parameters_with_placeholders(
    project_path=project_path,
    workspace_alias=workspace_alias,
    config_path=config_path,
    branch=branch,
)
