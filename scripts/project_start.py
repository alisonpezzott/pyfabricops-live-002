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

capacity_id = '7732a1eb-3893-4642-a85c-93fc3f35d076'

workspace =pf.create_workspace(
    workspace_name,
    capacity=capacity_id,
    description='A Microsoft Fabric project with PyFabricOps - Live',
    roles=pf.read_json('workspaces_roles.json'),
    df=True
)
print(workspace) 


# Export the workspace to config
pf.export_workspace_config(
    workspace_name,
    project_path=project_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)

# Retrieve the workspace_id from config
workspace_id = pf.read_json(config_path)[branch][workspace_alias]['workspace_config']['workspace_id']
print(f"Workspace ID: {workspace_id}")

# Create the folder structure for the project
pf.create_folder(
    workspace_id,
    folder='Engineering',
)

pf.create_folder(
    workspace_id,
    folder='PowerBI',
)

pf.create_folder(
    workspace_id,
    folder='Direct',
    parent_folder='PowerBI',
)

pf.create_folder(
    workspace_id,
    folder='Import',
    parent_folder='PowerBI',
)

pf.export_folders(
    workspace_id,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)

# Create the lakehouse
lakehouse = pf.create_lakehouse(
    workspace_id,
    display_name='MainStorage',
    description='A lakehouse for the Microsoft Fabric project with PyFabricOps',
    folder='Engineering',
)

# Export the lakehouse to config
pf.export_all_lakehouses(
    workspace_id,
    project_path=project_path,
    workspace_path=workspace_path,
    config_path=config_path,
    branch=branch,
    workspace_suffix=workspace_suffix,
)