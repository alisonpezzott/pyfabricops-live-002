import json
import re

def _extract_data_pipeline_variables(path: str) -> list:

    with open(path, 'r') as f:
        content = json.load(f)

    activities = content['properties']['activities'] 

    variables = []

    for activity_index, activity in enumerate(activities):
        activity_name = activity['name']

        subactivities = activity['typeProperties']['activities']
        for subactivity_index, subactivity in enumerate(subactivities):
            subactivity_name = subactivity['name']
            properties = subactivity['typeProperties']
            
            source = properties['source']['datasetSettings']
            source_database = source['typeProperties']['database']
            source_connection = source['externalReferences']['connection']

            sink = properties['sink']['datasetSettings']['linkedService']
            sink_name = sink['name']
            sink_properties = sink['properties']['typeProperties']
            sink_workspace_id = sink_properties['workspaceId']
            sink_artifact_id = sink_properties['artifactId']

            variables.append(
                {
                    'activity_index': activity_index,
                    'activity_name': activity_name,
                    'subactivity_index': subactivity_index,
                    'subactivity_name': subactivity_name,
                    'source_database': source_database,
                    'source_connection': source_connection,
                    'sink_name': sink_name,
                    'sink_workspace_id': sink_workspace_id,
                    'sink_artifact_id': sink_artifact_id
                }
            )

    return variables


def _replace_data_pipeline_variables_with_placeholders(path: str, variables: list) -> str:

    with open(path, 'r') as f:
        content = json.load(f)

    for variable in variables:
        # Use indexes to find correct variable - Does not assume unique names
        # This allows multiple activities/subactivities with the same name
        activity_idx = variable['activity_index']
        subactivity_idx = variable['subactivity_index']
        
        # Substitute just the values that need to be replaced with placeholders
        # Database
        content['properties']['activities'][activity_idx]['typeProperties']['activities'][subactivity_idx]['typeProperties']['source']['datasetSettings']['typeProperties']['database'] = f"#{{{variable['activity_name']}_{variable['subactivity_name']}_source_database}}#"
        
        # Connection  
        content['properties']['activities'][activity_idx]['typeProperties']['activities'][subactivity_idx]['typeProperties']['source']['datasetSettings']['externalReferences']['connection'] = f"#{{{variable['activity_name']}_{variable['subactivity_name']}_source_connection}}#"
        
        # Workspace ID
        content['properties']['activities'][activity_idx]['typeProperties']['activities'][subactivity_idx]['typeProperties']['sink']['datasetSettings']['linkedService']['properties']['typeProperties']['workspaceId'] = f"#{{{variable['activity_name']}_{variable['subactivity_name']}_sink_workspace_id}}#"
        
        # Artifact ID
        content['properties']['activities'][activity_idx]['typeProperties']['activities'][subactivity_idx]['typeProperties']['sink']['datasetSettings']['linkedService']['properties']['typeProperties']['artifactId'] = f"#{{{variable['activity_name']}_{variable['subactivity_name']}_sink_artifact_id}}#"

    return json.dumps(content, indent=2)


def _create_data_pipeline_placeholder_mapping(variables: list) -> dict:
    """
    Creates a mapping of placeholders to their real values based on the extracted variables.
    """
    placeholder_mapping = {}
    
    for variable in variables:
        activity_name = variable['activity_name']
        subactivity_name = variable['subactivity_name']
        
        # Create a unique placeholder for each variable
        placeholder_mapping[f"{activity_name}_{subactivity_name}_source_database"] = variable['source_database']
        placeholder_mapping[f"{activity_name}_{subactivity_name}_source_connection"] = variable['source_connection']
        placeholder_mapping[f"{activity_name}_{subactivity_name}_sink_workspace_id"] = variable['sink_workspace_id']
        placeholder_mapping[f"{activity_name}_{subactivity_name}_sink_artifact_id"] = variable['sink_artifact_id']
    
    return placeholder_mapping


def _replace_data_pipeline_placeholders_with_variables(path: str, variables: list) -> str:

    with open(path, 'r') as f:
        content_str = f.read()

    mappings = _create_data_pipeline_placeholder_mapping(variables)

    # Substitute each placeholder with the corresponding value
    for placeholder, value in mappings.items():
        placeholder_pattern = f"#{{{placeholder}}}#"
        content_str = content_str.replace(placeholder_pattern, value)
    
    return content_str


def export_data_pipeline_variables_to_config(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    data_pipeline_name: str,
    config_path: str, 
    branch: str,
):
    """
    Export the data pipeline variables to config  
    """
    data_pipeline_path = f'{project_path}/{workspace_path}/{data_pipeline_name}.DataPipeline/pipeline-content.json'

    variables = _extract_data_pipeline_variables(data_pipeline_path) 

    if not variables:
        print(f"No variables found in {data_pipeline_name}.")
        exit(0)

    with open(config_path, 'r') as file:
        config = json.load(file)
    
    if 'data_pipelines' not in config[branch][workspace_alias]:
        config[branch][workspace_alias]['data_pipelines'] = {}
    if data_pipeline_name not in config[branch][workspace_alias]['data_pipelines']:
        config[branch][workspace_alias]['data_pipelines'][data_pipeline_name] = {}
    if 'variables' not in config[branch][workspace_alias]['data_pipelines'][data_pipeline_name]:
        config[branch][workspace_alias]['data_pipelines'][data_pipeline_name]['variables'] = {}
    config[branch][workspace_alias]['data_pipelines'][data_pipeline_name]['variables'] = variables

    with open(config_path, 'w') as file:
        json.dump(config, file, indent=4)

    print(f"Variables from {data_pipeline_name} extracted and saved to {config_path}.") 


def replace_data_pipeline_variables_with_placeholders(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    data_pipeline_name: str,
    config_path: str, 
    branch: str,
):

    data_pipeline_path = f'{project_path}/{workspace_path}/{data_pipeline_name}.DataPipeline/pipeline-content.json'

    with open(config_path, 'r') as file:
        config = json.load(file)

    variables = config[branch][workspace_alias]['data_pipelines'][data_pipeline_name]['variables']

    print(variables) 

    if not variables:
        print(f"No variables found for {data_pipeline_name} in {config_path}.")
        exit(0)

    modified_content = _replace_data_pipeline_variables_with_placeholders(data_pipeline_path, variables)

    # Save the modified content back to the file
    with open(data_pipeline_path, 'w') as file:
        file.write(modified_content)

    print(f"Variables from {config_path} replaced in {data_pipeline_path} with placeholders.") 


def replace_data_pipeline_placeholders_with_variables(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    data_pipeline_name: str,
    config_path: str, 
    branch: str,
):
    data_pipeline_path = f'{project_path}/{workspace_path}/{data_pipeline_name}.DataPipeline/pipeline-content.json'

    with open(config_path, 'r') as file:
        config = json.load(file)

    variables = config[branch][workspace_alias]['data_pipelines'][data_pipeline_name]['variables']

    print(variables) 

    if not variables:
        print(f"No variables found for {data_pipeline_name} in {config_path}.")
        exit(0)

    modified_content = _replace_data_pipeline_placeholders_with_variables(data_pipeline_path, variables)

    # Save the modified content back to the file
    with open(data_pipeline_path, 'w') as file:
        file.write(modified_content)

    print(f"Placeholders from {config_path} replaced in {data_pipeline_path} with variables.") 


def _extract_dataflow_gen2_variables(path: str) -> list:
    """
    Extract parameters from a Dataflow Gen2 mashup.pq file, identifying each destination separately.
    
    Args:
        path (str): Path to the mashup.pq file
        
    Returns:
        list: List of dictionaries containing the extracted parameters for each destination
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read() 

    parameters = []
    
    # Pattern to find DataDestination sections
    # Look for shared QueryName_DataDestination = let
    destination_pattern = r'shared\s+(\w+)_DataDestination\s*=\s*let(.*?)in\s*\w+;'
    destination_matches = re.findall(destination_pattern, content, re.DOTALL)
    
    for query_name, destination_content in destination_matches:
        param_dict = {
            'destination_name': f"{query_name}_DataDestination",
            'query_name': query_name
        }
        
        # Extract workspaceId
        workspace_pattern = r'workspaceId\s*=\s*"([a-f0-9-]+)"'
        workspace_match = re.search(workspace_pattern, destination_content)
        if workspace_match:
            param_dict['workspaceId'] = workspace_match.group(1)
        
        # Extract lakehouseId
        lakehouse_pattern = r'lakehouseId\s*=\s*"([a-f0-9-]+)"'
        lakehouse_match = re.search(lakehouse_pattern, destination_content)
        if lakehouse_match:
            param_dict['lakehouseId'] = lakehouse_match.group(1)
            param_dict['destination_type'] = 'Lakehouse'
        
        # Extract warehouseId
        warehouse_pattern = r'warehouseId\s*=\s*"([a-f0-9-]+)"'
        warehouse_match = re.search(warehouse_pattern, destination_content)
        if warehouse_match:
            param_dict['warehouseId'] = warehouse_match.group(1)
            param_dict['destination_type'] = 'Warehouse'
        
        # Extract semanticModelId (if present)
        semantic_pattern = r'semanticModelId\s*=\s*"([a-f0-9-]+)"'
        semantic_match = re.search(semantic_pattern, destination_content)
        if semantic_match:
            param_dict['semanticModelId'] = semantic_match.group(1)
            param_dict['destination_type'] = 'SemanticModel'
        
        # Only add if we found at least one ID parameter
        if any(key.endswith('Id') for key in param_dict.keys()):
            parameters.append(param_dict)
    
    return parameters


def _replace_dataflow_gen2_parameters_with_placeholders(path: str, parameters: list, dataflow_name: str) -> str:
    """
    Replace parameters with placeholders in a Dataflow Gen2 mashup.pq file.
    Each destination gets unique placeholders based on its query name.
    
    Args:
        path (str): Path to the mashup.pq file
        parameters (list): List of parameter dictionaries to replace
        dataflow_name (str): Name of the dataflow for placeholder naming
        
    Returns:
        str: Modified content with placeholders
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace each destination's parameters with unique placeholders
    for param_dict in parameters:
        query_name = param_dict['query_name']
        
        # Replace workspaceId
        if 'workspaceId' in param_dict:
            workspace_id = param_dict['workspaceId']
            placeholder = f"#{{{dataflow_name}_{query_name}_workspaceId}}#"
            content = content.replace(f'workspaceId = "{workspace_id}"', f'workspaceId = "{placeholder}"')
        
        # Replace lakehouseId
        if 'lakehouseId' in param_dict:
            lakehouse_id = param_dict['lakehouseId']
            placeholder = f"#{{{dataflow_name}_{query_name}_lakehouseId}}#"
            content = content.replace(f'lakehouseId = "{lakehouse_id}"', f'lakehouseId = "{placeholder}"')
        
        # Replace warehouseId
        if 'warehouseId' in param_dict:
            warehouse_id = param_dict['warehouseId']
            placeholder = f"#{{{dataflow_name}_{query_name}_warehouseId}}#"
            content = content.replace(f'warehouseId = "{warehouse_id}"', f'warehouseId = "{placeholder}"')
        
        # Replace semanticModelId
        if 'semanticModelId' in param_dict:
            semantic_id = param_dict['semanticModelId']
            placeholder = f"#{{{dataflow_name}_{query_name}_semanticModelId}}#"
            content = content.replace(f'semanticModelId = "{semantic_id}"', f'semanticModelId = "{placeholder}"')
    
    return content


def _replace_dataflow_gen2_placeholders_with_parameters(path: str, parameters: list, dataflow_name: str) -> str:
    """
    Replace placeholders with actual parameters in a Dataflow Gen2 mashup.pq file.
    
    Args:
        path (str): Path to the mashup.pq file
        parameters (list): List of parameter dictionaries with actual values
        dataflow_name (str): Name of the dataflow for placeholder naming
        
    Returns:
        str: Modified content with actual parameter values
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace placeholders with actual values for each destination
    for param_dict in parameters:
        query_name = param_dict['query_name']
        
        # Replace workspaceId placeholder
        if 'workspaceId' in param_dict:
            workspace_id = param_dict['workspaceId']
            placeholder = f"#{{{dataflow_name}_{query_name}_workspaceId}}#"
            content = content.replace(f'workspaceId = "{placeholder}"', f'workspaceId = "{workspace_id}"')
        
        # Replace lakehouseId placeholder
        if 'lakehouseId' in param_dict:
            lakehouse_id = param_dict['lakehouseId']
            placeholder = f"#{{{dataflow_name}_{query_name}_lakehouseId}}#"
            content = content.replace(f'lakehouseId = "{placeholder}"', f'lakehouseId = "{lakehouse_id}"')
        
        # Replace warehouseId placeholder
        if 'warehouseId' in param_dict:
            warehouse_id = param_dict['warehouseId']
            placeholder = f"#{{{dataflow_name}_{query_name}_warehouseId}}#"
            content = content.replace(f'warehouseId = "{placeholder}"', f'warehouseId = "{warehouse_id}"')
        
        # Replace semanticModelId placeholder
        if 'semanticModelId' in param_dict:
            semantic_id = param_dict['semanticModelId']
            placeholder = f"#{{{dataflow_name}_{query_name}_semanticModelId}}#"
            content = content.replace(f'semanticModelId = "{placeholder}"', f'semanticModelId = "{semantic_id}"')
    
    return content


def export_dataflow_gen2_variables(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    dataflow_name: str,
    config_path: str, 
    branch: str,
):
    dataflow_path = f'{project_path}/{workspace_path}/{dataflow_name}.Dataflow/mashup.pq'

    # Extract current parameters from the dataflow file
    current_parameters = _extract_dataflow_gen2_variables(dataflow_path)

    if not current_parameters:
        print(f"No parameters found in {dataflow_path}.")
        exit(0)

    # Save the extracted parameters to config.json
    with open(config_path, 'r') as file:
        config = json.load(file)

    # Add dataflow configuration if it doesn't exist
    if 'dataflows' not in config[branch][workspace_alias]:
        config[branch][workspace_alias]['dataflows'] = {}

    if dataflow_name not in config[branch][workspace_alias]['dataflows']:
        config[branch][workspace_alias]['dataflows'][dataflow_gen2_name] = {}

    # Save the extracted parameters as variables
    config[branch][workspace_alias]['dataflows'][dataflow_name]['variables'] = current_parameters

    # Save updated config
    with open(config_path, 'w') as file:
        json.dump(config, file, indent=4)

    print(f"Parameters saved to {config_path} under dataflows.{dataflow_name}.variables")


def replace_dataflow_placeholders_with_variables(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    dataflow_name: str,
    config_path: str, 
    branch: str,
):
    dataflow_path = f'{project_path}/{workspace_path}/{dataflow_name}.Dataflow/mashup.pq'

    with open(config_path, 'r') as file:
        config = json.load(file)

    variables = config[branch][workspace_alias]['dataflows'][dataflow_name]['variables']

    print("Variables from config:")
    print(variables)

    if not variables:
        print(f"No variables found for {dataflow_name} in {config_path}.")
        exit(0)

    # Replace placeholders with actual values
    modified_content = _replace_dataflow_gen2_placeholders_with_parameters(dataflow_path, variables, dataflow_name)

    # Save the modified content back to the file
    with open(dataflow_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

    print(f"Placeholders replaced with variables from {config_path} in {dataflow_path}.")


def replace_dataflow_gen2_variables_with_placeholders(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    dataflow_name: str,
    config_path: str, 
    branch: str,
):
    dataflow_path = f'{project_path}/{workspace_path}/{dataflow_name}.Dataflow/mashup.pq'

    with open(config_path, 'r') as file:
        config = json.load(file)

    variables = config[branch][workspace_alias]['dataflows'][dataflow_name]['variables']

    if not variables:
        print(f"No parameters found in {dataflow_path}.")
        exit(0)

    # Replace parameters with placeholders
    modified_content = _replace_dataflow_gen2_parameters_with_placeholders(dataflow_path, variables, dataflow_name)

    # Save the modified content back to the file
    with open(dataflow_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

    print(f"Parameters replaced with placeholders in {dataflow_path}.")
    

def _extract_parameters_notebook(path: str) -> list:
    """
    Extract parameters from a Fabric notebook-content.py file.
    
    Args:
        path (str): Path to the notebook-content.py file
        
    Returns:
        list: List of dictionaries containing the extracted parameters
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    parameters = []
    
    # Find the PARAMETERS CELL section
    # Look for "# PARAMETERS CELL ********************" followed by the parameters
    parameters_pattern = r'# PARAMETERS CELL \*+\s*\n(.*?)(?=# METADATA|# CELL|# MARKDOWN|$)'
    parameters_match = re.search(parameters_pattern, content, re.DOTALL)
    
    if parameters_match:
        parameters_content = parameters_match.group(1).strip()
        
        # Extract variable assignments
        # Pattern to find variable = "value" or variable = f"value"
        variable_patterns = [
            r'(\w+)\s*=\s*"([^"]*)"',  # variable = "value"
            r'(\w+)\s*=\s*f"([^"]*)"', # variable = f"value"
            r'(\w+)\s*=\s*\'([^\']*)\'', # variable = 'value'
            r'(\w+)\s*=\s*f\'([^\']*)\'' # variable = f'value'
        ]
        
        for pattern in variable_patterns:
            matches = re.findall(pattern, parameters_content)
            for var_name, var_value in matches:
                # Skip variables that are derived from other variables (contain f-string references)
                if not re.search(r'\{[^}]+\}', var_value):
                    parameters.append({
                        'variable_name': var_name,
                        'variable_value': var_value,
                        'parameter_type': 'string'
                    })
        
        # Also look for numeric and boolean assignments
        numeric_pattern = r'(\w+)\s*=\s*(\d+(?:\.\d+)?)'
        numeric_matches = re.findall(numeric_pattern, parameters_content)
        for var_name, var_value in numeric_matches:
            parameters.append({
                'variable_name': var_name,
                'variable_value': var_value,
                'parameter_type': 'numeric'
            })
        
        boolean_pattern = r'(\w+)\s*=\s*(True|False)'
        boolean_matches = re.findall(boolean_pattern, parameters_content)
        for var_name, var_value in boolean_matches:
            parameters.append({
                'variable_name': var_name,
                'variable_value': var_value,
                'parameter_type': 'boolean'
            })
    
    return parameters


def _replace_notebook_parameters_with_placeholders(path: str, parameters: list, notebook_name: str) -> str:
    """
    Replace parameters with placeholders in a Fabric notebook-content.py file.
    
    Args:
        path (str): Path to the notebook-content.py file
        parameters (list): List of parameter dictionaries to replace
        notebook_name (str): Name of the notebook for placeholder naming
        
    Returns:
        str: Modified content with placeholders
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace each parameter with a placeholder
    for param_dict in parameters:
        var_name = param_dict['variable_name']
        var_value = param_dict['variable_value']
        param_type = param_dict['parameter_type']
        
        placeholder = f"#{{{notebook_name}_{var_name}}}#"
        
        # Create different replacement patterns based on parameter type
        if param_type == 'string':
            # Handle both regular strings and f-strings
            old_patterns = [
                f'{var_name} = "{var_value}"',
                f'{var_name} = f"{var_value}"',
                f'{var_name} = \'{var_value}\'',
                f'{var_name} = f\'{var_value}\''
            ]
            new_value = f'{var_name} = "{placeholder}"'
        elif param_type in ['numeric', 'boolean']:
            old_patterns = [f'{var_name} = {var_value}']
            new_value = f'{var_name} = "{placeholder}"'
        
        # Replace all matching patterns
        for old_pattern in old_patterns:
            if old_pattern in content:
                content = content.replace(old_pattern, new_value)
                break
    
    return content


def _replace_notebook_placeholders_with_parameters(path: str, parameters: list, notebook_name: str) -> str:
    """
    Replace placeholders with actual parameters in a Fabric notebook-content.py file.
    
    Args:
        path (str): Path to the notebook-content.py file
        parameters (list): List of parameter dictionaries with actual values
        notebook_name (str): Name of the notebook for placeholder naming
        
    Returns:
        str: Modified content with actual parameter values
    """
    with open(path, 'r', encoding='utf-8') as f:
        content = f.read()
    
    # Replace placeholders with actual values
    for param_dict in parameters:
        var_name = param_dict['variable_name']
        var_value = param_dict['variable_value']
        param_type = param_dict['parameter_type']
        
        placeholder = f"#{{{notebook_name}_{var_name}}}#"
        
        # Restore original format based on parameter type
        if param_type == 'string':
            new_value = f'{var_name} = "{var_value}"'
        elif param_type in ['numeric', 'boolean']:
            new_value = f'{var_name} = {var_value}'
        
        # Replace placeholder with original value
        old_pattern = f'{var_name} = "{placeholder}"'
        if old_pattern in content:
            content = content.replace(old_pattern, new_value)
    
    return content


def export_notebook_variables(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    notebook_name: str,
    config_path: str, 
    branch: str,
):
    notebook_path = f'{project_path}/{workspace_path}/{notebook_name}.Notebook/notebook-content.py'

    # Extract current parameters from the notebook file
    current_parameters = _extract_parameters_notebook(notebook_path)

    if not current_parameters:
        print(f"No parameters found in {notebook_path}.")
        exit(0)

    # Save the extracted parameters to config.json
    with open(config_path, 'r') as file:
        config = json.load(file)

    # Add notebook configuration if it doesn't exist
    if 'notebooks' not in config[branch][workspace_alias]:
        config[branch][workspace_alias]['notebooks'] = {}

    if notebook_name not in config[branch][workspace_alias]['notebooks']:
        config[branch][workspace_alias]['notebooks'][notebook_name] = {}

    # Save the extracted parameters as variables
    config[branch][workspace_alias]['notebooks'][notebook_name]['variables'] = current_parameters

    # Save updated config
    with open(config_path, 'w') as file:
        json.dump(config, file, indent=4)

    print(f"Parameters saved to {config_path} under notebooks.{notebook_name}.variables")


def replace_notebook_placeholders_with_variables(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    notebook_name: str,
    config_path: str, 
    branch: str,
):
    notebook_path = f'{project_path}/{workspace_path}/{notebook_name}.Notebook/notebook-content.py'

    # Load parameters from config
    print("Loading parameters from config...")
    with open(config_path, 'r') as file:
        config = json.load(file)

    variables = config[branch][workspace_alias]['notebooks'][notebook_name]['variables']

    if not variables:
        print(f"No variables found for {notebook_name} in {config_path}.")
        exit(0)

    # Replace placeholders with actual values
    print("\nReplacing placeholders with actual values...")
    modified_content = _replace_notebook_placeholders_with_parameters(notebook_path, variables, notebook_name)

    # Save the modified content back to the file
    with open(notebook_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

    print(f"✓ Placeholders replaced with variables from {config_path} in {notebook_path}.")


def replace_notebook_variables_with_placeholders(
    project_path: str,
    workspace_alias: str, 
    workspace_path: str, 
    notebook_name: str,
    config_path: str, 
    branch: str,
):
    notebook_path = f'{project_path}/{workspace_path}/{notebook_name}.Notebook/notebook-content.py'

    # Retrieve variables from config
    with open(config_path, 'r') as file:
        config = json.load(file)

    notebook_variables = config[branch][workspace_alias]['notebooks'][notebook_name]['variables']

    print("Current variables extracted from notebook:")

    if not notebook_variables:
        print(f"No variables found for {notebook_name} in {config_path}.")
        exit(0)

    # Replace variables with placeholders
    print("\nReplacing variables with placeholders...")
    modified_content = _replace_notebook_parameters_with_placeholders(notebook_path, notebook_variables, notebook_name)

    # Save the modified content back to the file
    with open(notebook_path, 'w', encoding='utf-8') as file:
        file.write(modified_content)

    print(f"✓ Variables replaced with placeholders in {notebook_path}.")
