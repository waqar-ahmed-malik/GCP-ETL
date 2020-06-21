import json


def flat_dict(nested_dict: dict, transformed_data: dict, row_id: list, column_path: list):
    for key, value in nested_dict.items():
        column_path.append(key)
        if isinstance(value, dict):
            flat_dict(value, transformed_data, row_id, column_path)
        elif isinstance(value, list):
            flat_list(value, transformed_data, row_id, column_path)
        else:
            sheet_name = '_'.join(column_path.copy()[:-1])
            if sheet_name not in transformed_data:
                transformed_data.__setitem__(sheet_name, {})
            row_identifier = '_'.join([str(id) for id in row_id.copy()])
            if row_identifier not in transformed_data[sheet_name]:
                transformed_data[sheet_name].__setitem__(row_identifier, {})
            transformed_data[sheet_name][row_identifier].__setitem__(column_path.copy()[-1], value)
        column_path.pop()


def flat_list(nested_list: list, transformed_data: dict, row_id: list, column_path: list):
    temp_list = []
    row_id.append(0)
    for item in nested_list:
        row_id[-1] = row_id[-1] + 1
        if isinstance(item, list):
            flat_list(item, transformed_data, row_id, column_path)
        elif isinstance(item, dict):
            flat_dict(item, transformed_data, row_id, column_path)
        else:
            temp_list.append(str(item))
    row_id.pop()
    if len(temp_list) > 0:
        sheet_name = '_'.join(column_path.copy()[:-1])
        if sheet_name not in transformed_data:
            transformed_data.__setitem__(sheet_name, {})
        row_identifier = '_'.join([str(id) for id in row_id.copy()])
        if row_identifier not in transformed_data[sheet_name]:
            transformed_data[sheet_name].__setitem__(row_identifier, {})
        transformed_data[sheet_name][row_identifier].__setitem__(column_path.copy()[-1], '|||'.join(temp_list))
    

def JSON_to_excel(source_path: str, destination_path: str, main_sheet_name:str):
    with open(source_path, encoding='cp850') as f:
        json_data = json.load(f)
    transformed_data = {}
    if isinstance(json_data, list):
        flat_list(json_data, transformed_data, [], [main_sheet_name])
    else:
        flat_dict(json_data, transformed_data, [], [])
    # print(transformed_data)
    flat_data = {}
    for sheet_name, data in transformed_data.items():
        flat_data.__setitem__(sheet_name, [])
        for row_identifier, row in data.items():
            for i in range(len(row_identifier.split('_'))):
                row.__setitem__('{}_Row_ID'.format(sheet_name.split('_')[i]), row_identifier.split('_')[i])
            flat_data[sheet_name].append(row)
    
    import pandas as pd    
    # https://github.com/PyCQA/pylint/issues/3060 pylint: disable=abstract-class-instantiated
    with pd.ExcelWriter(destination_path) as writer:
        for sheet_name, data in flat_data.items():
            df = pd.DataFrame(data)
            df.to_excel(writer, sheet_name=sheet_name, index=False)
    
                

