import json
import csv

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_merged_data = 'data_processed/merged_data.csv'

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

def read_json(path_json):
    with open(path_json, 'r') as file:
        data_json = json.load(file)
    return data_json

def read_csv(path_csv):
    data_csv = []

    with open(path_csv, 'r') as file:
        reader = csv.reader(file)
        for row in reader:
            data_csv.append(row)
    
    return data_csv

def read_data(path: str, dataType: str):
    if(dataType == 'json'):
        return read_json(path)
    elif(dataType == 'csv'):
        return read_csv(path)

def fix_list_header(list: list, dict_header: dict): 
    new_header = []
    list_header_fixed = []
    
    new_header = [dict_header.get(row) for row in list[0]]

    list_header_fixed.append(new_header)
    list_header_fixed.extend(list[1:])

    return list_header_fixed

def list_into_dict(list: list):
    list = [dict(zip(list[0], values)) for values in list[1:]]
    return list

def merge_lists(data1, data2):
    merged_list = []
    merged_list.extend(data1)
    merged_list.extend(data2)
    return merged_list

def data_merge_validation(merged_data, data1, data2):
    data1_last_item = len(data1)-1
    data2_last_item = len(data2)-1
    merged_data2_initial_pos = len(data1)
    merged_data_last_position = len(merged_data)-1

    data1_state = False
    data2_state = False

    print("\nFIRST DATA ========================================")
    if(merged_data[0] == data1[0]): 
        print("First Item: OK")
        data1_state = True
    else: 
        print("WARNING: The first item merged does not combine with the first item of the first dataset")
        data1_state = False
    
    if(merged_data[data1_last_item] == data1[data1_last_item]): 
        print("Last Item: OK")
        data1_state = True
    else: 
        print("WARNING: The last item merged does not combine with the first item of the last dataset")
        data1_state = False

    print("\nSECOND DATA ========================================")    
    if(merged_data[merged_data2_initial_pos] == data2[0]): 
        print("First item of data 2: OK")
        data2_state = True
    else: 
        print("WARNING: The first item merged does not combine with the first item of the second dataset")
        data2_state = False
    
    if(merged_data[merged_data_last_position] == data2[data2_last_item]): 
        print("Last Item of data 2: OK")
        data2_state = True
    else: 
        print("WARNING: The last item merged does not combine with the first data of the last dataset\n")
        data2_state = False
    
    if(data1_state and data2_state): return True
    else: return False

def fix_null_dict_data(data: dict):
    column_names = list(data[-1].keys())
    merged_column_fixed_data = [column_names]

    for item in data:
        fixed_item = []
        for column in column_names:
            fixed_item.append(item.get(column, 'Indisponivel')) #cria uma nova lista com os dados corrigidos
        merged_column_fixed_data.append(fixed_item) #adiciona todos os dados corrigidos na lista com o cabeçalho correto
    return merged_column_fixed_data

def write_csv(path: str, data: list):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(data)

data_json = read_data(path_json, 'json')
data_csv = read_data(path_csv, 'csv')

data_csv_header_fixed = fix_list_header(data_csv, key_mapping)
data_csv_into_dict = list_into_dict(data_csv_header_fixed)

merged_data = merge_lists(data_json, data_csv_into_dict)

if(data_merge_validation(merged_data, data_json, data_csv_into_dict)):
    merged_data_nulls_fixed = fix_null_dict_data(merged_data)
    write_csv(path_merged_data, merged_data_nulls_fixed)
