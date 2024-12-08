import json
import csv

path_json = 'data_raw/dados_empresaA.json'

old_csv = 'data_raw/dados_empresaB.csv'
new_csv_fixed = 'data_processed/dados_empresaB_fixed.csv'

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
        spamreader = csv.DictReader(file, delimiter=',')

        for row in spamreader:
            data_csv.append(row)
    
    return data_csv
    # with open(path_csv, 'r') as file:
    #     reader = csv.reader(file)
    #     return list(reader)
    
def data_reader(path, type):
    data = []
    if type == 'json':
        data = read_json(path)
    elif type == 'csv':
        data = read_csv(path)

    return data

def fix_csv_header(old_csv, new_csv_fixed):
    with open(old_csv, 'r') as old_file, open(new_csv_fixed, 'w') as new_file:
        reader = csv.reader(old_file)
        writer = csv.writer(new_file)

        original_header = next(reader)
        renamed_header = [key_mapping.get(coluna, coluna) for coluna in original_header]
        
        writer.writerow(renamed_header)

        for row in reader:
            writer.writerow(row)

def join(dataA, dataB):
    merged_list = []
    merged_list.extend(dataA)
    merged_list.extend(dataB)
    return merged_list

def transform_data_table(merged_list, column_names):
    merged_data_table = [column_names] #adiciona o cabeçalho na lista final

    for row in merged_list:
        row_data = []
        for column in column_names:
            row_data.append(row.get(column, 'Indisponivel')) #cria uma nova lista com os dados corrigidos
        merged_data_table.append(row_data) #adiciona todos os dados corrigidos na lista com o cabeçalho correto

    return merged_data_table

def show_data_properties(data, data_name = ''):
    if(data_name != ''):
       print("============" + data_name + "============")
    print(get_columns(data))
    print(len(data))

def get_columns(data):
    try: #if json / dictionary
        return list(data[-1].keys())
    except: #if csv / list
        return data[0]

def saving_data(data, path):
    with open(path, 'w') as file:
        writer = csv.writer(file)
        writer.writerows(data)

fix_csv_header(old_csv, new_csv_fixed)

data_json = data_reader(path_json, 'json')
show_data_properties(data_json, 'Data A')

data_csv = data_reader(new_csv_fixed, 'csv')
show_data_properties(data_csv, 'Data B')

data_fusion = join(data_json, data_csv)
data_fusion_columns = get_columns(data_fusion)
show_data_properties(data_fusion, 'Merged Data')

data_fusion_table = transform_data_table(data_fusion, data_fusion_columns)

path_merged_data = 'data_processed/merged_data.csv'

saving_data(data_fusion_table, path_merged_data)

