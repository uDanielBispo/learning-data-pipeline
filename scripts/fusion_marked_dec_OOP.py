from data_processment import Data

path_json = 'data_raw/dados_empresaA.json'
path_csv = 'data_raw/dados_empresaB.csv'
path_merged_data = 'data_processed/merged_data.csv'

key_mapping = {'Nome do Item': 'Nome do Produto',
                'Classificação do Produto': 'Categoria do Produto',
                'Valor em Reais (R$)': 'Preço do Produto (R$)',
                'Quantidade em Estoque': 'Quantidade em Estoque',
                'Nome da Loja': 'Filial',
                'Data da Venda': 'Data da Venda'}

data_json = Data(path_json, 'json', key_mapping)
data_csv = Data(path_csv, 'csv', key_mapping)

data_csv.fix_list_header()
data_csv.list_into_dict()

merged_data = Data.merge_lists(data_json, data_csv, key_mapping)
merged_data.write_csv(path_merged_data)