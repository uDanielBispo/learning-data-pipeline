import json, csv

class Data:
    def __init__(self, path, type, dataDict: dict):
        self.path = path
        self.type = type
        self.data = self.read_data()
        self.dataDict = dataDict
        self.dataSize = self.size_data()
    
    def size_data(self):
        return len(self.data)

    def read_json(self):
        with open(self.path, 'r') as file:
            data_json = json.load(file)
        return data_json

    def read_csv(self):
        data_csv = []
        with open(self.path, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                data_csv.append(row)
        
        return data_csv

    def read_data(self):
        data = []

        if(self.type == 'json'):
            data = self.read_json()
        elif(self.type == 'csv'):
            data = self.read_csv()
        elif(self.type == 'list'):
            data = self.path
            self.path = 'list in memory'
        return data
    
    def fix_list_header(self): 
        if(self.type == 'csv'):
            new_header = []
            list_header_fixed = []
            
            new_header = [self.dataDict.get(row) for row in self.data[0]]

            list_header_fixed.append(new_header)
            list_header_fixed.extend(self.data[1:])

            self.data = list_header_fixed
        else: print("ERRO: Função disponivel apenas para tipo csv")

    def list_into_dict(self):
        self.data = [dict(zip(self.data[0], values)) for values in self.data[1:]]

    def merge_lists(data1, data2, dataDict):
        merged_list = []
        merged_list.extend(data1.data)
        merged_list.extend(data2.data)

        return Data(merged_list, 'list', dataDict)

    def fix_null_dict_data(self):
        column_names = list(self.data[-1].keys())
        merged_column_fixed_data = [column_names]

        for item in self.data:
            fixed_item = []
            for column in column_names:
                fixed_item.append(item.get(column, 'Indisponivel')) #cria uma nova lista com os dados corrigidos
            merged_column_fixed_data.append(fixed_item) #adiciona todos os dados corrigidos na lista com o cabeçalho correto
        return merged_column_fixed_data

    def write_csv(self, path):
        merged_column_fixed_data = self.fix_null_dict_data()

        with open(path, 'w') as file:
            writer = csv.writer(file)
            writer.writerows(merged_column_fixed_data)