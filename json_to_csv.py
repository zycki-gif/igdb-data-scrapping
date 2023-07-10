import os
import json
import pandas as pd


def convert_json_to_csv(folder_path):
    # Cria a pasta "csv" se ela não existir
    output_folder = os.path.join('./csv')
    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Obtém a lista de pastas no caminho especificado
    folders = [f for f in os.listdir(folder_path) if os.path.isdir(
        os.path.join(folder_path, f))]

    # Itera sobre as pastas
    for folder in folders:
        # Cria o caminho completo da pasta
        folder_full_path = os.path.join(folder_path, folder)

        # Obtém a lista de arquivos JSON na pasta
        json_files = [file for file in os.listdir(
            folder_full_path) if file.endswith('.json')]

        # Itera sobre os arquivos JSON
        for json_file in json_files:
            # Cria o caminho completo do arquivo JSON
            json_path = os.path.join(folder_full_path, json_file)

            # Abre o arquivo JSON
            with open(json_path, 'r') as file:
                # Carrega os dados JSON
                data = json.load(file)

            # Cria o nome do arquivo CSV de saída com base no nome da pasta
            csv_file = folder + '.csv'

            # Cria o caminho completo para o arquivo CSV de saída
            csv_path = os.path.join(output_folder, csv_file)

            # Obtém as chaves únicas dos dados JSON
            keys = set()
            for record in data:
                keys.update(record.keys())

            values = {key: [] for key in keys}

            for record in data:
                for key in keys:
                    if key in record:
                        values[key].append(record[key])
                    else:
                        values[key].append(None)

            # Criar o dataframe do pandas
            df = pd.DataFrame(values)

            # Salvar como arquivo CSV
            df.to_csv(csv_path, index=False)


# Exemplo de uso
caminho_pasta = "./json"
convert_json_to_csv(caminho_pasta)
