import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conectar ao MongoDB
client = MongoClient("mongodb://admin:senhaforte@serorc.serpra.com.br:27017/")  # Substitua pelo URL do seu MongoDB
db = client['serorc']  # Substitua pelo nome do seu banco de dados
insumos_collection = db['insumos']  # Nome da coleção 'insumos'

# Definir a data de cotação (01/11/2024)
data_cotacao = datetime(2024, 11, 1)

# Ler o arquivo Excel
excel_path = r"C:\Users\gabriel amorim\Downloads\SINAPI_Preco_Ref_Insumos_MT_202411_Desonerado.xlsx"
df = pd.read_excel(excel_path, sheet_name="sheet1", header=None, skiprows=7, usecols="A,B,C,E")

# Ajuste no dataframe (renomeando as colunas)
df.columns = ['codigo', 'nome', 'unidade_medida', 'preco_desonerado']
tipo_insumo = "SINAPI"

# Iterar sobre as linhas do DataFrame para criar os insumos
insumos = []
for index, row in df.iterrows():
    # Garantir que 'nome' e 'unidade_medida' sejam strings e remover os espaços extras
    nome = str(row['nome']).strip()  # Convertendo para string e removendo espaços
    unidade_medida = str(row['unidade_medida']).strip()  # Convertendo para string e removendo espaços

    # Converter preco_desonerado para float (Double no MongoDB)
    try:
        preco_desonerado = str(row['preco_desonerado']).strip()  # Remover espaços extras
        # Substituir a vírgula por ponto e remover o ponto de milhares
        preco_desonerado = preco_desonerado.replace(".", "").replace(",", ".")
        preco_desonerado = float(preco_desonerado)  # Converter para float (double no MongoDB)
    except ValueError:
        print(f"Preço desonerado inválido na linha {index + 8}. Ignorando insumo.")
        continue

    insumo = {
        "codigo": row['codigo'],
        "nome": nome,  # Nome sem espaços extras
        "tipo": tipo_insumo,
        "unidade_medida": unidade_medida,  # Unidade de medida sem espaços extras
        "empresa": None,  # Empresa nula para insumos globais
        "precos_cotacao": [
            {
                "preco_desonerado": preco_desonerado,  # Agora como um float (Double no MongoDB)
                "preco_nao_desonerado": None,  # Se não houver preço não desonerado, deixar como None
                "data_cotacao": data_cotacao
            }
        ]
    }
    insumos.append(insumo)

# Inserir os insumos no MongoDB
if insumos:
    insumos_collection.insert_many(insumos)
    print(f'{len(insumos)} insumos adicionados com sucesso!')
else:
    print('Nenhum insumo foi encontrado para adicionar.')

# Fechar a conexão com o MongoDB
client.close()
