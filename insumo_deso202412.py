import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conectar ao MongoDB
client = MongoClient("mongodb://admin:senhaforte@serorc.serpra.com.br:27017/")  # Substitua pelo URL do seu MongoDB
db = client['serorc']  # Substitua pelo nome do seu banco de dados
insumos_collection = db['insumos']  # Nome da coleção 'insumos'

# Definir a data de cotação para dezembro de 2024 (01/12/2024)
data_cotacao = datetime(2024, 12, 1)

# Ler o arquivo Excel para dezembro
excel_path = r"C:\Users\gabriel amorim\Downloads\SINAPI_Preco_Ref_Insumos_MT_202412_Desonerado.xlsx"
df = pd.read_excel(excel_path, sheet_name="sheet1", header=None, skiprows=7, usecols="A,B,C,E")

# Ajuste no dataframe (renomeando as colunas)
df.columns = ['codigo', 'nome', 'unidade_medida', 'preco_desonerado']
tipo_insumo = "SINAPI"

# Iterar sobre as linhas do DataFrame para criar os insumos
for index, row in df.iterrows():
    # Validar que o código não está vazio e que o preço desonerado é um número válido
    if pd.isna(row['codigo']) or row['codigo'] == '':
        print(f"Código inválido na linha {index + 8}. Ignorando insumo.")
        continue  # Ignorar insumo sem código válido

    try:
        preco_desonerado = str(row['preco_desonerado']).strip()  # Remover espaços extras
        # Substituir a vírgula por ponto e remover o ponto de milhares
        preco_desonerado = preco_desonerado.replace(".", "").replace(",", ".")
        preco_desonerado = float(preco_desonerado)  # Converter para float (double no MongoDB)
    except ValueError:
        print(f"Preço desonerado inválido na linha {index + 8}. Ignorando insumo.")
        continue # Ignorar insumo com preço desonerado inválido

    # Verificar se o preço desonerado foi lido corretamente
    if preco_desonerado is None or preco_desonerado <= 0:
        print(f"Preço desonerado inválido para o insumo com código {row['codigo']}. Ignorando insumo.")
        continue

    # Verificar se o insumo já existe no banco
    insumo_existente = insumos_collection.find_one({"codigo": row['codigo']})

    nome = str(row['nome']).strip()  # Convertendo para string e removendo espaços


    # Criar o objeto de insumo
    insumo = {
        "codigo": row['codigo'],
        "nome": nome,
        "tipo": tipo_insumo,
        "unidade_medida": row['unidade_medida'],
        "empresa": None,  # Empresa nula para insumos globais
        "precos_cotacao": [
            {
                "preco_desonerado": preco_desonerado,
                "preco_nao_desonerado": None,  # Se não houver preço não desonerado, deixar como None
                "data_cotacao": data_cotacao
            }
        ]
    }

    # Se o insumo já existe no banco, adicionar o novo preço à lista de preços
    if insumo_existente:
        atualizado = False
        for preco in insumo_existente.get('precos_cotacao', []):
            if isinstance(preco['data_cotacao'], datetime) and preco['data_cotacao'].date() == data_cotacao.date():
                preco['preco_desonerado'] = preco_desonerado
                atualizado = True
                break

        if atualizado:
            insumos_collection.update_one(
                {"_id": insumo_existente["_id"]},
                {"$set": {"precos_cotacao": insumo_existente['precos_cotacao']}}
            )
        else:
            insumos_collection.update_one(
                {"_id": insumo_existente["_id"]},
                {"$push": {"precos_cotacao": insumo["precos_cotacao"][0]}}
            )


    else:
        # Caso contrário, inserir o insumo novo
        insumos_collection.insert_one(insumo)
        print(f"Insumo com código {row['codigo']} inserido com sucesso!")

# Fechar a conexão com o MongoDB
client.close()
