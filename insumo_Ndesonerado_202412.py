import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conectar ao MongoDB
client = MongoClient("mongodb://admin:senhaforte@serorc.serpra.com.br:27017/")
db = client['serorc']
insumos_collection = db['insumos']

# Data da cotação
data_cotacao = datetime(2024, 12, 1)

# Caminho do Excel
excel_path = r"C:\Users\gabriel amorim\Downloads\SINAPI_Preco_Ref_Insumos_MT_202412_NaoDesonerado.xlsx"
df = pd.read_excel(excel_path, sheet_name="sheet1", header=None, skiprows=7, usecols="A,B,C,E")

df.columns = ['codigo', 'nome', 'unidade_medida', 'preco_nao_desonerado']
tipo_insumo = "SINAPI"

# Processar cada linha
for index, row in df.iterrows():
    nome = str(row['nome']).strip()
    unidade_medida = str(row['unidade_medida']).strip()

    try:
        preco_str = str(row['preco_nao_desonerado']).strip().replace(".", "").replace(",", ".")
        preco_nao_desonerado = float(preco_str)
    except ValueError:
        print(f"Preço inválido na linha {index + 8}. Ignorando.")
        continue

    codigo = row['codigo']

    # Verificar se o insumo já existe
    insumo_existente = insumos_collection.find_one({"codigo": codigo})

    if insumo_existente:
        atualizado = False
        # Verificar se já existe uma cotação para a mesma data
        for preco in insumo_existente.get('precos_cotacao', []):
            if isinstance(preco['data_cotacao'], datetime) and preco['data_cotacao'].date() == data_cotacao.date():
                preco['preco_nao_desonerado'] = preco_nao_desonerado
                atualizado = True
                break

        if atualizado:
            insumos_collection.update_one(
                {"_id": insumo_existente["_id"]},
                {"$set": {"precos_cotacao": insumo_existente['precos_cotacao']}}
            )
        else:
            # Adicionar nova cotação
            insumos_collection.update_one(
                {"_id": insumo_existente["_id"]},
                {"$push": {
                    "precos_cotacao": {
                        "preco_desonerado": None,
                        "preco_nao_desonerado": preco_nao_desonerado,
                        "data_cotacao": data_cotacao
                    }
                }}
            )
    else:
        # Criar novo insumo
        novo_insumo = {
            "codigo": codigo,
            "nome": nome,
            "tipo": tipo_insumo,
            "unidade_medida": unidade_medida,
            "empresa": None,
            "precos_cotacao": [{
                "preco_desonerado": None,
                "preco_nao_desonerado": preco_nao_desonerado,
                "data_cotacao": data_cotacao
            }]
        }
        insumos_collection.insert_one(novo_insumo)

print("Importação e atualização concluídas.")
client.close()
