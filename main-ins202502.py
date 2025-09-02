import pandas as pd
from pymongo import MongoClient
from datetime import datetime

# Conectar ao MongoDB
client = MongoClient(
    "mongodb://admin:3ng3nh4r1427611CPLUSPLUSOP@serorc.serpra.com.br:27017/serorc?authSource=admin"
)
db = client['serorc']
insumos_collection = db['insumos']

# Data da nova cotação
data_cotacao = datetime(2025, 2, 1)

# Caminho do arquivo
excel_path = r"C:\Users\Dell\Downloads\202502\SINAPI_Referência_2025_02.xlsx"

abas = {
    'ICD': 'preco_desonerado',
    'ISD': 'preco_nao_desonerado'
}

for aba, tipo_preco in abas.items():
    print(f"Processando aba: {aba}")
    df = pd.read_excel(excel_path, sheet_name=aba, header=None)

    # Identificar colunas com "MT" na linha 10 (índice 9)
    linha_estado = df.iloc[9]
    colunas_mt = [i for i, val in linha_estado.items() if str(val).strip().upper() == 'MT']

    if not colunas_mt:
        print(f"Nenhuma coluna 'MT' encontrada na aba {aba}. Pulando.")
        continue

    # Processar a partir da linha 11 (índice 10)
    for i in range(10, len(df)):
        row = df.iloc[i]

        codigo = str(row[1]).strip()
        nome = str(row[2]).strip()
        unidade = str(row[3]).strip()

        # Ignorar linhas sem código ou nome
        if pd.isna(codigo) or pd.isna(nome):
            continue

        preco_valido = None
        for col in colunas_mt:
            preco_raw = row[col]
            if pd.notna(preco_raw):
                try:
                    preco_str = str(preco_raw).strip()
                    preco_float = float(preco_str)
                    preco_valido = preco_float
                    break
                except (ValueError, TypeError):
                    continue

        if preco_valido is None:
            continue  # Pular se não houver preço válido

        # Montar entrada da cotação
        preco_cotacao_entry = {
            "preco_desonerado": None,
            "preco_nao_desonerado": None,
            "data_cotacao": data_cotacao
        }

        preco_cotacao_entry[tipo_preco] = preco_valido

        # Buscar insumo existente
        insumo_existente = insumos_collection.find_one({
            "codigo": codigo,
            "nome": nome
        })

        if insumo_existente:
            # Verifica se já existe cotação para essa data
            precos = insumo_existente.get("precos_cotacao", [])
            for idx, cotacao in enumerate(precos):
                if cotacao.get("data_cotacao") == data_cotacao:
                    # Atualiza o campo do tipo de preço na cotação existente
                    insumos_collection.update_one(
                        {"_id": insumo_existente["_id"]},
                        {"$set": {f"precos_cotacao.{idx}.{tipo_preco}": preco_valido}}
                    )
                    break
            else:
                # Não encontrou cotação com essa data, então adiciona
                insumos_collection.update_one(
                    {"_id": insumo_existente["_id"]},
                    {"$push": {"precos_cotacao": preco_cotacao_entry}}
                )
        else:
            # Criar novo insumo
            novo_insumo = {
                "codigo": codigo,
                "nome": nome,
                "tipo": "SINAPI",
                "unidade_medida": unidade,
                "empresa": None,
                "precos_cotacao": [preco_cotacao_entry]
            }
            insumos_collection.insert_one(novo_insumo)

print("✅ Processamento finalizado com sucesso.")
client.close()
