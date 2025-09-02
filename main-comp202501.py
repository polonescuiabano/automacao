import pandas as pd
from pymongo import MongoClient
from datetime import datetime
import openpyxl
import re


# Conexão com o MongoDB
client = MongoClient(
    "mongodb://admin:3ng3nh4r1427611CPLUSPLUSOP@serorc.serpra.com.br:27017/serorc?authSource=admin"
)
db = client['serorc']
composicoes_collection = db['composicoes']
insumos_collection = db['insumos']  # Nome da coleção 'insumos'

# Caminho do arquivo
excel_path = r"C:\Users\Dell\Downloads\202501\SINAPI_Referência_2025_01.xlsx"

# Data da nova cotação
data_cotacao = datetime(2025, 1, 1)

novas_composicoes = []

# Abrir o workbook com openpyxl para extrair valores de fórmulas/hiperlinks
wb = openpyxl.load_workbook(excel_path, data_only=True)

abas = {
    'CCD': 'preco_desonerado',
    'CSD': 'preco_nao_desonerado'
}

for aba, tipo_preco in abas.items():
    print(f"📄 Processando aba: {aba}")

    df = pd.read_excel(excel_path, sheet_name=aba, header=None, engine='openpyxl')
    ws = wb[aba]

    # Linha 9 (índice 8) contém os nomes dos estados
    linha_estados = df.iloc[8]
    colunas_mt = [i for i, val in linha_estados.items() if str(val).strip().upper() == 'MT']

    if not colunas_mt:
        print(f"⚠️ Nenhuma coluna MT encontrada na aba {aba}. Pulando.")
        continue

    for i in range(10, len(df)):
        row = df.iloc[i]

        codigo_cell = ws.cell(row=i + 1, column=2)  # Coluna B = 2
        codigo_val = codigo_cell.value

        # Tenta extrair o valor do HYPERLINK se necessário
        if not isinstance(codigo_val, (int, float)):
            if codigo_cell.data_type == 'f' and isinstance(codigo_cell.value, str):
                formula = codigo_cell.value
                match = re.search(r',\s*(\d+)\s*\)?$', formula)
                if match:
                    codigo_val = match.group(1)

        # Validações adicionais
        if codigo_val in [None, '', '-', '–', 0, '0']:
            continue

        try:
            codigo = str(codigo_val).strip()
            if codigo == "0":
                continue
        except:
            continue

        descricao = str(row[2]).strip()
        unidade = str(row[3]).strip()

        if pd.isna(descricao) or pd.isna(codigo):
            continue

        preco_valido = None
        for col in colunas_mt:
            preco_raw = row[col]

            if pd.isna(preco_raw):  # ignora se for NaN
                continue

            try:
                preco_str = str(preco_raw).strip()
                if preco_str in ["", "-", "–", "0"]:  # ignora se for string vazia, traço ou zero
                    continue

                preco_float = float(preco_str)
                if preco_float == 0:  # ignora valores zero numéricos
                    continue

                preco_valido = preco_float
                break
            except:
                continue

        # Se nenhum valor válido foi encontrado, pula a linha
        if preco_valido is None:
            continue

        preco_entry = {
            "preco_desonerado": preco_valido if tipo_preco == 'preco_desonerado' else None,
            "preco_nao_desonerado": preco_valido if tipo_preco == 'preco_nao_desonerado' else None,
            "data_cotacao": data_cotacao
        }

        composicao_existente = composicoes_collection.find_one({
            "codigo": codigo,
            "descricao": descricao
        })

        if composicao_existente:
            cotacoes = composicao_existente.get("precos_cotacao", [])
            for idx, cotacao in enumerate(cotacoes):
                if cotacao.get("data_cotacao") == data_cotacao:
                    # Atualiza somente o campo correspondente (desonerado ou não desonerado)
                    campo_update = f"precos_cotacao.{idx}.{tipo_preco}"
                    composicoes_collection.update_one(
                        {"_id": composicao_existente["_id"]},
                        {"$set": {campo_update: preco_valido}}
                    )
                    break
            else:
                # Não encontrou data_cotacao existente → adiciona nova entrada
                composicoes_collection.update_one(
                    {"_id": composicao_existente["_id"]},
                    {"$push": {"precos_cotacao": preco_entry}}
                )

        else:
            nova_composicao = {
                "tipo": "SINAPI",
                "codigo": codigo,
                "descricao": descricao,
                "unidade_medida": unidade,
                "descricao_classe": None,
                "sigla_classe": None,
                "empresa": None,
                "precos_cotacao": [preco_entry],
                "composicoes_auxiliares": [],
                "insumos": []
            }
            composicoes_collection.insert_one(nova_composicao)
            novas_composicoes.append({"codigo":nova_composicao["codigo"], "descricao": nova_composicao["descricao"]})
            print(novas_composicoes[0])
print("✅ Processamento de composições finalizado com sucesso.")

df_analitico = pd.read_excel(excel_path, sheet_name="Analítico", header=None, engine='openpyxl')

for comp in novas_composicoes:
    codigo_principal = str(comp["codigo"]).strip()

    # Filtra todas as linhas que têm esse código na coluna B (índice 1)
    linhas_relacionadas = df_analitico[df_analitico[1] == codigo_principal]

    composicoes_auxiliares = []
    insumos = []

    for _, linha in linhas_relacionadas.iterrows():
        tipo = linha[2]  # coluna C (índice 2)
        if pd.isna(tipo) or str(tipo).strip() == '':
            continue  # pula linhas sem tipo definido

        tipo = str(tipo).strip().upper()
        codigo_item = linha[3]  # coluna D (índice 3)
        coeficiente = linha[6]  # coluna G (índice 6)

        if pd.isna(codigo_item) or pd.isna(coeficiente):
            continue  # pula se faltar dados importantes

        try:
            codigo_item = str(codigo_item).strip()
            print(codigo_item)
            coeficiente = float(str(coeficiente))
        except:
            continue  # se falhar na conversão, pula

        if tipo == "COMPOSICAO":
            composicoes_auxiliares.append({
                "codigo": codigo_item,
                "coeficiente": coeficiente
            })
        elif tipo == "INSUMO":
            insumo=insumos_collection.find_one({"codigo": codigo_item})
            if insumo:
                insumos.append({
                    "insumo_id": insumo["_id"],
                    "coeficiente": coeficiente
                })
    print(composicoes_auxiliares[0])
    print(insumos[0])


    # Atualiza o documento da composição com os insumos e composicoes auxiliares encontrados
    composicao_doc = composicoes_collection.find_one({"codigo": codigo_principal})
    if composicao_doc:
        composicoes_collection.update_one(
            {"_id": composicao_doc["_id"]},
            {"$set": {
                "composicoes_auxiliares": composicoes_auxiliares,
                "insumos": insumos if insumos else None
            }}
        )
        print(f"Atualizado composição {codigo_principal} com {len(composicoes_auxiliares)} composicoes auxiliares e {len(insumos)} insumos.")
    else:
        print(f"⚠️ Composição {codigo_principal} não encontrada para atualizar insumos/composições.")

print("✅ Processamento de insumos e composições auxiliares finalizado.")

client.close()
