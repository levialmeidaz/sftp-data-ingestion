# arquivo: carga_pedidos_csv.py
import os
import io
import csv
import shutil
from glob import glob
from typing import List
import pandas as pd
import psycopg2
from dotenv import load_dotenv

# ===== CONFIG =====
DIR_NOVOS = r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\novos"
DIR_LIDOS = r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\lidos"
TABELA_DESTINO = "stagging.stg_pedidos"

# Credenciais via .env (ex.: banco.env no mesmo diretório do script)
load_dotenv("banco.env")
DB_CFG = {
    "host": os.getenv("PGHOST", "localhost"),
    "user": os.getenv("PGUSER", "postgres"),
    "password": os.getenv("PGPASSWORD", "admin"),
    "dbname": os.getenv("PGDATABASE", "banco_prod"),
    "port": int(os.getenv("PGPORT", "5432")),
}

COLUNAS_DESTINO = [
    "id","data_insercao","tipo_entrega","pedido","data_nfe","serie_nfe","numero_nfe","valor_nfe",
    "qtd_volumes","peso","remessa","nome_destinatario","endereco_completo","cep","cod_cd","cd",
    "cnpj_cpf_transportadora","transportador","lead_time","data_prev_entrega","status_prazo",
    "id_ult_ocr","ultima_ocorrencia","chave_ult_ocr","data_ultima_ocr","agrupador",
    "endereco","numero","bairro","cidades","uf","etiquetas","chegada_transportadora",
    "cod_vendedor","chave_nfe","qtd_itens","data_prev_entrega_original","cpf_destinatario",
    "grau_risco","tipo_operacao","arquivo_origem"
]

# DE->PARA: cabeçalhos originais -> snake_case da tabela
DE_PARA = {
    "ID": "id",
    "Data Inserção": "data_insercao",
    "Tipo Entrega": "tipo_entrega",
    "Pedido": "pedido",
    "Data Nfe": "data_nfe",
    "Serie Nfe": "serie_nfe",
    "Número Nfe": "numero_nfe",
    "Valor Nfe": "valor_nfe",
    "Qtd. Volumes": "qtd_volumes",
    "Peso": "peso",
    "Remessa": "remessa",
    "Nome Destinatário": "nome_destinatario",
    "Endereço Completo": "endereco_completo",
    "CEP": "cep",
    "Cód. CD": "cod_cd",
    "CD": "cd",
    "CNPJ/CPF Transportadora": "cnpj_cpf_transportadora",
    "Transportador": "transportador",
    "Lead Time": "lead_time",
    "Data Prev. Entrega": "data_prev_entrega",
    "Status Prazo": "status_prazo",
    "ID Últ. Ocr.": "id_ult_ocr",
    "Última Ocorrência": "ultima_ocorrencia",
    "Chave Últ. Ocr.": "chave_ult_ocr",
    "Data Última Ocr.": "data_ultima_ocr",
    "Agrupador": "agrupador",
    "Endereço": "endereco",
    "Numero": "numero",
    "Bairro": "bairro",
    "Cidades": "cidades",
    "UF": "uf",
    "Etiquetas": "etiquetas",
    "Chegada na Transportadora": "chegada_transportadora",
    "Cod. Vendedor": "cod_vendedor",
    "Chave NFe": "chave_nfe",
    "Qtd. Itens": "qtd_itens",
    "Data Prev. Entrega (Original)": "data_prev_entrega_original",
    "CPF Destinatário": "cpf_destinatario",
    "Grau de Risco": "grau_risco",
    "Tipo de Operação": "tipo_operacao",
}

# ========= UTIL =========

def listar_csv_novos(dir_novos: str, dir_lidos: str) -> List[str]:
    os.makedirs(dir_lidos, exist_ok=True)
    lidos = {os.path.basename(p).lower() for p in glob(os.path.join(dir_lidos, "*.csv"))}
    candidatos = glob(os.path.join(dir_novos, "*.csv"))
    return [p for p in candidatos if os.path.basename(p).lower() not in lidos]

def detectar_sep_por_frequencia(caminho: str, encoding: str) -> str:
    candidatos = [",", ";", "|", "\t"]
    contagem = {c: 0 for c in candidatos}
    with open(caminho, "r", encoding=encoding, errors="replace", newline="") as f:
        for i, linha in enumerate(f):
            if i > 200:
                break
            for c in candidatos:
                contagem[c] += linha.count(c)
    return max(contagem, key=contagem.get) or ","

def ler_csv_robusto(caminho: str) -> pd.DataFrame:
    r"""Leitor robusto que:
    - tenta encodings cp1252/latin-1/utf-8-sig/utf-8
    - detecta separador por frequência
    - tolera linhas com colunas a mais/menos
    - preserva literais como \N e strings vazias
    - retorna DataFrame vazio se não houver dados (arquivo vazio ou só espaços)
    """
    encodings = ["cp1252", "latin-1", "utf-8-sig", "utf-8"]
    for enc in encodings:
        try:
            sep = detectar_sep_por_frequencia(caminho, enc)
            with open(caminho, "r", encoding=enc, errors="replace", newline="") as f:
                reader = csv.reader(
                    f,
                    delimiter=sep,
                    quotechar='"',
                    doublequote=True,
                    escapechar="\\",
                    strict=False,
                )
                rows = [r for r in reader]
        except Exception:
            continue

        # remove linhas totalmente vazias
        rows = [r for r in rows if any(cell is not None and str(cell).strip() != "" for cell in r)]
        if not rows:
            return pd.DataFrame()  # vazio

        header = [h.strip().replace("\ufeff", "") for h in rows[0]]
        n_cols = len(header)

        # se header parece 1 coluna mas linhas tem mais, tenta re-sniff em outra base
        if n_cols == 1 and len(rows) > 1 and len(rows[1]) > 1:
            continue  # tenta próximo encoding

        norm_rows = []
        for r in rows[1:]:
            if len(r) > n_cols:
                r = r[: n_cols - 1] + [sep.join(r[n_cols - 1 :])]
            elif len(r) < n_cols:
                r = r + [""] * (n_cols - len(r))
            norm_rows.append(r)

        df = pd.DataFrame(norm_rows, columns=header, dtype=str).fillna("")
        return df

    # todos encodings falharam ou arquivo sem conteúdo útil
    return pd.DataFrame()

def aplicar_mapeamento(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df  # nada a mapear
    df.columns = [c.strip().replace("\ufeff", "") for c in df.columns]
    col_renome = {c: DE_PARA[c] for c in df.columns if c in DE_PARA}
    df = df.rename(columns=col_renome)
    for col in COLUNAS_DESTINO:
        if col != "arquivo_origem" and col not in df.columns:
            df[col] = ""
    colunas_final = [c for c in COLUNAS_DESTINO if c != "arquivo_origem"]
    return df[colunas_final]

def inserir_copy(conn, tabela: str, df: pd.DataFrame, arquivo_origem: str):
    if df.empty:
        return  # nada a inserir
    df = df.copy()
    df["arquivo_origem"] = os.path.basename(arquivo_origem)
    df = df.astype(str)

    buf = io.StringIO()
    df.to_csv(buf, index=False, header=False, quoting=csv.QUOTE_ALL)
    buf.seek(0)

    cols_sql = ", ".join(COLUNAS_DESTINO)
    sql = f"COPY {tabela} ({cols_sql}) FROM STDIN WITH (FORMAT csv)"
    with conn.cursor() as cur:
        cur.copy_expert(sql, buf)
    conn.commit()

# ========= PIPELINE =========

def processar():
    novos = listar_csv_novos(DIR_NOVOS, DIR_LIDOS)
    if not novos:
        print("Nenhum arquivo novo para processar.")
        return

    with psycopg2.connect(**DB_CFG) as conn:
        for caminho in novos:
            print(f"Lendo: {caminho}")
            df = ler_csv_robusto(caminho)
            if df.empty:
                print("Arquivo vazio ou sem dados úteis. Pulando carga.")
            else:
                df = aplicar_mapeamento(df)
                inserir_copy(conn, TABELA_DESTINO, df, caminho)

            # copiar para lidos sempre
            destino = os.path.join(DIR_LIDOS, os.path.basename(caminho))
            shutil.copy2(caminho, destino)
            print(f"Copiado para lidos: {destino}")

if __name__ == "__main__":
    processar()
