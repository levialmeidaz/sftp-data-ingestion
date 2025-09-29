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
from datetime import datetime

# ===== CONFIG =====
DIR_NOVOS  = r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\novos"
DIR_LIDOS  = r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\lidos"
DIR_ERROS  = r"C:\Users\atend\OneDrive - grupojb.log.br\STORAGE_SFTP\rel_83\erros"
TABELA_DESTINO = "staging.stg_pedidos"

# Credenciais via .env (ex.: banco.env no mesmo diretório do script)
# Carrega .env a partir do diretório do script, não do CWD
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
ENV_PATH = os.path.join(BASE_DIR, "banco.env")
load_dotenv(ENV_PATH)

def _req(key: str) -> str:
    v = os.getenv(key)
    if not v:
        raise RuntimeError(f"Variável de ambiente ausente: {key}. Verifique {ENV_PATH}")
    return v

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

def safe_copy(src: str, dst_dir: str) -> str:
    os.makedirs(dst_dir, exist_ok=True)
    base = os.path.basename(src)
    dst  = os.path.join(dst_dir, base)
    if os.path.exists(dst):
        ts = datetime.now().strftime("%Y%m%d%H%M%S")
        name, ext = os.path.splitext(base)
        dst = os.path.join(dst_dir, f"{name}__dup_{ts}{ext}")
    shutil.copy2(src, dst)
    return dst

def listar_csv_novos(dir_novos: str, dir_lidos: str, dir_erros: str) -> List[str]:
    os.makedirs(dir_lidos, exist_ok=True)
    os.makedirs(dir_erros, exist_ok=True)
    lidos = {os.path.basename(p).lower() for p in glob(os.path.join(dir_lidos, "*.csv"))}
    err   = {os.path.basename(p).lower() for p in glob(os.path.join(dir_erros, "*.csv"))}
    candidatos = glob(os.path.join(dir_novos, "*.csv"))
    return [p for p in candidatos if os.path.basename(p).lower() not in lidos | err]

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
    r"""Tenta cp1252/latin-1/utf-8-sig/utf-8. Detecta separador. Normaliza nº de colunas."""
    encodings = ["cp1252", "latin-1", "utf-8-sig", "utf-8"]
    for enc in encodings:
        try:
            sep = detectar_sep_por_frequencia(caminho, enc)
            with open(caminho, "r", encoding=enc, errors="replace", newline="") as f:
                reader = csv.reader(
                    f, delimiter=sep, quotechar='"', doublequote=True,
                    escapechar="\\", strict=False
                )
                rows = [r for r in reader]
        except Exception:
            continue

        rows = [r for r in rows if any(str(cell).strip() != "" for cell in r)]
        if not rows:
            return pd.DataFrame()  # vazio

        header = [h.strip().replace("\ufeff", "") for h in rows[0]]
        n_cols = len(header)
        if n_cols == 0:
            return pd.DataFrame()

        norm = []
        for r in rows[1:]:
            if len(r) > n_cols:
                r = r[: n_cols - 1] + [sep.join(r[n_cols - 1 :])]
            elif len(r) < n_cols:
                r = r + [""] * (n_cols - len(r))
            norm.append(r)

        df = pd.DataFrame(norm, columns=header, dtype=str).fillna("")
        return df

    return pd.DataFrame()

def header_valido(df_original: pd.DataFrame) -> bool:
    if df_original is None or df_original.empty:
        return False
    presentes = sum(1 for c in df_original.columns if c.strip().replace("\ufeff","") in DE_PARA)
    return presentes >= 10  # ajuste se necessário

def aplicar_mapeamento(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    df.columns = [c.strip().replace("\ufeff", "") for c in df.columns]
    df = df.rename(columns={c: DE_PARA[c] for c in df.columns if c in DE_PARA})
    for col in COLUNAS_DESTINO:
        if col != "arquivo_origem" and col not in df.columns:
            df[col] = ""
    return df[[c for c in COLUNAS_DESTINO if c != "arquivo_origem"]]

def inserir_copy(conn, tabela: str, df: pd.DataFrame, arquivo_origem: str) -> int:
    if df.empty:
        return 0
    linhas = len(df)
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
    return linhas

# ========= PIPELINE =========

def processar():
    novos = listar_csv_novos(DIR_NOVOS, DIR_LIDOS, DIR_ERROS)
    if not novos:
        print("Nenhum arquivo novo para processar.")
        return

    with psycopg2.connect(**DB_CFG) as conn:
        for caminho in novos:
            try:
                print(f"Lendo: {caminho}")
                df_raw = ler_csv_robusto(caminho)

                if not header_valido(df_raw):
                    print("Arquivo vazio ou cabeçalho inesperado. Enviando para 'erros'.")
                    dst = safe_copy(caminho, DIR_ERROS)
                    print(f"Copiado para erros: {dst}")
                    continue

                df = aplicar_mapeamento(df_raw)
                inseridas = inserir_copy(conn, TABELA_DESTINO, df, caminho)
                print(f"Linhas inseridas: {inseridas}")

                if inseridas > 0:
                    dst = safe_copy(caminho, DIR_LIDOS)
                    print(f"Copiado para lidos: {dst}")
                else:
                    print("0 linhas inseridas. Enviando para 'erros'.")
                    dst = safe_copy(caminho, DIR_ERROS)
                    print(f"Copiado para erros: {dst}")

            except Exception as e:
                print(f"Falha ao processar. Enviando para 'erros'. Motivo: {e}")
                dst = safe_copy(caminho, DIR_ERROS)
                print(f"Copiado para erros: {dst}")

if __name__ == "__main__":
    processar()
