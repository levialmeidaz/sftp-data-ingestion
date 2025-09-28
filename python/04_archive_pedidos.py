# move_staging_to_archive_safe.py
import os, sys, uuid, time
import psycopg2
from psycopg2.extras import DictCursor
from dotenv import load_dotenv

ENV_PATH = r"C:\Users\atend\OneDrive\Área de Trabalho\git_jb\sftp-data-ingestion\.env\banco.env"

def load_env():
    ok = load_dotenv(ENV_PATH, override=True)
    if not ok:
        raise RuntimeError(f"Não encontrei o .env em: {ENV_PATH}")
    required = ["PGHOST","PGUSER","PGPASSWORD","PGDATABASE","PGPORT"]
    missing = [k for k in required if not os.getenv(k)]
    if missing:
        raise RuntimeError(f"Variáveis ausentes: {', '.join(missing)}")

COLS = [
    "id","data_insercao","tipo_entrega","pedido","data_nfe","serie_nfe","numero_nfe",
    "valor_nfe","qtd_volumes","peso","remessa","nome_destinatario","endereco_completo",
    "cep","cod_cd","cd","cnpj_cpf_transportadora","transportador","lead_time",
    "data_prev_entrega","status_prazo","id_ult_ocr","ultima_ocorrencia","chave_ult_ocr",
    "data_ultima_ocr","agrupador","endereco","numero","bairro","cidades","uf","etiquetas",
    "chegada_transportadora","cod_vendedor","chave_nfe","qtd_itens","data_prev_entrega_original",
    "cpf_destinatario","grau_risco","tipo_operacao","arquivo_origem"
]
COL_LIST = ", ".join(COLS)
SRC_LIST = ", ".join(f"s.{c}" for c in COLS)

SQL_BATCH_MOVE_WITH_CTRL = f"""
WITH to_move AS (
  SELECT ctid
  FROM staging.stg_pedidos
  LIMIT %(batch_size)s
),
ins AS (
  INSERT INTO hist.archive_pedidos (processed_ts, batch_id, {COL_LIST})
  SELECT now(), %(batch_id)s, {SRC_LIST}
  FROM staging.stg_pedidos s
  JOIN to_move t ON t.ctid = s.ctid
  RETURNING 1
),
del AS (
  DELETE FROM staging.stg_pedidos s
  USING to_move t
  WHERE s.ctid = t.ctid
  RETURNING 1
)
SELECT (SELECT count(*) FROM ins) AS inserted,
       (SELECT count(*) FROM del) AS deleted;
"""

def move_to_archive_safe(batch_size=5000, use_control_columns=True, lock_wait_ms=3000, stmt_timeout_ms=900000):
    """
    Move staging -> hist em lotes. Retorna (total_inserted, total_deleted, batch_id).
    - lock não bloqueante
    - lock_timeout e statement_timeout configurados
    """
    load_env()
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
        port=os.getenv("PGPORT"),
        options="-c search_path=public,staging,hist"
    )
    batch_id = str(uuid.uuid4())

    try:
        with conn:
            with conn.cursor(cursor_factory=DictCursor) as cur:
                # Identificação e timeouts
                cur.execute("SET application_name = 'move_staging_to_archive_safe'")
                cur.execute("SET LOCAL lock_timeout = %s", (f"{lock_wait_ms}ms",))
                cur.execute("SET LOCAL statement_timeout = %s", (f"{stmt_timeout_ms}ms",))

                # Tenta lock advisory sem bloquear indefinidamente
                cur.execute("SELECT pg_try_advisory_xact_lock(hashtext('move_staging_to_archive'))")
                got = cur.fetchone()[0]
                if not got:
                    raise RuntimeError("Outro processo está movendo staging -> histórico. Abortado sem aguardar.")

                total_ins = total_del = 0
                while True:
                    if use_control_columns:
                        cur.execute(SQL_BATCH_MOVE_WITH_CTRL, {
                            "batch_id": batch_id,
                            "batch_size": batch_size
                        })
                    else:
                        # Se sua hist NÃO tiver processed_ts/batch_id, adapte a SQL removendo essas colunas
                        raise NotImplementedError("Ajuste a SQL para cenário sem colunas de controle.")

                    inserted, deleted = cur.fetchone()
                    total_ins += int(inserted or 0)
                    total_del += int(deleted or 0)

                    if inserted == 0 and deleted == 0:
                        break  # staging esvaziada
                return total_ins, total_del, batch_id
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        ins, dele, bid = move_to_archive_safe()
        print(f"OK. inserted={ins}, deleted={dele}, batch_id={bid}")
    except Exception as e:
        print(f"Falha: {e}", file=sys.stderr)
        sys.exit(1)
