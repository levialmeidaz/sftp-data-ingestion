# upsert_dw.py
import os, sys, psycopg2
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

CREATE_UNIQUE = r"""
CREATE UNIQUE INDEX IF NOT EXISTS ux_fat_pedidos_chave_nfe
  ON dw.fat_pedidos (chave_nfe);
"""

UPSERT_SQL = r"""
WITH src AS (
  SELECT
    NULLIF(TRIM(s.id),'')                              AS id,
    NULLIF(TRIM(s.pedido),'')                          AS pedido,
    CASE WHEN length(regexp_replace(s.chave_nfe,'\D','','g'))=44
         THEN regexp_replace(s.chave_nfe,'\D','','g')  ELSE NULL END AS chave_nfe,

    /* ---- DATE ---- */
    CASE
      WHEN btrim(s.data_nfe) IN ('', '00/00/0000', '00/00/0000 00:00:00', '0000-00-00') THEN NULL
      WHEN btrim(s.data_nfe) ~ '^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'
        THEN to_timestamp(CASE WHEN position(' ' in btrim(s.data_nfe))>0 THEN btrim(s.data_nfe) ELSE btrim(s.data_nfe)||' 00:00:00' END,'DD/MM/YYYY HH24:MI:SS')::date
      WHEN btrim(s.data_nfe) ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(s.data_nfe,'DD-MM-YYYY')
      WHEN btrim(s.data_nfe) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$'
        THEN to_timestamp(replace(btrim(s.data_nfe),'T',' '),'YYYY-MM-DD HH24:MI:SS')::date
      WHEN btrim(s.data_nfe) ~ '^\d{8}$' THEN to_date(s.data_nfe,'YYYYMMDD')
      ELSE NULL
    END AS data_nfe,

    CASE
      WHEN btrim(s.data_prev_entrega) IN ('', '00/00/0000', '00/00/0000 00:00:00', '0000-00-00') THEN NULL
      WHEN btrim(s.data_prev_entrega) ~ '^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'
        THEN to_timestamp(CASE WHEN position(' ' in btrim(s.data_prev_entrega))>0 THEN btrim(s.data_prev_entrega) ELSE btrim(s.data_prev_entrega)||' 00:00:00' END,'DD/MM/YYYY HH24:MI:SS')::date
      WHEN btrim(s.data_prev_entrega) ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(s.data_prev_entrega,'DD-MM-YYYY')
      WHEN btrim(s.data_prev_entrega) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$'
        THEN to_timestamp(replace(btrim(s.data_prev_entrega),'T',' '),'YYYY-MM-DD HH24:MI:SS')::date
      WHEN btrim(s.data_prev_entrega) ~ '^\d{8}$' THEN to_date(s.data_prev_entrega,'YYYYMMDD')
      ELSE NULL
    END AS data_prev_entrega,

    CASE
      WHEN btrim(s.data_prev_entrega_original) IN ('', '00/00/0000', '00/00/0000 00:00:00', '0000-00-00') THEN NULL
      WHEN btrim(s.data_prev_entrega_original) ~ '^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'
        THEN to_timestamp(CASE WHEN position(' ' in btrim(s.data_prev_entrega_original))>0 THEN btrim(s.data_prev_entrega_original) ELSE btrim(s.data_prev_entrega_original)||' 00:00:00' END,'DD/MM/YYYY HH24:MI:SS')::date
      WHEN btrim(s.data_prev_entrega_original) ~ '^\d{2}-\d{2}-\d{4}$' THEN to_date(s.data_prev_entrega_original,'DD-MM-YYYY')
      WHEN btrim(s.data_prev_entrega_original) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$'
        THEN to_timestamp(replace(btrim(s.data_prev_entrega_original),'T',' '),'YYYY-MM-DD HH24:MI:SS')::date
      WHEN btrim(s.data_prev_entrega_original) ~ '^\d{8}$' THEN to_date(s.data_prev_entrega_original,'YYYYMMDD')
      ELSE NULL
    END AS data_prev_entrega_original,

    /* ---- TIMESTAMP ---- */
    CASE
      WHEN btrim(s.data_ultima_ocr) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
        THEN to_timestamp(btrim(s.data_ultima_ocr),'DD/MM/YYYY HH24:MI:SS')
      WHEN btrim(s.data_ultima_ocr) ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?(\.\d+)?(Z|[+-]\d{2}:?\d{2})?$'
        THEN to_timestamp(replace(btrim(s.data_ultima_ocr),'T',' '),'YYYY-MM-DD HH24:MI:SS')
      WHEN btrim(s.data_ultima_ocr) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN to_timestamp(btrim(s.data_ultima_ocr)||' 00:00:00','DD/MM/YYYY HH24:MI:SS')
      ELSE NULL
    END                                               AS data_ultima_ocr_ts,

    CASE
      WHEN btrim(s.chegada_transportadora) ~ '^\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}$'
        THEN to_timestamp(btrim(s.chegada_transportadora),'DD/MM/YYYY HH24:MI:SS')
      WHEN btrim(s.chegada_transportadora) ~ '^\d{4}-\d{2}-\d{2}[ T]\d{2}:\d{2}(:\d{2})?$'
        THEN to_timestamp(replace(btrim(s.chegada_transportadora),'T',' '),'YYYY-MM-DD HH24:MI:SS')
      WHEN btrim(s.chegada_transportadora) ~ '^\d{2}/\d{2}/\d{4}$'
        THEN to_timestamp(btrim(s.chegada_transportadora)||' 00:00:00','DD/MM/YYYY HH24:MI:SS')
      ELSE NULL
    END                                               AS chegada_transportadora,

    NULLIF(TRIM(s.data_ultima_ocr),'')                AS data_ultima_ocr_raw,

    /* ---- NUMÉRICOS ---- */
    -- valor_nfe
    CASE
      WHEN s.valor_nfe IS NULL OR btrim(s.valor_nfe) = '' THEN NULL
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d{1,3}(\.\d{3})+,\d{1,2}$'
        THEN replace(replace(btrim(s.valor_nfe),'.',''),',','.')::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d{1,3}(,\d{3})+\.\d{1,2}$'
        THEN replace(btrim(s.valor_nfe),',','')::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d+,\d{1,2}$'
        THEN replace(btrim(s.valor_nfe),',','.')::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d+\.\d{1,2}$'
        THEN btrim(s.valor_nfe)::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d{1,3}(\.\d{3})+$'
        THEN replace(btrim(s.valor_nfe),'.','')::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d{1,3}(,\d{3})+$'
        THEN replace(btrim(s.valor_nfe),',','')::numeric(15,2)
      WHEN btrim(s.valor_nfe) ~ '^[+-]?\d+$'
        THEN btrim(s.valor_nfe)::numeric(15,2)
      ELSE CAST(replace(replace(regexp_replace(s.valor_nfe,'[^0-9,.-]','','g'),'.',''),',','.') AS numeric(15,2))
    END                                               AS valor_nfe,

    -- peso (corrigido)
    CASE
      WHEN s.peso IS NULL OR btrim(s.peso) = '' THEN NULL
      WHEN btrim(s.peso) ~ '^[+-]?\d{1,3}(\.\d{3})+,\d{1,3}$'
        THEN replace(replace(btrim(s.peso),'.',''),',','.')::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d{1,3}(,\d{3})+\.\d{1,3}$'
        THEN replace(btrim(s.peso),',','')::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d+,\d{1,3}$'
        THEN replace(btrim(s.peso),',','.')::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d+\.\d{1,3}$'
        THEN btrim(s.peso)::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d{1,3}(\.\d{3})+$'
        THEN replace(btrim(s.peso),'.','')::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d{1,3}(,\d{3})+$'
        THEN replace(btrim(s.peso),',','')::numeric(12,3)
      WHEN btrim(s.peso) ~ '^[+-]?\d+$'
        THEN btrim(s.peso)::numeric(12,3)
      ELSE CAST(replace(replace(regexp_replace(s.peso,'[^0-9,.-]','','g'),'.',''),',','.') AS numeric(12,3))
    END                                               AS peso,

    NULLIF(regexp_replace(s.qtd_volumes,'\D','','g'),'')::int AS qtd_volumes,
    NULLIF(regexp_replace(s.cod_cd,'\D','','g'),'')::int AS cod_cd,

    /* ---- TEXTOS ---- */
    NULLIF(TRIM(s.serie_nfe),'')                      AS serie_nfe,
    NULLIF(TRIM(s.numero_nfe),'')                     AS numero_nfe,
    NULLIF(TRIM(s.remessa),'')                        AS remessa,
    NULLIF(TRIM(s.nome_destinatario),'')              AS nome_destinatario,
    NULLIF(TRIM(s.endereco_completo),'')              AS endereco_completo,
    NULLIF(TRIM(s.cep),'')                            AS cep,
    NULLIF(TRIM(s.cd),'')                             AS cd,
    NULLIF(TRIM(regexp_replace(s.cnpj_cpf_transportadora,'\D','','g')),'') AS cnpj_cpf_transportadora,
    NULLIF(TRIM(s.transportador),'')                  AS transportador,
    NULLIF(TRIM(s.lead_time),'')                      AS lead_time,
    NULLIF(TRIM(s.status_prazo),'')                   AS status_prazo,
    NULLIF(TRIM(s.id_ult_ocr),'')                     AS id_ult_ocr,
    NULLIF(TRIM(s.ultima_ocorrencia),'')              AS ultima_ocorrencia,
    NULLIF(TRIM(s.chave_ult_ocr),'')                  AS chave_ult_ocr,
    NULLIF(TRIM(s.tipo_entrega),'')                   AS tipo_entrega,
    NULLIF(TRIM(s.agrupador),'')                      AS agrupador,
    NULLIF(TRIM(s.endereco),'')                       AS endereco,
    NULLIF(TRIM(s.numero),'')                         AS numero,
    NULLIF(TRIM(s.bairro),'')                         AS bairro,
    NULLIF(TRIM(s.cidades),'')                        AS cidades,
    CASE WHEN length(upper(regexp_replace(s.uf,'[^A-Za-z]','','g'))) BETWEEN 2 AND 3
         THEN upper(regexp_replace(s.uf,'[^A-Za-z]','','g')) ELSE NULL END AS uf,
    NULLIF(TRIM(s.etiquetas),'')                      AS etiquetas,
    NULLIF(TRIM(s.cod_vendedor),'')                   AS cod_vendedor,
    NULLIF(TRIM(s.qtd_itens),'')                      AS qtd_itens,
    NULLIF(TRIM(regexp_replace(s.cpf_destinatario,'\D','','g')),'') AS cpf_destinatario,
    NULLIF(TRIM(s.grau_risco),'')                     AS grau_risco,
    NULLIF(TRIM(s.tipo_operacao),'')                  AS tipo_operacao,
    NULLIF(TRIM(s.arquivo_origem),'')                 AS arquivo_origem,

    /* ---- CONTROLE ---- */
    COALESCE(
      CASE
        WHEN btrim(s.data_insercao) ~ '^\d{2}/\d{2}/\d{4}( \d{2}:\d{2}:\d{2})?$'
          THEN to_timestamp(CASE WHEN position(' ' in btrim(s.data_insercao))>0 THEN btrim(s.data_insercao) ELSE btrim(s.data_insercao)||' 00:00:00' END,'DD/MM/YYYY HH24:MI:SS')
        WHEN btrim(s.data_insercao) ~ '^\d{4}-\d{2}-\d{2}([ T]\d{2}:\d{2}(:\d{2})?)?$'
          THEN to_timestamp(replace(btrim(s.data_insercao),'T',' '),'YYYY-MM-DD HH24:MI:SS')
        ELSE NULL END,
      now()
    )                                                 AS data_insercao
  FROM staging.stg_pedidos s
),
ranked AS (
  SELECT s.*,
         row_number() OVER (
           PARTITION BY s.chave_nfe
           ORDER BY s.data_ultima_ocr_ts DESC NULLS LAST,
                    s.data_insercao     DESC NULLS LAST
         ) AS rn
  FROM src s
)
INSERT INTO dw.fat_pedidos (
  id, data_insercao, tipo_entrega, pedido, data_nfe, serie_nfe, numero_nfe, valor_nfe,
  qtd_volumes, peso, remessa, nome_destinatario, endereco_completo, cep, cod_cd, cd,
  cnpj_cpf_transportadora, transportador, lead_time, data_prev_entrega, status_prazo,
  id_ult_ocr, ultima_ocorrencia, chave_ult_ocr, data_ultima_ocr, agrupador, endereco,
  numero, bairro, cidades, uf, etiquetas, chegada_transportadora, cod_vendedor,
  chave_nfe, qtd_itens, data_prev_entrega_original, cpf_destinatario, grau_risco,
  tipo_operacao, arquivo_origem
)
SELECT
  r.id, r.data_insercao, r.tipo_entrega, r.pedido, r.data_nfe, r.serie_nfe, r.numero_nfe, r.valor_nfe,
  r.qtd_volumes, r.peso, r.remessa, r.nome_destinatario, r.endereco_completo, r.cep, r.cod_cd, r.cd,
  r.cnpj_cpf_transportadora, r.transportador, r.lead_time, r.data_prev_entrega, r.status_prazo,
  r.id_ult_ocr, r.ultima_ocorrencia, r.chave_ult_ocr, r.data_ultima_ocr_ts, r.agrupador, r.endereco,
  r.numero, r.bairro, r.cidades, r.uf, r.etiquetas, r.chegada_transportadora, r.cod_vendedor,
  r.chave_nfe, r.qtd_itens, r.data_prev_entrega_original, r.cpf_destinatario, r.grau_risco,
  r.tipo_operacao, r.arquivo_origem
FROM ranked r
WHERE r.chave_nfe IS NOT NULL
  AND r.rn = 1
ON CONFLICT (chave_nfe) DO UPDATE
SET
  data_ultima_ocr = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.data_ultima_ocr
    ELSE dw.fat_pedidos.data_ultima_ocr END,

  data_prev_entrega = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.data_prev_entrega ELSE dw.fat_pedidos.data_prev_entrega END,
  status_prazo      = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.status_prazo      ELSE dw.fat_pedidos.status_prazo      END,
  id_ult_ocr        = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.id_ult_ocr        ELSE dw.fat_pedidos.id_ult_ocr        END,
  ultima_ocorrencia = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.ultima_ocorrencia ELSE dw.fat_pedidos.ultima_ocorrencia END,
  chave_ult_ocr     = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.chave_ult_ocr     ELSE dw.fat_pedidos.chave_ult_ocr     END,
  chegada_transportadora = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.chegada_transportadora ELSE dw.fat_pedidos.chegada_transportadora END,
  arquivo_origem    = CASE WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.arquivo_origem    ELSE dw.fat_pedidos.arquivo_origem    END,

  data_insercao = GREATEST(dw.fat_pedidos.data_insercao, EXCLUDED.data_insercao),
  valor_nfe     = COALESCE(EXCLUDED.valor_nfe, dw.fat_pedidos.valor_nfe),
  qtd_volumes   = COALESCE(EXCLUDED.qtd_volumes, dw.fat_pedidos.qtd_volumes),
  peso          = COALESCE(EXCLUDED.peso, dw.fat_pedidos.peso),
  cod_cd        = COALESCE(EXCLUDED.cod_cd, dw.fat_pedidos.cod_cd),

  id                  = COALESCE(EXCLUDED.id, dw.fat_pedidos.id),
  tipo_entrega        = COALESCE(EXCLUDED.tipo_entrega, dw.fat_pedidos.tipo_entrega),
  pedido              = COALESCE(EXCLUDED.pedido, dw.fat_pedidos.pedido),
  serie_nfe           = COALESCE(EXCLUDED.serie_nfe, dw.fat_pedidos.serie_nfe),
  numero_nfe          = COALESCE(EXCLUDED.numero_nfe, dw.fat_pedidos.numero_nfe),
  remessa             = COALESCE(EXCLUDED.remessa, dw.fat_pedidos.remessa),
  nome_destinatario   = COALESCE(EXCLUDED.nome_destinatario, dw.fat_pedidos.nome_destinatario),
  endereco_completo   = COALESCE(EXCLUDED.endereco_completo, dw.fat_pedidos.endereco_completo),
  cep                 = COALESCE(EXCLUDED.cep, dw.fat_pedidos.cep),
  cd                  = COALESCE(EXCLUDED.cd, dw.fat_pedidos.cd),
  cnpj_cpf_transportadora = COALESCE(EXCLUDED.cnpj_cpf_transportadora, dw.fat_pedidos.cnpj_cpf_transportadora),
  transportador       = COALESCE(EXCLUDED.transportador, dw.fat_pedidos.transportador),
  lead_time           = COALESCE(EXCLUDED.lead_time, dw.fat_pedidos.lead_time),
  agrupador           = COALESCE(EXCLUDED.agrupador, dw.fat_pedidos.agrupador),
  endereco            = COALESCE(EXCLUDED.endereco, dw.fat_pedidos.endereco),
  numero              = COALESCE(EXCLUDED.numero, dw.fat_pedidos.numero),
  bairro              = COALESCE(EXCLUDED.bairro, dw.fat_pedidos.bairro),
  cidades             = COALESCE(EXCLUDED.cidades, dw.fat_pedidos.cidades),
  uf                  = COALESCE(EXCLUDED.uf, dw.fat_pedidos.uf),
  etiquetas           = COALESCE(EXCLUDED.etiquetas, dw.fat_pedidos.etiquetas),
  cod_vendedor        = COALESCE(EXCLUDED.cod_vendedor, dw.fat_pedidos.cod_vendedor),
  qtd_itens           = COALESCE(EXCLUDED.qtd_itens, dw.fat_pedidos.qtd_itens),
  cpf_destinatario    = COALESCE(EXCLUDED.cpf_destinatario, dw.fat_pedidos.cpf_destinatario),
  grau_risco          = COALESCE(EXCLUDED.grau_risco, dw.fat_pedidos.grau_risco),
  tipo_operacao       = COALESCE(EXCLUDED.tipo_operacao, dw.fat_pedidos.tipo_operacao)
;
"""

def run_upsert():
    load_env()
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
        port=os.getenv("PGPORT"),
        options="-c search_path=public,staging,dw"
    )
    try:
        with conn, conn.cursor() as cur:
            cur.execute(CREATE_UNIQUE)
            cur.execute(UPSERT_SQL)
    finally:
        conn.close()

if __name__ == "__main__":
    try:
        run_upsert()
        print("Upsert concluído.")
    except Exception as e:
        print(f"Erro ao executar upsert: {e}", file=sys.stderr)
        sys.exit(1)