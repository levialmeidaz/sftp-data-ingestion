# upsert_pedidos.py
import os
import sys
import psycopg2
from psycopg2 import sql
from dotenv import load_dotenv

load_dotenv("../.env/banco.env")

UPSERT_SQL = """
WITH src AS (
  SELECT
    NULLIF(TRIM(s.pedido),'')                                               AS pedido,
    CASE
      WHEN length(regexp_replace(s.chave_nfe, '\\D', '', 'g')) = 44
      THEN regexp_replace(s.chave_nfe, '\\D', '', 'g')::char(44)
      ELSE NULL
    END                                                                     AS chave_nfe,

    -- datas (DATE)
    CASE
      WHEN s.data_nfe ~ '^\\d{4}-\\d{2}-\\d{2}$'           THEN to_date(s.data_nfe,'YYYY-MM-DD')
      WHEN s.data_nfe ~ '^\\d{2}/\\d{2}/\\d{4}$'           THEN to_date(s.data_nfe,'DD/MM/YYYY')
      ELSE NULL
    END                                                                     AS data_nfe,

    CASE
      WHEN s.data_prev_entrega ~ '^\\d{4}-\\d{2}-\\d{2}$'  THEN to_date(s.data_prev_entrega,'YYYY-MM-DD')
      WHEN s.data_prev_entrega ~ '^\\d{2}/\\d{2}/\\d{4}$'  THEN to_date(s.data_prev_entrega,'DD/MM/YYYY')
      ELSE NULL
    END                                                                     AS data_prev_entrega,

    CASE
      WHEN s.data_prev_entrega_original ~ '^\\d{4}-\\d{2}-\\d{2}$' THEN to_date(s.data_prev_entrega_original,'YYYY-MM-DD')
      WHEN s.data_prev_entrega_original ~ '^\\d{2}/\\d{2}/\\d{4}$' THEN to_date(s.data_prev_entrega_original,'DD/MM/YYYY')
      ELSE NULL
    END                                                                     AS data_prev_entrega_original,

    -- timestamps
    CASE
      WHEN s.data_ultima_ocr ~ '^\\d{4}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(:\\d{2})?)?$'
        THEN to_timestamp(replace(s.data_ultima_ocr,'T',' '), 'YYYY-MM-DD HH24:MI:SS')
      WHEN s.data_ultima_ocr ~ '^\\d{2}/\\d{2}/\\d{4}(?: \\d{2}:\\d{2}(:\\d{2})?)?$'
        THEN to_timestamp(s.data_ultima_ocr, 'DD/MM/YYYY HH24:MI:SS')
      ELSE NULL
    END                                                                     AS data_ultima_ocr,

    CASE
      WHEN s.chegada_transportadora ~ '^\\d{4}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(:\\d{2})?)?$'
        THEN to_timestamp(replace(s.chegada_transportadora,'T',' '), 'YYYY-MM-DD HH24:MI:SS')
      WHEN s.chegada_transportadora ~ '^\\d{2}/\\d{2}/\\d{4}(?: \\d{2}:\\d{2}(:\\d{2})?)?$'
        THEN to_timestamp(s.chegada_transportadora, 'DD/MM/YYYY HH24:MI:SS')
      ELSE NULL
    END                                                                     AS chegada_transportadora,

    -- números e valores
    NULLIF(TRIM(s.serie_nfe),'')::varchar(10)                                AS serie_nfe,
    NULLIF(TRIM(s.numero_nfe),'')::bigint                                    AS numero_nfe,
    CASE
      WHEN s.valor_nfe IS NULL OR btrim(s.valor_nfe) = '' THEN NULL
      ELSE CAST(replace(replace(regexp_replace(s.valor_nfe,'[^0-9,.-]','','g'),'.',''),',','.') AS numeric(15,2))
    END                                                                     AS valor_nfe,
    NULLIF(TRIM(s.qtd_volumes),'')::integer                                  AS qtd_volumes,
    NULLIF(TRIM(s.qtd_itens),'')::integer                                    AS qtd_itens,
    CASE
      WHEN s.peso IS NULL OR btrim(s.peso) = '' THEN NULL
      ELSE CAST(replace(replace(regexp_replace(s.peso,'[^0-9,.-]','','g'),'.',''),',','.') AS numeric(12,3))
    END                                                                     AS peso,
    NULLIF(TRIM(s.lead_time),'')::integer                                    AS lead_time,

    -- CEP e localização
    lpad(regexp_replace(s.cep,'\\D','','g'), 8, '0')::char(8)                 AS cep_texto,
    NULLIF(regexp_replace(s.cep,'\\D','','g'),'')::integer                    AS cep_num,
    upper(NULLIF(TRIM(s.uf),'') )::char(2)                                    AS uf,
    NULLIF(TRIM(s.cidades),'')                                               AS cidades,
    NULLIF(TRIM(s.endereco),'')                                              AS endereco,
    NULLIF(TRIM(s.numero),'')::varchar(10)                                    AS numero,
    NULLIF(TRIM(s.bairro),'')                                                AS bairro,
    NULLIF(TRIM(s.endereco_completo),'')                                      AS endereco_completo,

    -- transporte
    NULLIF(TRIM(s.remessa),'')                                               AS remessa,
    NULLIF(TRIM(regexp_replace(s.cnpj_cpf_transportadora,'\\D','','g')),'')::char(14) AS cnpj_cpf_transportadora,
    NULLIF(TRIM(s.transportador),'')                                         AS transportador,

    -- ocorrências
    NULLIF(TRIM(s.id_ult_ocr),'')                                            AS id_ult_ocr,
    NULLIF(TRIM(s.ultima_ocorrencia),'')                                     AS ultima_ocorrencia,
    NULLIF(TRIM(s.chave_ult_ocr),'')                                         AS chave_ult_ocr,

    -- categóricos
    NULLIF(TRIM(s.tipo_entrega),'')                                          AS tipo_entrega,
    NULLIF(TRIM(s.status_prazo),'')                                          AS status_prazo,
    NULLIF(TRIM(s.grau_risco),'')                                            AS grau_risco,
    NULLIF(TRIM(s.tipo_operacao),'')                                         AS tipo_operacao,

    -- demais
    NULLIF(TRIM(s.cod_cd),'')::integer                                       AS cod_cd,
    NULLIF(TRIM(s.cd),'')                                                    AS cd,
    NULLIF(TRIM(s.cod_vendedor),'')                                          AS cod_vendedor,
    CASE
      WHEN s.etiquetas ~ '^\\s*\\[.*\\]\\s*$' OR s.etiquetas ~ '^\\s*\\{.*\\}\\s*$' THEN s.etiquetas::jsonb
      WHEN coalesce(btrim(s.etiquetas),'') = '' THEN NULL
      ELSE to_jsonb(s.etiquetas)
    END                                                                     AS etiquetas,
    NULLIF(TRIM(regexp_replace(s.cpf_destinatario,'\\D','','g')),'')::char(11) AS cpf_destinatario,
    NULLIF(TRIM(s.agrupador),'')                                             AS agrupador,
    NULLIF(TRIM(s.arquivo_origem),'')                                        AS arquivo_origem,

    -- controle de carga
    COALESCE(
      CASE
        WHEN s.data_insercao ~ '^\\d{4}-\\d{2}-\\d{2}(?:[ T]\\d{2}:\\d{2}(:\\d{2})?)?$'
          THEN to_timestamp(replace(s.data_insercao,'T',' '), 'YYYY-MM-DD HH24:MI:SS')
        WHEN s.data_insercao ~ '^\\d{2}/\\d{2}/\\d{4}(?: \\d{2}:\\d{2}(:\\d{2})?)?$'
          THEN to_timestamp(s.data_insercao, 'DD/MM/YYYY HH24:MI:SS')
        ELSE NULL
      END,
      now()
    )                                                                       AS data_insercao
  FROM stagging.stg_pedidos s
)
INSERT INTO dw.fat_pedidos (
  chave_nfe, pedido,
  data_insercao, data_nfe, data_prev_entrega, data_prev_entrega_original, data_ultima_ocr, chegada_transportadora,
  serie_nfe, numero_nfe, valor_nfe, qtd_volumes, qtd_itens, peso, lead_time,
  cep_num, cep_texto, uf, cidades, endereco, numero, bairro, endereco_completo,
  remessa, cnpj_cpf_transportadora, transportador,
  id_ult_ocr, ultima_ocorrencia, chave_ult_ocr,
  tipo_entrega, status_prazo, grau_risco, tipo_operacao,
  cod_cd, cd, cod_vendedor, etiquetas, cpf_destinatario, agrupador, arquivo_origem
)
SELECT
  src.chave_nfe, src.pedido,
  src.data_insercao, src.data_nfe, src.data_prev_entrega, src.data_prev_entrega_original, src.data_ultima_ocr, src.chegada_transportadora,
  src.serie_nfe, src.numero_nfe, src.valor_nfe, src.qtd_volumes, src.qtd_itens, src.peso, src.lead_time,
  src.cep_num, src.cep_texto, src.uf, src.cidades, src.endereco, src.numero, src.bairro, src.endereco_completo,
  src.remessa, src.cnpj_cpf_transportadora, src.transportador,
  src.id_ult_ocr, src.ultima_ocorrencia, src.chave_ult_ocr,
  src.tipo_entrega, src.status_prazo, src.grau_risco, src.tipo_operacao,
  src.cod_cd, src.cd, src.cod_vendedor, src.etiquetas, src.cpf_destinatario, src.agrupador, src.arquivo_origem
FROM src
WHERE src.chave_nfe IS NOT NULL
ON CONFLICT (chave_nfe) DO UPDATE
SET
  -- atualiza somente se a nova ocorrência for mais recente
  data_prev_entrega = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.data_prev_entrega
    ELSE dw.fat_pedidos.data_prev_entrega
  END,
  status_prazo = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.status_prazo
    ELSE dw.fat_pedidos.status_prazo
  END,
  id_ult_ocr = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.id_ult_ocr
    ELSE dw.fat_pedidos.id_ult_ocr
  END,
  ultima_ocorrencia = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.ultima_ocorrencia
    ELSE dw.fat_pedidos.ultima_ocorrencia
  END,
  chave_ult_ocr = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.chave_ult_ocr
    ELSE dw.fat_pedidos.chave_ult_ocr
  END,
  data_ultima_ocr = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.data_ultima_ocr
    ELSE dw.fat_pedidos.data_ultima_ocr
  END,
  chegada_transportadora = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.chegada_transportadora
    ELSE dw.fat_pedidos.chegada_transportadora
  END,
  arquivo_origem = CASE
    WHEN EXCLUDED.data_ultima_ocr > dw.fat_pedidos.data_ultima_ocr THEN EXCLUDED.arquivo_origem
    ELSE dw.fat_pedidos.arquivo_origem
  END
;
"""

def run_upsert():
    conn = psycopg2.connect(
        host=os.getenv("PGHOST"),
        user=os.getenv("PGUSER"),
        password=os.getenv("PGPASSWORD"),
        dbname=os.getenv("PGDATABASE"),
        port=os.getenv("PGPORT"),
        options="-c search_path=public,stagging,dw"
    )
    try:
        with conn, conn.cursor() as cur:
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
