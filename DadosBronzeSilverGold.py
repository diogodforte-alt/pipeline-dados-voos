# Databricks notebook source
from pyspark.sql import functions as F

REF = "/Volumes/vooceuazul/bronze/arquivos/referencias"

# caractere que NAO existe no arquivo -> desliga o quoting do leitor de CSV
SEM_ASPAS = chr(0)

# COMMAND ----------

aerodromos = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")   # latin-1, nao UTF-8
    .option("quote", SEM_ASPAS)         # desliga o quoting: aspas aqui sao "segundos"
    .load(f"{REF}/AerodromosPublicos.csv")
)

# COMMAND ----------

aerodromos = aerodromos.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("nome"),
    F.col("`Município`").alias("municipio"),
    F.col("UF").alias("uf"),
    F.col("`Município Servido`").alias("municipio_servido"),
    F.col("`UF Servido`").alias("uf_servido"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("situacao"),
).withColumn("_ingerido_em", F.current_timestamp())

# COMMAND ----------

# DBTITLE 1,Cell 4
REF = "/Volumes/vooceuazul/bronze/arquivos/REFERENCIA"

aerodromos = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)
    .option("encoding", "ISO-8859-1")
    .option("quote", SEM_ASPAS)
    .load(f"{REF}/AerodromosPublicos.csv")
)

aerodromos = aerodromos.select(
    F.col("`Código OACI`").alias("icao"),
    F.col("CIAD").alias("ciad"),
    F.col("Nome").alias("nome"),
    F.col("`Município`").alias("municipio"),
    F.col("UF").alias("uf"),
    F.col("`Município Servido`").alias("municipio_servido"),
    F.col("`UF Servido`").alias("uf_servido"),
    F.col("Latitude").alias("latitude"),
    F.col("Longitude").alias("longitude"),
    F.col("Altitude").alias("altitude"),
    F.col("`Situação`").alias("situacao"),
).withColumn("_ingerido_em", F.current_timestamp())

aerodromos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("vooceuazul.bronze.aerodromos")

print(f"bronze.aerodromos: {spark.table('vooceuazul.bronze.aerodromos').count():,} linhas")
display(spark.sql("SELECT icao, nome, municipio, uf FROM vooceuazul.bronze.aerodromos WHERE icao IN ('SBRB','SBGR','SBSP','SBFZ')"))


# COMMAND ----------

def ler_empresas(arquivo: str):
    """Le um cadastro de empresas. Sem uniao, sem enriquecimento: uma tabela por arquivo."""
    return (
        spark.read.format("csv")
        .option("sep", ";")
        .option("header", "true")
        .option("skipRows", 1)
        .option("encoding", "UTF-8")
        .option("quote", '"')
        .load(f"{REF}/{arquivo}")
        .select(
            F.col("ICAO").alias("icao"),
            F.col("Estrangeira").alias("sigla_iata"),
            F.col("Razao").alias("razao_social"),
            F.col("Servico").alias("servico"),
            F.col("Cidade").alias("cidade"),
            F.col("UF").alias("uf"),
            F.col("Ativa").alias("situacao"),
        )
        .withColumn("_arquivo_origem", F.lit(arquivo))
        .withColumn("_ingerido_em", F.current_timestamp())
    )

# COMMAND ----------

for arquivo, tabela in [
    ("pda_empresas_aereas_nacionais.csv",    "vooceuazul.bronze.empresas_nacionais"),
    ("pda_empresas_aereas_estrangeiros.csv", "vooceuazul.bronze.empresas_estrangeiras"),
]:
    ler_empresas(arquivo).write.format("delta").mode("overwrite").option(
        "overwriteSchema", "true"
    ).saveAsTable(tabela)
    print(f"{tabela}: {spark.table(tabela).count():,} linhas")

# COMMAND ----------

display(spark.sql("""
    SELECT 'empresas_nacionais' AS tabela, COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM vooceuazul.bronze.empresas_nacionais
    UNION ALL
    SELECT 'empresas_estrangeiras', COUNT(*),
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END)
    FROM vooceuazul.bronze.empresas_estrangeiras
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT icao, razao_social, servico, uf, situacao
    FROM vooceuazul.bronze.empresas_nacionais
    WHERE icao IN ('GLO','TAM','AZU','PAM')
    ORDER BY icao
"""))

display(spark.sql("""
    SELECT icao, razao_social, servico, situacao
    FROM vooceuazul.bronze.empresas_estrangeiras
    WHERE icao IN ('AAL','TAP','AVA','ARG')
    ORDER BY icao
"""))

# COMMAND ----------

CODIGOS = [
    ("codigo_di", "0", "Etapa Regular"),
    ("codigo_di", "2", "Etapa Extra"),
    ("codigo_di", "3", "Etapa de Retorno"),
    ("codigo_di", "4", "Inclusão de Etapa"),
    ("codigo_di", "6", "Etapa Não Remunerada Sem Transporte de Objetos"),
    ("codigo_di", "7", "Etapa de Voo de Fretamento"),
    ("codigo_di", "9", "Etapa de Voo Charter"),
    ("codigo_di", "D", "Etapa de Voo Duplicada"),
    ("codigo_di", "E", "Etapa Não Remunerada Com Transporte de Objetos"),
    ("codigo_tipo_linha", "N", "Doméstica Mista"),
    ("codigo_tipo_linha", "C", "Doméstica Cargueira"),
    ("codigo_tipo_linha", "I", "Internacional Mista"),
    ("codigo_tipo_linha", "G", "Internacional Cargueira"),
]

codigos = spark.createDataFrame(CODIGOS, "dominio string, codigo string, descricao string")
codigos.write.format("delta").mode("overwrite").option(
    "overwriteSchema", "true"
).saveAsTable("vooceuazul.bronze.codigos_operacao")

print(f"bronze.codigos_operacao: {spark.table('vooceuazul.bronze.codigos_operacao').count()} linhas")
display(spark.table("vooceuazul.bronze.codigos_operacao"))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN vooceuazul.bronze"))

# COMMAND ----------

display(spark.sql("""
    SELECT version, timestamp, operation,
           operationMetrics.numOutputRows AS linhas_escritas
    FROM (DESCRIBE HISTORY vooceuazul.bronze.vra)
    ORDER BY version
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT 'versao 0 (1a carga)'   AS versao,
           COUNT(*)                AS linhas,
           MIN(_ingerido_em)       AS ingerido_em
    FROM vooceuazul.bronze.vra VERSION AS OF 0
    UNION ALL
    SELECT 'versao atual', COUNT(*), MIN(_ingerido_em)
    FROM vooceuazul.bronze.vra
"""))

# COMMAND ----------

for tabela, comentario in [
    ("vooceuazul.bronze.aerodromos",
     "Bronze - cadastro de aerodromos publicos da ANAC, como chegou. Chave: codigo ICAO (OACI). "
     "Cobre apenas aerodromos brasileiros - aeroportos estrangeiros do VRA nao estao aqui."),
    ("vooceuazul.bronze.empresas_nacionais",
     "Bronze - cadastro de empresas aereas NACIONAIS da ANAC, como chegou. Chave: codigo ICAO. "
     "Nao unir com empresas_estrangeiras nesta camada: a uniao e feita na silver."),
    ("vooceuazul.bronze.empresas_estrangeiras",
     "Bronze - cadastro de empresas aereas ESTRANGEIRAS autorizadas a operar no Brasil, como chegou. "
     "Chave: codigo ICAO. Cadastro separado do nacional na origem, mantido separado no bronze."),
    ("vooceuazul.bronze.codigos_operacao",
     "Bronze - seed table curada a partir da pagina de descricao de variaveis da ANAC. "
     "Traduz codigo_di e codigo_tipo_linha para descricao em portugues."),
]:
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")

print("comentarios aplicados")
# Databricks notebook source
display(spark.sql("""
    SELECT
      COUNT(*)                                                        AS linhas,
      SUM(CASE WHEN partida_real     IS NULL THEN 1 ELSE 0 END)       AS partida_real_null_de_verdade,
      SUM(CASE WHEN partida_real     = 'null' THEN 1 ELSE 0 END)      AS partida_real_string_null,
      SUM(CASE WHEN partida_prevista = 'null' THEN 1 ELSE 0 END)      AS partida_prevista_string_null,
      SUM(CASE WHEN partida_prevista LIKE '%.%' THEN 1 ELSE 0 END)    AS com_fracao_de_segundo
    FROM vooceuazul.bronze.vra
"""))

# COMMAND ----------

# DBTITLE 1,Cell 2
spark.sql("CREATE SCHEMA IF NOT EXISTS vooceuazul.silver")

spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.silver.vra AS
WITH tipado AS (
  SELECT
    icao_empresa,
    numero_voo,
    codigo_di,
    codigo_tipo_linha,
    icao_origem,
    icao_destino,
    try_cast(nullif(partida_prevista, 'null') AS TIMESTAMP) AS partida_prevista,
    try_cast(nullif(partida_real,     'null') AS TIMESTAMP) AS partida_real,
    try_cast(nullif(chegada_prevista, 'null') AS TIMESTAMP) AS chegada_prevista,
    try_cast(nullif(chegada_real,     'null') AS TIMESTAMP) AS chegada_real,
    situacao_voo,
    nullif(codigo_justificativa, 'N/A')                     AS codigo_justificativa,
    _arquivo_origem,
    _ingerido_em
  FROM vooceuazul.bronze.vra
)
SELECT
  icao_empresa,
  numero_voo,
  codigo_di,
  codigo_tipo_linha,
  icao_origem,
  icao_destino,

  partida_prevista,
  CAST(partida_prevista AS DATE)                     AS partida_prevista_data,
  date_format(partida_prevista, 'HH:mm')             AS partida_prevista_hora,

  partida_real,
  CAST(partida_real AS DATE)                         AS partida_real_data,
  date_format(partida_real, 'HH:mm')                 AS partida_real_hora,

  chegada_prevista,
  CAST(chegada_prevista AS DATE)                     AS chegada_prevista_data,
  date_format(chegada_prevista, 'HH:mm')             AS chegada_prevista_hora,

  chegada_real,
  CAST(chegada_real AS DATE)                         AS chegada_real_data,
  date_format(chegada_real, 'HH:mm')                 AS chegada_real_hora,

  situacao_voo,
  codigo_justificativa,

  -- aritmetica pura: subtracao de colunas da propria linha, sem limiar e sem decisao
  CAST(timestampdiff(MINUTE, partida_prevista, partida_real) AS INT) AS atraso_partida_min,
  CAST(timestampdiff(MINUTE, chegada_prevista, chegada_real) AS INT) AS atraso_chegada_min,
  CAST(timestampdiff(MINUTE, partida_prevista, partida_real)
     - timestampdiff(MINUTE, chegada_prevista, chegada_real) AS INT) AS minutos_recuperados,

  _arquivo_origem,
  _ingerido_em,
  current_timestamp()                                AS _transformado_em
FROM tipado
""")

print("silver.vra criada")

# COMMAND ----------

display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM vooceuazul.bronze.vra) AS bronze_vra,
      (SELECT COUNT(*) FROM vooceuazul.silver.vra) AS silver_vra,
      (SELECT COUNT(*) FROM vooceuazul.bronze.vra)
        - (SELECT COUNT(*) FROM vooceuazul.silver.vra) AS diferenca
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT
      COUNT(partida_prevista)     AS partida_prevista_ok,
      COUNT(partida_real)         AS partida_real_ok,
      COUNT(chegada_prevista)     AS chegada_prevista_ok,
      COUNT(chegada_real)         AS chegada_real_ok,
      COUNT(atraso_partida_min)   AS atraso_partida_ok,
      COUNT(minutos_recuperados)  AS minutos_recuperados_ok
    FROM vooceuazul.silver.vra
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT icao_empresa, numero_voo, icao_origem, icao_destino,
           partida_prevista, partida_prevista_data, partida_prevista_hora,
           atraso_partida_min, atraso_chegada_min, minutos_recuperados, situacao_voo
    FROM vooceuazul.silver.vra
    ORDER BY partida_prevista
    LIMIT 5
"""))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.silver.empresas AS
SELECT
  icao,
  sigla_iata,
  razao_social,
  servico,
  cidade,
  uf,
  situacao,
  'nacional'      AS origem_cadastro,
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformado_em
FROM vooceuazul.bronze.empresas_nacionais
UNION ALL
SELECT
  icao,
  sigla_iata,
  razao_social,
  servico,
  cidade,
  uf,
  situacao,
  'estrangeira'   AS origem_cadastro,
  _arquivo_origem,
  _ingerido_em,
  current_timestamp() AS _transformado_em
FROM vooceuazul.bronze.empresas_estrangeiras
""")

display(spark.sql("""
    SELECT
      (SELECT COUNT(*) FROM vooceuazul.bronze.empresas_nacionais)    AS bronze_nacionais,
      (SELECT COUNT(*) FROM vooceuazul.bronze.empresas_estrangeiras) AS bronze_estrangeiras,
      (SELECT COUNT(*) FROM vooceuazul.bronze.empresas_nacionais)
        + (SELECT COUNT(*) FROM vooceuazul.bronze.empresas_estrangeiras) AS soma_esperada,
      (SELECT COUNT(*) FROM vooceuazul.silver.empresas)              AS silver_empresas
"""))


# COMMAND ----------

display(spark.sql("""
    SELECT origem_cadastro,
           COUNT(*) AS linhas,
           COUNT(CASE WHEN icao IS NOT NULL AND icao <> '' THEN 1 END) AS com_icao
    FROM vooceuazul.silver.empresas
    GROUP BY origem_cadastro
    ORDER BY origem_cadastro
"""))

# COMMAND ----------

spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.silver.aerodromos AS
SELECT
  icao,
  ciad,
  nome,
  municipio,
  uf                                            AS uf_nome,
  municipio_servido,
  uf_servido                                    AS uf_servido_nome,
  latitude                                      AS latitude_dms,
  longitude                                     AS longitude_dms,
  try_cast(replace(altitude, ',', '.') AS DOUBLE) AS altitude_m,
  situacao,
  _ingerido_em,
  current_timestamp()                           AS _transformado_em
FROM vooceuazul.bronze.aerodromos
""")

spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.silver.codigos_operacao AS
SELECT
  dominio,
  codigo,
  descricao,
  current_timestamp() AS _transformado_em
FROM vooceuazul.bronze.codigos_operacao
""")

# COMMAND ----------

display(spark.sql("""
    SELECT 'aerodromos' AS tabela,
           (SELECT COUNT(*) FROM vooceuazul.bronze.aerodromos) AS bronze,
           (SELECT COUNT(*) FROM vooceuazul.silver.aerodromos) AS silver
    UNION ALL
    SELECT 'codigos_operacao',
           (SELECT COUNT(*) FROM vooceuazul.bronze.codigos_operacao),
           (SELECT COUNT(*) FROM vooceuazul.silver.codigos_operacao)
"""))

# COMMAND ----------

COMENTARIOS_VRA = {
    "icao_empresa":            "Codigo ICAO de tres letras da empresa aerea que operou a etapa. Chave para silver.empresas.",
    "numero_voo":              "Numero do voo divulgado pela companhia. Identificador comercial, nao numerico: pode ter zero a esquerda e se repete entre datas.",
    "codigo_di":               "Codigo de autorizacao (DI) da etapa: distingue etapa regular, extra, de retorno, charter. Descricao em silver.codigos_operacao (dominio codigo_di).",
    "codigo_tipo_linha":       "Codigo do tipo de linha: N e C domesticas, I e G internacionais. Descricao em silver.codigos_operacao (dominio codigo_tipo_linha).",
    "icao_origem":             "Codigo ICAO do aerodromo de onde a etapa partiu. Chave para silver.aerodromos - aeroportos estrangeiros nao constam no cadastro da ANAC.",
    "icao_destino":            "Codigo ICAO do aerodromo onde a etapa pousou. Mesma observacao de cobertura da origem.",
    "partida_prevista":        "Horario de partida programado pela companhia, na hora local do aeroporto de origem.",
    "partida_prevista_data":   "Data da partida programada, separada para facilitar analise por dia.",
    "partida_prevista_hora":   "Hora e minuto da partida programada (HH:mm), separada para analise por faixa horaria.",
    "partida_real":            "Horario em que a aeronave efetivamente saiu. Nulo em voo cancelado, que nao chegou a partir.",
    "partida_real_data":       "Data da partida efetiva.",
    "partida_real_hora":       "Hora e minuto da partida efetiva (HH:mm).",
    "chegada_prevista":        "Horario de chegada programado, na hora local do aeroporto de destino.",
    "chegada_prevista_data":   "Data da chegada programada.",
    "chegada_prevista_hora":   "Hora e minuto da chegada programada (HH:mm).",
    "chegada_real":            "Horario em que a aeronave efetivamente pousou. Nulo em voo cancelado.",
    "chegada_real_data":       "Data da chegada efetiva.",
    "chegada_real_hora":       "Hora e minuto da chegada efetiva (HH:mm).",
    "situacao_voo":            "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao.",
    "codigo_justificativa":    "Motivo declarado do atraso. Deixou de ser exigido pela ANAC em abril de 2020 com a revogacao da IAC 1504: vem vazio em toda a janela deste projeto.",
    "atraso_partida_min":      "Minutos entre a partida programada e a partida efetiva. Positivo e atraso, negativo e antecipacao. Aritmetica pura: nao aplica limiar de pontualidade.",
    "atraso_chegada_min":      "Minutos entre a chegada programada e a chegada efetiva. Positivo e atraso, negativo e antecipacao.",
    "minutos_recuperados":     "Minutos que a etapa recuperou em voo: atraso de partida menos atraso de chegada. Positivo significa que chegou menos atrasada do que saiu.",
    "_arquivo_origem":         "Auditoria: nome do arquivo CSV mensal da ANAC de onde a linha veio.",
    "_ingerido_em":            "Auditoria: momento em que a linha entrou no bronze.",
    "_transformado_em":        "Auditoria: momento em que a silver foi reconstruida a partir do bronze.",
}

for coluna, comentario in COMENTARIOS_VRA.items():
    spark.sql(f"ALTER TABLE vooceuazul.silver.vra ALTER COLUMN {coluna} COMMENT '{comentario}'")

print(f"{len(COMENTARIOS_VRA)} colunas comentadas em silver.vra")

# COMMAND ----------

COMENTARIOS_EMPRESAS = {
    "icao":            "Codigo ICAO de tres letras da empresa. Vazio para operadores sem codigo (aviacao agricola, taxi aereo, aeroclube).",
    "sigla_iata":      "Sigla de duas letras da empresa no padrao IATA, como publicada pela ANAC.",
    "razao_social":    "Razao social da empresa aerea. E o nome que aparece para quem consome o produto final.",
    "servico":         "Tipo de servico autorizado pela ANAC: transporte regular, nao regular, aeroagricola, taxi aereo.",
    "cidade":          "Municipio da sede ou do representante legal no Brasil.",
    "uf":              "Sigla da unidade federativa da sede.",
    "situacao":        "Situacao do registro na ANAC: ATIVA ou nao. Registro inativo permanece na tabela porque a empresa pode ter voado no periodo analisado.",
    "origem_cadastro": "De qual dos dois cadastros da ANAC este registro veio: nacional ou estrangeira. E a coluna que preserva a fronteira entre as duas fontes depois da uniao.",
    "_arquivo_origem": "Auditoria: arquivo CSV de origem.",
    "_ingerido_em":    "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":"Auditoria: momento da construcao da silver.",
}

COMENTARIOS_AERODROMOS = {
    "icao":              "Codigo ICAO (OACI) do aerodromo. Chave de ligacao com origem e destino do VRA.",
    "ciad":              "Codigo de identificacao do aerodromo no cadastro da ANAC.",
    "nome":              "Nome do aerodromo como publicado pela ANAC.",
    "municipio":         "Municipio onde o aerodromo esta fisicamente localizado.",
    "uf_nome":           "Nome da unidade federativa POR EXTENSO (Acre, Sao Paulo), nao a sigla: e assim que a ANAC publica.",
    "municipio_servido": "Municipio principal atendido pelo aerodromo, que pode ser diferente do municipio onde ele fica.",
    "uf_servido_nome":   "Nome por extenso da UF do municipio servido.",
    "latitude_dms":      "Latitude em graus, minutos e segundos, como publicada pela ANAC.",
    "longitude_dms":     "Longitude em graus, minutos e segundos, como publicada pela ANAC.",
    "altitude_m":        "Altitude do aerodromo em metros. Na origem vem com virgula decimal.",
    "situacao":          "Situacao do aerodromo no cadastro da ANAC.",
    "_ingerido_em":      "Auditoria: momento da ingestao no bronze.",
    "_transformado_em":  "Auditoria: momento da construcao da silver.",
}

COMENTARIOS_CODIGOS = {
    "dominio":          "A qual coluna do VRA este codigo pertence: codigo_di ou codigo_tipo_linha.",
    "codigo":           "O codigo como aparece no VRA.",
    "descricao":        "Descricao oficial do codigo, curada da pagina de descricao de variaveis da ANAC.",
    "_transformado_em": "Auditoria: momento da construcao da silver.",
}

for tabela, mapa in [
    ("vooceuazul.silver.empresas",         COMENTARIOS_EMPRESAS),
    ("vooceuazul.silver.aerodromos",       COMENTARIOS_AERODROMOS),
    ("vooceuazul.silver.codigos_operacao", COMENTARIOS_CODIGOS),
]:
    for coluna, comentario in mapa.items():
        spark.sql(f"ALTER TABLE {tabela} ALTER COLUMN {coluna} COMMENT '{comentario}'")
    print(f"{len(mapa)} colunas comentadas em {tabela}")

# COMMAND ----------

TABELAS = {
    "vooceuazul.silver.vra": (
        "Silver - espelho governado de bronze.vra. Mesmo grao (uma linha por etapa de voo) e "
        "MESMA contagem de linhas do bronze: sem filtro, sem agregacao e sem regra de negocio. "
        "Traz tipagem, data e hora separadas e as tres metricas de aritmetica pura de atraso. "
        "Pontualidade, escopo e exclusoes ficam na gold.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-VRA", "grao": "etapa_de_voo"},
    ),
    "vooceuazul.silver.empresas": (
        "Silver - cadastro unificado de empresas aereas: uniao dos dois cadastros do bronze "
        "(nacionais e estrangeiras) com a coluna origem_cadastro preservando a fonte de cada registro. "
        "Contagem igual a soma exata das duas tabelas de origem.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Operador-Aereo", "grao": "empresa"},
    ),
    "vooceuazul.silver.aerodromos": (
        "Silver - espelho governado do cadastro de aerodromos publicos da ANAC. Cobre apenas "
        "aerodromos brasileiros: aeroportos estrangeiros do VRA nao constam aqui, e isso e "
        "propriedade da fonte, nao defeito.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-Aerodromos", "grao": "aerodromo"},
    ),
    "vooceuazul.silver.codigos_operacao": (
        "Silver - espelho da seed table de codigos de operacao (DI e tipo de linha) com as "
        "descricoes oficiais da ANAC.",
        {"camada": "silver", "dominio": "aviacao", "fonte": "ANAC-seed", "grao": "codigo"},
    ),
}

for tabela, (comentario, tags) in TABELAS.items():
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

display(spark.sql("""
    SELECT table_name,
           COUNT(*)                                                          AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END)  AS sem_comentario,
           ROUND(100.0 * SUM(CASE WHEN comment IS NOT NULL AND comment <> '' THEN 1 ELSE 0 END)
                 / COUNT(*), 1)                                              AS pct_documentado
    FROM vooceuazul.information_schema.columns
    WHERE table_schema = 'silver'
    GROUP BY table_name
    ORDER BY table_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM vooceuazul.information_schema.table_tags
    WHERE schema_name = 'silver'
    ORDER BY table_name, tag_name
"""))

# COMMAND ----------

display(spark.sql("SHOW TABLES IN vooceuazul.silver"))
# Databricks notebook source
# DBTITLE 1,Título
# MAGIC %md
# MAGIC # Governança Gold
# MAGIC
# MAGIC Cria o schema `gold` e a tabela One Big Table `obt_voos` a partir da silver.

# COMMAND ----------

# DBTITLE 1,Criar schema gold e tabela obt_voos
spark.sql("CREATE SCHEMA IF NOT EXISTS vooceuazul.gold")

spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.gold.obt_voos
AS SELECT *,
       CASE WHEN partida_real IS NULL THEN NULL
            WHEN partida_real <= partida_prevista THEN TRUE
            ELSE FALSE END AS partida_pontual
FROM vooceuazul.silver.vra
""")
print("Schema gold e tabela obt_voos criados com sucesso.")

# COMMAND ----------

# DBTITLE 1,Criar dim_aeroporto
spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.gold.dim_aeroporto AS
WITH icaos AS (
    SELECT icao_origem AS icao
    FROM vooceuazul.silver.vra
    WHERE icao_origem IS NOT NULL AND icao_origem <> ''
    UNION
    SELECT icao_destino AS icao
    FROM vooceuazul.silver.vra
    WHERE icao_destino IS NOT NULL AND icao_destino <> ''
)
SELECT
    i.icao AS icao_aeroporto,
    COALESCE(a.nome, CONCAT('AEROPORTO FORA DO CADASTRO ANAC ', i.icao)) AS nome_aeroporto,
    a.municipio AS municipio_aeroporto,
    a.uf_nome AS uf_aeroporto,
    CASE WHEN i.icao LIKE 'SB%' THEN 'Brasil' ELSE 'Exterior' END AS pais_aeroporto,
    (a.icao IS NOT NULL) AS no_cadastro_anac,
    current_timestamp() AS _processado_em
FROM icaos i
LEFT JOIN vooceuazul.silver.aerodromos a
    ON i.icao = a.icao
""")
print("Tabela gold.dim_aeroporto criada com sucesso.")

# COMMAND ----------

# DBTITLE 1,Criar fato_voos
spark.sql("""
CREATE OR REPLACE TABLE vooceuazul.gold.fato_voos AS
SELECT
    v.icao_empresa,
    e.razao_social AS nome_companhia,
    v.numero_voo,
    v.codigo_di,
    di.descricao AS descricao_di,
    v.codigo_tipo_linha,
    tl.descricao AS descricao_tipo_linha,
    CASE WHEN v.codigo_tipo_linha IN ('N', 'C') THEN 'Domestico'
         WHEN v.codigo_tipo_linha IN ('I', 'G') THEN 'Internacional'
    END AS escopo_voo,
    v.icao_origem,
    v.icao_destino,
    CONCAT(v.icao_origem, ' - ', v.icao_destino) AS rota,
    v.partida_prevista,
    v.partida_prevista_data,
    v.partida_prevista_hora,
    HOUR(v.partida_prevista) AS hora_partida_prevista,
    lower(date_format(v.partida_prevista, 'EEEE')) AS dia_semana,
    date_trunc('month', v.partida_prevista) AS mes_referencia,
    v.partida_real,
    v.chegada_prevista,
    v.chegada_real,
    v.atraso_partida_min,
    v.atraso_chegada_min,
    v.minutos_recuperados,
    CASE WHEN v.situacao_voo = 'REALIZADO'
              AND v.partida_real IS NOT NULL
              AND v.partida_prevista IS NOT NULL
              AND v.atraso_partida_min IS NULL
         THEN TRUE ELSE FALSE END AS atraso_fora_de_faixa,
    CASE WHEN v.atraso_partida_min IS NULL THEN NULL
         WHEN v.atraso_partida_min <= 15 THEN TRUE ELSE FALSE END AS partida_pontual,
    CASE WHEN v.atraso_chegada_min IS NULL THEN NULL
         WHEN v.atraso_chegada_min <= 15 THEN TRUE ELSE FALSE END AS chegada_pontual,
    v.situacao_voo,
    (v.situacao_voo = 'REALIZADO') AS voo_realizado,
    (v.situacao_voo = 'CANCELADO') AS voo_cancelado,
    e.origem_cadastro AS cadastro_companhia,
    current_timestamp() AS _processado_em
FROM vooceuazul.silver.vra v
LEFT JOIN vooceuazul.silver.empresas e
    ON v.icao_empresa = e.icao
LEFT JOIN vooceuazul.silver.codigos_operacao di
    ON v.codigo_di = di.codigo AND di.dominio = 'codigo_di'
LEFT JOIN vooceuazul.silver.codigos_operacao tl
    ON v.codigo_tipo_linha = tl.codigo AND tl.dominio = 'codigo_tipo_linha'
""")
print("Tabela gold.fato_voos criada com sucesso.")

# COMMAND ----------

display(spark.sql("""
    SELECT
      COUNT(*)                                                      AS recuperou_algum_minuto,
      SUM(CASE WHEN atraso_chegada_min <= 0 THEN 1 ELSE 0 END)      AS chegou_adiantado_ou_no_horario,
      SUM(CASE WHEN atraso_chegada_min >  0 THEN 1 ELSE 0 END)      AS chegou_atrasado_mesmo_assim,
      SUM(CASE WHEN atraso_chegada_min > 15 THEN 1 ELSE 0 END)      AS chegou_atrasado_mais_de_15
    FROM vooceuazul.gold.obt_voos
    WHERE minutos_recuperados > 0
"""))


# COMMAND ----------

display(spark.sql("""
    SELECT situacao_voo,
           COUNT(*)                                                AS voos,
           SUM(CASE WHEN partida_pontual IS NULL THEN 1 ELSE 0 END) AS partida_pontual_null
    FROM vooceuazul.gold.obt_voos GROUP BY situacao_voo
"""))

# COMMAND ----------

COMENTARIOS_OBT = {
    # ---- companhia ----
    "icao_empresa":           "Codigo ICAO de tres letras da companhia que operou a etapa. Use nome_companhia para exibir; este codigo serve para filtro exato.",
    "nome_companhia":         "Razao social da companhia aerea. Quando o codigo nao existe no cadastro da ANAC, traz COMPANHIA NAO CADASTRADA seguida do codigo, em vez de vazio.",
    "numero_voo":             "Numero comercial do voo divulgado pela companhia. Nao e identificador unico: o mesmo numero se repete todos os dias.",

    # ---- operacao ----
    "codigo_di":              "Codigo de autorizacao da etapa (DI) publicado pela ANAC. O codigo 1 aparece no dado e nao consta na tabela oficial de descricoes.",
    "descricao_di":           "Tipo da etapa por extenso: regular, extra, de retorno, charter, fretamento. Quando o codigo nao esta catalogado pela ANAC, diz isso explicitamente.",
    "codigo_tipo_linha":      "Codigo do tipo de linha da ANAC: N e C domesticas, I e G internacionais.",
    "descricao_tipo_linha":   "Tipo de linha por extenso, combinando escopo e natureza da operacao: Domestica Mista, Internacional Cargueira, etc.",
    "escopo_voo":             "Classificacao de negocio do voo em Domestico ou Internacional, derivada do tipo de linha. E a coluna certa para comparar os dois universos.",

    # ---- origem ----
    "icao_origem":            "Codigo ICAO do aeroporto de partida. Use nome_aeroporto_origem para exibir.",
    "nome_aeroporto_origem":  "Nome do aeroporto de partida. Aeroporto estrangeiro nao consta no cadastro da ANAC e aparece como AEROPORTO FORA DO CADASTRO ANAC seguido do codigo.",
    "municipio_origem":       "Municipio do aeroporto de partida. Vazio para aeroporto estrangeiro, que nao esta no cadastro brasileiro.",
    "uf_origem":              "Unidade federativa do aeroporto de partida, escrita POR EXTENSO (Sao Paulo, Ceara), como a ANAC publica. Nao e a sigla.",
    "pais_origem":            "Brasil ou Exterior, deduzido do prefixo do codigo ICAO. Serve para separar operacao domestica de internacional pelo lado do aeroporto.",

    # ---- destino ----
    "icao_destino":           "Codigo ICAO do aeroporto de chegada. Use nome_aeroporto_destino para exibir.",
    "nome_aeroporto_destino": "Nome do aeroporto de chegada. Mesma regra de fallback do aeroporto de origem.",
    "municipio_destino":      "Municipio do aeroporto de chegada. Vazio para aeroporto estrangeiro.",
    "uf_destino":             "Unidade federativa do aeroporto de chegada, por extenso.",
    "pais_destino":           "Brasil ou Exterior para o aeroporto de chegada.",

    # ---- rota ----
    "rota_icao":              "Rota no formato ORIGEM - DESTINO usando codigos ICAO. E a chave estavel para agrupar por rota.",
    "rota_municipios":        "Rota no formato municipio de origem - municipio de destino, para leitura humana. Aeroporto estrangeiro aparece pelo codigo ICAO, porque nao tem municipio no cadastro.",

    # ---- tempo ----
    "partida_prevista":       "Data e hora que a companhia programou para a partida, na hora local do aeroporto de origem.",
    "partida_prevista_data":  "Data programada da partida. Use para series diarias e para recortar periodo.",
    "partida_prevista_hora":  "Hora e minuto programados da partida, no formato HH:mm, para leitura.",
    "hora_partida_prevista":  "Hora cheia programada da partida, de 0 a 23. E a coluna certa para analisar o efeito cascata do atraso ao longo do dia.",
    "dia_semana":             "Dia da semana da partida programada, por extenso e em minusculas (domingo a sabado).",
    "mes_referencia":         "Primeiro dia do mes da partida programada, para agregacao mensal. E nulo nos voos que nao tem horario previsto informado.",
    "partida_real":           "Data e hora em que a aeronave efetivamente partiu. Nulo em voo cancelado.",
    "chegada_prevista":       "Data e hora programadas para a chegada, na hora local do aeroporto de destino.",
    "chegada_real":           "Data e hora em que a aeronave efetivamente pousou. Nulo em voo cancelado.",

    # ---- metricas ----
    "atraso_partida_min":     "Atraso de partida em minutos: horario real menos programado. Negativo significa que saiu adiantado. Nulo quando o voo foi cancelado, quando nao ha horario programado, ou quando o valor esta fora da faixa plausivel.",
    "atraso_chegada_min":     "Atraso de chegada em minutos: horario real menos programado. Negativo significa que pousou adiantado. Mesmas regras de nulo do atraso de partida.",
    "minutos_recuperados":    "Minutos que a etapa recuperou no ar: atraso de partida menos atraso de chegada. Positivo significa que chegou MENOS ATRASADA do que saiu, e NAO que chegou no horario - um voo pode recuperar 20 minutos e ainda assim pousar atrasado. Negativo significa que perdeu ainda mais tempo depois de decolar.",
    "atraso_fora_de_faixa":   "Verdadeiro quando o atraso calculado estava fora da faixa plausivel (menos de -2h ou mais de 24h), sinal de erro de data na origem. A linha continua contando como voo, mas as tres metricas de atraso foram anuladas.",
    "partida_pontual":        "Verdadeiro quando a partida atrasou 15 minutos ou menos, criterio de pontualidade deste projeto. Falso significa atraso maior que 15 minutos. Nulo significa que NAO DA para avaliar - voo cancelado ou sem horario programado - e nunca deve ser contado como atraso.",
    "chegada_pontual":        "Verdadeiro quando a chegada atrasou 15 minutos ou menos. Mesma regra de nulo da pontualidade de partida.",

    # ---- situacao ----
    "situacao_voo":           "Situacao informada pela companhia: REALIZADO quando a etapa aconteceu, CANCELADO quando nao aconteceu.",
    "voo_realizado":          "Verdadeiro quando a etapa foi realizada. Use como denominador de metricas operacionais.",
    "voo_cancelado":          "Verdadeiro quando a etapa foi cancelada. Voo cancelado NAO entra em nenhuma media de atraso, porque nao tem horario real; use esta coluna para taxa de cancelamento.",

    "_processado_em":         "Auditoria: momento em que esta linha foi construida na camada gold.",
}

COLUNAS_EXISTENTES = {r[0] for r in spark.sql("DESCRIBE TABLE vooceuazul.gold.obt_voos").collect() if r[0] and not r[0].startswith("#")}

comentadas = 0
for coluna, comentario in COMENTARIOS_OBT.items():
    if coluna not in COLUNAS_EXISTENTES:
        continue
    spark.sql(f"ALTER TABLE vooceuazul.gold.obt_voos ALTER COLUMN {coluna} COMMENT '{comentario}'")
    comentadas += 1

print(f"{comentadas} colunas comentadas em gold.obt_voos")


# COMMAND ----------

COMENTARIOS_DIM = {
    "icao_aeroporto":      "Codigo ICAO do aeroporto. Chave da dimensao, serve tanto para origem quanto para destino do fato.",
    "nome_aeroporto":      "Nome do aeroporto. Traz fallback textual com o codigo quando o aeroporto nao esta no cadastro da ANAC.",
    "municipio_aeroporto": "Municipio onde o aeroporto esta localizado. Vazio para aeroporto estrangeiro.",
    "uf_aeroporto":        "Unidade federativa por extenso, como a ANAC publica. Nao e a sigla.",
    "pais_aeroporto":      "Brasil ou Exterior, deduzido do prefixo ICAO. Regra de negocio criada na gold.",
    "no_cadastro_anac":    "Verdadeiro quando o aeroporto existe no cadastro de aerodromos publicos da ANAC. Falso e o esperado para aeroporto estrangeiro, e nao indica erro.",
    "_processado_em":      "Auditoria: momento da construcao da dimensao.",
}

COMENTARIOS_FATO = dict(COMENTARIOS_OBT)
COMENTARIOS_FATO["icao_origem"]  = "Codigo ICAO do aeroporto de partida. Chave para gold.dim_aeroporto."
COMENTARIOS_FATO["icao_destino"] = "Codigo ICAO do aeroporto de chegada. Chave para gold.dim_aeroporto."
COMENTARIOS_FATO["rota"] = "Rota no formato ORIGEM - DESTINO usando codigos ICAO."
COMENTARIOS_FATO["cadastro_companhia"] = "De qual cadastro da ANAC veio a companhia: nacional ou estrangeira. Nulo quando o codigo nao tem cadastro."
TABELAS_GOLD = {r.tableName for r in spark.sql("SHOW TABLES IN vooceuazul.gold").collect()}

COLUNAS_FATO = [c for c in COMENTARIOS_FATO if c not in (
    "nome_aeroporto_origem", "municipio_origem", "uf_origem", "pais_origem",
    "nome_aeroporto_destino", "municipio_destino", "uf_destino", "pais_destino",
    "rota_icao", "rota_municipios")]

if "fato_voos" in TABELAS_GOLD:
    for coluna in COLUNAS_FATO:
        spark.sql(f"ALTER TABLE vooceuazul.gold.fato_voos ALTER COLUMN {coluna} COMMENT '{COMENTARIOS_FATO[coluna]}'")
    print(f"{len(COLUNAS_FATO)} colunas comentadas em gold.fato_voos")
else:
    print("gold.fato_voos ainda nao existe — comentarios serao aplicados quando a tabela for criada")

if "dim_aeroporto" in TABELAS_GOLD:
    for coluna, comentario in COMENTARIOS_DIM.items():
        spark.sql(f"ALTER TABLE vooceuazul.gold.dim_aeroporto ALTER COLUMN {coluna} COMMENT '{comentario}'")
    print(f"{len(COMENTARIOS_DIM)} colunas comentadas em gold.dim_aeroporto")
else:
    print("gold.dim_aeroporto ainda nao existe — comentarios serao aplicados quando a tabela for criada")

# COMMAND ----------

TABELAS_GOLD = {
    "vooceuazul.gold.obt_voos": (
        "Gold - One Big Table de voos da ANAC, desnormalizada e desenhada para consumo por agente de IA. "
        "Uma linha por etapa de voo, com nomes ja resolvidos e metricas prontas: responde as perguntas de "
        "negocio do projeto sem nenhum JOIN. Criterio de pontualidade: 15 minutos. "
        "Voo cancelado nao tem metrica de atraso.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "genie", "grao": "etapa_de_voo", "padrao": "obt"},
    ),
    "vooceuazul.gold.fato_voos": (
        "Gold - fato de voos no grao de uma linha por etapa, com companhia e codigos de operacao como "
        "dimensoes degeneradas. E aqui que nascem as regras de negocio: pontualidade a 15 minutos, "
        "escopo domestico/internacional e as decisoes sobre a quarentena. "
        "Contagem = silver.vra menos 41 duplicatas exatas.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "etapa_de_voo", "padrao": "fato"},
    ),
    "vooceuazul.gold.dim_aeroporto": (
        "Gold - dimensao de aeroporto, servindo origem e destino do fato. Construida a partir dos codigos "
        "presentes no fato e enriquecida pelo cadastro da ANAC, para cobrir 100 por cento do fato inclusive "
        "os aeroportos estrangeiros, que a ANAC nao cadastra.",
        {"camada": "gold", "dominio": "aviacao", "consumo": "bi", "grao": "aeroporto", "padrao": "dimensao"},
    ),
}

TABELAS_EXISTENTES = {r.tableName for r in spark.sql("SHOW TABLES IN vooceuazul.gold").collect()}

for tabela, (comentario, tags) in TABELAS_GOLD.items():
    nome_tabela = tabela.split(".")[-1]
    if nome_tabela not in TABELAS_EXISTENTES:
        print(f"{tabela}: tabela ainda nao existe — comentarios e tags serao aplicados quando a tabela for criada")
        continue
    spark.sql(f"COMMENT ON TABLE {tabela} IS '{comentario}'")
    pares = ", ".join(f"'{k}' = '{v}'" for k, v in tags.items())
    spark.sql(f"ALTER TABLE {tabela} SET TAGS ({pares})")
    print(f"{tabela}: comentario + {len(tags)} tags")

# COMMAND ----------

display(spark.sql("""
    SELECT table_schema, table_name,
           COUNT(*)                                                         AS colunas,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS sem_comentario
    FROM vooceuazul.information_schema.columns
    WHERE table_schema IN ('silver', 'gold')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
"""))

# COMMAND ----------

display(spark.sql("""
    SELECT table_name, tag_name, tag_value
    FROM vooceuazul.information_schema.table_tags
    WHERE schema_name = 'gold'
    ORDER BY table_name, tag_name
"""))


# COMMAND ----------

display(spark.sql("""
    SELECT
      COALESCE(nullif(source_table_full_name, ''), '(arquivo no volume)') AS origem,
      target_table_full_name                                             AS destino
    FROM system.access.table_lineage
    WHERE target_table_full_name LIKE 'vooceuazul.%'
      AND event_date >= current_date() - 7
    GROUP BY 1, 2
    ORDER BY destino, origem
"""))
# Databricks notebook source
from pyspark.sql import functions as F

CAMINHO = "/Volumes/vooceuazul/bronze/arquivos/vra/*.csv"
TABELA = "vooceuazul.bronze.vra"

# COMMAND ----------

# DBTITLE 1,Cell 2
CAMINHO = "/Volumes/vooceuazul/bronze/arquivos/VRA/*.csv"

bruto = (
    spark.read.format("csv")
    .option("sep", ";")
    .option("header", "true")
    .option("skipRows", 1)          # descarta "Atualizado em: ..." (e o BOM junto)
    .option("quote", '"')
    .option("escape", '"')
    .option("encoding", "UTF-8")
    .option("mode", "PERMISSIVE")   # bronze nao descarta linha nenhuma
    .load(CAMINHO)
)

print("colunas lidas do arquivo:")
for c in bruto.columns:
    print(f"  {c!r}")

# COMMAND ----------

RENOMEAR = {
    "ICAO Empresa Aérea": "icao_empresa",
    "Número Voo": "numero_voo",
    "Código Autorização (DI)": "codigo_di",
    "Código Tipo Linha": "codigo_tipo_linha",
    "ICAO Aeródromo Origem": "icao_origem",
    "ICAO Aeródromo Destino": "icao_destino",
    "Partida Prevista": "partida_prevista",
    "Partida Real": "partida_real",
    "Chegada Prevista": "chegada_prevista",
    "Chegada Real": "chegada_real",
    "Situação Voo": "situacao_voo",
    "Código Justificativa": "codigo_justificativa",
}

faltando = [c for c in RENOMEAR if c not in bruto.columns]
assert not faltando, f"Coluna esperada nao encontrada no CSV: {faltando}"

renomeado = bruto.select(
    *[F.col(f"`{origem}`").cast("string").alias(novo) for origem, novo in RENOMEAR.items()]
)

# COMMAND ----------

bronze = renomeado.withColumn(
    "_arquivo_origem", F.col("_metadata.file_name")
).withColumn(
    "_ingerido_em", F.current_timestamp()
)


# COMMAND ----------

(
    bronze.write.format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable(TABELA)
)

print(f"{TABELA}: {spark.table(TABELA).count():,} linhas")


# COMMAND ----------

spark.sql(f"""
    COMMENT ON TABLE {TABELA} IS
    'Bronze - VRA (Voo Regular Ativo) da ANAC, 12 meses (jan/2025 a dez/2025).
     Dado bruto: todas as colunas string, nenhuma linha descartada.
     Carga full refresh idempotente a partir de /Volumes/vooceuazul/bronze/arquivos/vra/.'
""")

# COMMAND ----------

display(
    spark.sql(f"""
        SELECT _arquivo_origem, COUNT(*) AS linhas, MAX(_ingerido_em) AS ingerido_em
        FROM {TABELA}
        GROUP BY _arquivo_origem
        ORDER BY _arquivo_origem
    """)
)
