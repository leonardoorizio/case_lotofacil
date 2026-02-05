import requests
import pandas as pd
from pyspark.sql.functions import current_date

# 1) API
url = "https://loteriascaixa-api.herokuapp.com/api/lotofacil"
response = requests.get(url, timeout=30)

if response.status_code != 200:
    raise Exception(f"Erro na API: {response.status_code}")

df = pd.DataFrame(response.json())

# 2) Garantir que colunas "lista" não venham nulas
for col in ["dezenas", "dezenasOrdemSorteio", "premiacoes", "localGanhadores"]:
    if col in df.columns:
        df[col] = df[col].apply(lambda x: x if isinstance(x, list) else [])

# 3) Tabela principal concurso
df_concurso = df.drop(
    columns=[
        "dezenas", "dezenasOrdemSorteio", "premiacoes", "localGanhadores",
        "trevos", "timeCoracao", "mesSorte", "observacao", "estadosPremiados"
    ],
    errors="ignore"  # evita quebrar se alguma coluna não existir
)

# 4) Tabela normalizada dezenas
df_dezenas = (
    df[["concurso", "dezenas"]]
    .explode("dezenas")
    .rename(columns={"dezenas": "dezena"})
)

# 5) Tabela normalizada dezenas ordem
df_dezenas_ordem = (
    df[["concurso", "dezenasOrdemSorteio"]]
    .explode("dezenasOrdemSorteio")
    .rename(columns={"dezenasOrdemSorteio": "dezena"})
)

# 6) Tabelas normalizadas premiações
df_premiacoes = df[["concurso", "premiacoes"]].explode("premiacoes")
premiacoes_cols = pd.json_normalize(df_premiacoes["premiacoes"]).reset_index(drop=True)
df_premiacoes = pd.concat(
    [df_premiacoes.drop(columns=["premiacoes"]).reset_index(drop=True), premiacoes_cols],
    axis=1
)

# 7) Tabela normalizada local ganhadores
df_local_ganhadores = df[["concurso", "localGanhadores"]].explode("localGanhadores")
local_ganhadores_cols = pd.json_normalize(df_local_ganhadores["localGanhadores"]).reset_index(drop=True)
df_local_ganhadores = pd.concat(
    [df_local_ganhadores.drop(columns=["localGanhadores"]).reset_index(drop=True), local_ganhadores_cols],
    axis=1
)

# 8) Converte para Spark + metadado (mais simples como date direto)
df_concurso = spark.createDataFrame(df_concurso).withColumn("data_extracao", current_date())
df_dezenas = spark.createDataFrame(df_dezenas).withColumn("data_extracao", current_date())
df_dezenas_ordem = spark.createDataFrame(df_dezenas_ordem).withColumn("data_extracao", current_date())
df_premiacoes = spark.createDataFrame(df_premiacoes).withColumn("data_extracao", current_date())
df_local_ganhadores = spark.createDataFrame(df_local_ganhadores).withColumn("data_extracao", current_date())

# 9) Tabelas 
catalog = "raw"
schema = "default"

tables = {
    "df_concurso": df_concurso,
    "df_dezenas": df_dezenas,
    "df_dezenas_ordem": df_dezenas_ordem,
    "df_premiacoes": df_premiacoes,
    "df_local_ganhadores": df_local_ganhadores
}

for name, sdf in tables.items():
    full_name = f"{catalog}.{schema}.{name}"

    
    spark.sql(f"DROP TABLE IF EXISTS {full_name}")

    (sdf.write
        .mode("overwrite")
        .saveAsTable(full_name)
    )
