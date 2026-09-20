# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 07 — Exportação dos resultados
# MAGIC
# MAGIC Reúne todos os resultados da análise em um único arquivo Excel, uma aba por
# MAGIC consulta, gravado em um Volume do Unity Catalog para download.
# MAGIC
# MAGIC Serve para redigir o README sem precisar voltar ao Databricks a cada número, e
# MAGIC para conferir os valores fora do ambiente.
# MAGIC
# MAGIC **Não faz parte do pipeline.** É utilitário de apoio à documentação.

# COMMAND ----------

# MAGIC %pip install openpyxl --quiet

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

import pandas as pd

GOLD = "workspace.gold"
VOLUME = "/Volumes/workspace/gold/exports"
ARQUIVO = f"{VOLUME}/resultados_analise_glp.xlsx"

spark.sql("CREATE VOLUME IF NOT EXISTS workspace.gold.exports "
          "COMMENT 'Arquivos de exportacao dos resultados, para apoio a documentacao'")

abas = {}  # nome da aba -> DataFrame pandas

def coletar(nome, sql):
    """Executa a consulta e guarda o resultado como aba. Nome com até 31 caracteres."""
    df = spark.sql(sql).toPandas()
    abas[nome[:31]] = df
    print(f"  {nome:34s} {len(df):>6,} linhas x {len(df.columns)} colunas")
    return df

# COMMAND ----------

# MAGIC %md
# MAGIC ## Contexto do conjunto

# COMMAND ----------

coletar("00_contexto", f"""
  SELECT 'registros no fato'        AS metrica, cast(count(*) AS string)               AS valor FROM {GOLD}.fato_coleta_preco
  UNION ALL SELECT 'revendas distintas',        cast(count(*) AS string) FROM {GOLD}.dim_revenda
  UNION ALL SELECT 'municipios distintos',      cast(count(*) AS string) FROM {GOLD}.dim_localidade
  UNION ALL SELECT 'capitais no painel',        cast(count(*) AS string) FROM {GOLD}.dim_localidade WHERE flag_painel_capitais
  UNION ALL SELECT 'bandeiras distintas',       cast(count(*) AS string) FROM {GOLD}.dim_bandeira
  UNION ALL SELECT 'datas de coleta',           cast(count(*) AS string) FROM {GOLD}.dim_tempo
  UNION ALL SELECT 'primeira coleta',           cast(min(data_coleta) AS string) FROM {GOLD}.dim_tempo
  UNION ALL SELECT 'ultima coleta',             cast(max(data_coleta) AS string) FROM {GOLD}.dim_tempo
  UNION ALL SELECT 'preco medio geral',         cast(round(avg(valor_venda), 2) AS string) FROM {GOLD}.fato_coleta_preco
  UNION ALL SELECT 'preco minimo',              cast(min(valor_venda) AS string) FROM {GOLD}.fato_coleta_preco
  UNION ALL SELECT 'preco maximo',              cast(max(valor_venda) AS string) FROM {GOLD}.fato_coleta_preco
""")

coletar("00_bandeiras", f"""
  SELECT bandeira, flag_bandeira_branca, qtd_revendas, qtd_coletas
  FROM {GOLD}.dim_bandeira ORDER BY qtd_coletas DESC
""")

coletar("00_cobertura_mensal", f"""
  SELECT t.ano_mes,
         count(*)                      AS coletas,
         count(DISTINCT l.municipio)   AS municipios,
         count(DISTINCT f.sk_revenda)  AS revendas,
         round(avg(f.valor_venda), 2)  AS preco_medio_bruto
  FROM {GOLD}.fato_coleta_preco f
  JOIN {GOLD}.dim_tempo      t ON f.sk_tempo = t.sk_tempo
  JOIN {GOLD}.dim_localidade l ON f.sk_local = l.sk_local
  GROUP BY t.ano_mes ORDER BY t.ano_mes
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — variação regional

# COMMAND ----------

coletar("P1_regioes", f"""
  WITH por_capital AS (
    SELECT regiao_nome, municipio, avg(preco_medio) AS preco
    FROM {GOLD}.agg_preco_capital_mes GROUP BY regiao_nome, municipio)
  SELECT regiao_nome, count(*) AS capitais, round(avg(preco), 2) AS preco_medio,
         round(100 * (avg(preco) / min(avg(preco)) OVER () - 1), 1) AS vs_mais_barata_pct
  FROM por_capital GROUP BY regiao_nome ORDER BY preco_medio
""")

coletar("P1_capitais", f"""
  WITH por_capital AS (
    SELECT municipio, uf_sigla, regiao_nome, avg(preco_medio) AS preco
    FROM {GOLD}.agg_preco_capital_mes GROUP BY municipio, uf_sigla, regiao_nome)
  SELECT municipio, uf_sigla, regiao_nome, round(preco, 2) AS preco_medio,
         round(100 * (preco / min(preco) OVER () - 1), 1)  AS acima_da_minima_pct,
         round(100 * preco / 109.0, 1) AS pct_linha_extrema_pobreza,
         round(100 * preco / 218.0, 1) AS pct_linha_pobreza,
         round(100 * preco / 600.0, 1) AS pct_piso_bolsa_familia
  FROM por_capital ORDER BY preco
""")

coletar("P1_estabilidade", f"""
  WITH por_capital AS (
    SELECT regiao_nome, municipio,
           CASE WHEN ano_mes <= '2024-12' THEN 'primeiros_6m'
                WHEN ano_mes >= '2026-03' THEN 'ultimos_6m' END AS janela,
           avg(preco_medio) AS preco
    FROM {GOLD}.agg_preco_capital_mes
    WHERE ano_mes <= '2024-12' OR ano_mes >= '2026-03'
    GROUP BY regiao_nome, municipio, janela)
  SELECT regiao_nome, janela, round(avg(preco), 2) AS preco_medio
  FROM por_capital GROUP BY regiao_nome, janela ORDER BY janela, preco_medio
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — dispersão intramunicipal

# COMMAND ----------

coletar("P2_dispersao_geral", f"""
  SELECT count(*)                                      AS grupos_municipio_semana,
         round(percentile(coef_variacao_pct, 0.5), 1)  AS cv_mediano_pct,
         round(percentile(coef_variacao_pct, 0.9), 1)  AS cv_p90_pct,
         round(percentile(spread_pct, 0.5), 1)         AS spread_mediano_pct,
         round(percentile(spread_pct, 0.9), 1)         AS spread_p90_pct,
         round(percentile(economia_rs, 0.5), 2)        AS economia_mediana_rs,
         round(percentile(economia_rs, 0.9), 2)        AS economia_p90_rs
  FROM {GOLD}.agg_dispersao_municipio_semana
""")

coletar("P2_dispersao_regiao", f"""
  SELECT regiao_nome, count(*) AS grupos,
         round(percentile(spread_pct, 0.5), 1)  AS spread_mediano_pct,
         round(percentile(economia_rs, 0.5), 2) AS economia_mediana_rs
  FROM {GOLD}.agg_dispersao_municipio_semana
  GROUP BY regiao_nome ORDER BY economia_mediana_rs DESC
""")

coletar("P2_top_municipios", f"""
  SELECT municipio, uf_sigla, regiao_nome, count(*) AS semanas,
         round(avg(spread_pct), 1)  AS spread_medio_pct,
         round(avg(economia_rs), 2) AS economia_media_rs
  FROM {GOLD}.agg_dispersao_municipio_semana
  GROUP BY municipio, uf_sigla, regiao_nome
  HAVING count(*) >= 26
  ORDER BY economia_media_rs DESC LIMIT 30
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — bandeira branca (comparação pareada)

# COMMAND ----------

PARES = f"""
  WITH pares AS (
    SELECT l.municipio, l.uf_sigla, l.regiao_nome, t.ano_mes,
           avg(CASE WHEN b.flag_bandeira_branca     THEN f.valor_venda END) AS branca,
           avg(CASE WHEN NOT b.flag_bandeira_branca THEN f.valor_venda END) AS bandeirada
    FROM {GOLD}.fato_coleta_preco f
    JOIN {GOLD}.dim_tempo      t ON f.sk_tempo    = t.sk_tempo
    JOIN {GOLD}.dim_localidade l ON f.sk_local    = l.sk_local
    JOIN {GOLD}.dim_bandeira   b ON f.sk_bandeira = b.sk_bandeira
    GROUP BY l.municipio, l.uf_sigla, l.regiao_nome, t.ano_mes
    HAVING branca IS NOT NULL AND bandeirada IS NOT NULL)
"""

coletar("P3_bandeira_geral", PARES + """
  SELECT count(*)                                       AS pares_municipio_mes,
         round(avg(branca - bandeirada), 2)             AS diferenca_media_rs,
         round(percentile(branca - bandeirada, 0.5), 2) AS diferenca_mediana_rs,
         round(100 * avg(branca - bandeirada) / avg(bandeirada), 1) AS diferenca_pct,
         round(100 * avg(CASE WHEN branca < bandeirada THEN 1.0 ELSE 0.0 END), 1)
                                                        AS pct_branca_mais_barata
  FROM pares
""")

coletar("P3_bandeira_regiao", PARES + """
  SELECT regiao_nome, count(*) AS pares,
         round(avg(branca - bandeirada), 2) AS diferenca_media_rs,
         round(100 * avg(CASE WHEN branca < bandeirada THEN 1.0 ELSE 0.0 END), 1)
                                            AS pct_branca_mais_barata
  FROM pares GROUP BY regiao_nome ORDER BY diferenca_media_rs
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — concorrência local

# COMMAND ----------

coletar("P4_municipios_2026", f"""
  SELECT l.municipio, l.uf_sigla, l.regiao_nome,
         count(DISTINCT f.sk_revenda)  AS revendas,
         round(avg(f.valor_venda), 2)  AS preco_medio
  FROM {GOLD}.fato_coleta_preco f
  JOIN {GOLD}.dim_tempo      t ON f.sk_tempo = t.sk_tempo
  JOIN {GOLD}.dim_localidade l ON f.sk_local = l.sk_local
  WHERE t.ano = 2026
  GROUP BY l.municipio, l.uf_sigla, l.regiao_nome
  HAVING count(DISTINCT f.sk_revenda) >= 3
  ORDER BY revendas DESC
""")

coletar("P4_faixas", f"""
  WITH mun AS (
    SELECT l.municipio, l.regiao_nome,
           count(DISTINCT f.sk_revenda) AS revendas,
           avg(f.valor_venda)           AS preco
    FROM {GOLD}.fato_coleta_preco f
    JOIN {GOLD}.dim_tempo      t ON f.sk_tempo = t.sk_tempo
    JOIN {GOLD}.dim_localidade l ON f.sk_local = l.sk_local
    WHERE t.ano = 2026
    GROUP BY l.municipio, l.regiao_nome
    HAVING count(DISTINCT f.sk_revenda) >= 3)
  SELECT CASE WHEN revendas < 6  THEN '1. menos de 6'
              WHEN revendas < 11 THEN '2. de 6 a 10'
              WHEN revendas < 21 THEN '3. de 11 a 20'
              ELSE '4. 21 ou mais' END AS faixa_revendas,
         count(*) AS municipios, round(avg(preco), 2) AS preco_medio
  FROM mun GROUP BY 1 ORDER BY 1
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — evolução temporal

# COMMAND ----------

coletar("P5_indice_mensal", f"""
  SELECT ano_mes, capitais, coletas, preco_medio_painel, indice_base_100,
         variacao_mensal_pct, variacao_acumulada_pct,
         pct_linha_extrema_pobreza, pct_piso_bolsa_familia
  FROM {GOLD}.agg_indice_p13_nacional ORDER BY ano_mes
""")

coletar("P5_maiores_variacoes", f"""
  SELECT ano_mes, preco_medio_painel, variacao_mensal_pct
  FROM {GOLD}.agg_indice_p13_nacional
  WHERE variacao_mensal_pct IS NOT NULL
  ORDER BY abs(variacao_mensal_pct) DESC LIMIT 10
""")

coletar("P5_por_regiao_mes", f"""
  WITH por_capital AS (
    SELECT ano_mes, regiao_nome, municipio, avg(preco_medio) AS p
    FROM {GOLD}.agg_preco_capital_mes GROUP BY ano_mes, regiao_nome, municipio)
  SELECT ano_mes, regiao_nome, round(avg(p), 2) AS preco_medio
  FROM por_capital GROUP BY ano_mes, regiao_nome ORDER BY ano_mes, regiao_nome
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — sincronia entre regiões

# COMMAND ----------

reg = (abas["P5_por_regiao_mes"]
       .pivot(index="ano_mes", columns="regiao_nome", values="preco_medio")
       .sort_index().astype(float))
var = reg.pct_change().dropna()

abas["P6_correlacao"] = var.corr().round(3).reset_index()
abas["P6_correlacao_lag1"] = pd.DataFrame(
    {a: {b: var[a].corr(var[b].shift(-1)) for b in var.columns} for a in var.columns}
).round(3).reset_index()
abas["P6_variacoes_mensais"] = (100 * var).round(2).reset_index()

print(f"  P6_correlacao                      {len(var.columns)} regioes x {len(var)} meses")
print(var.corr().round(2).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 7 — margem bruta

# COMMAND ----------

coletar("P7_margem_bruta", f"""
  SELECT count(*)                                       AS registros,
         count(valor_compra)                            AS com_valor_compra,
         round(100 * count(valor_compra) / count(*), 2) AS pct_preenchido
  FROM {GOLD}.fato_coleta_preco
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Qualidade de dados

# COMMAND ----------

coletar("Q_cnpj_por_arquivo", """
  SELECT _arquivo_origem,
         CASE WHEN trim(cnpj_revenda) RLIKE '^[0-9]{2}\\\\.[0-9]{3}\\\\.[0-9]{3}/[0-9]{4}-[0-9]{2}$'
              THEN 'formatado' ELSE 'apenas digitos' END AS formato,
         CASE WHEN cnpj_revenda <> trim(cnpj_revenda) THEN 'com espaco'
              ELSE 'sem espaco' END                      AS espacos,
         count(*) AS registros
  FROM workspace.bronze.preco_glp_raw
  GROUP BY 1, 2, 3 ORDER BY 1
""")

coletar("Q_capitais_fora_painel", f"""
  SELECT municipio, uf_sigla, regiao_nome, meses_com_coleta, qtd_coletas
  FROM {GOLD}.dim_localidade
  WHERE flag_capital AND NOT flag_painel_capitais
""")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Gravação do arquivo

# COMMAND ----------

import io
from databricks.sdk import WorkspaceClient

# Volumes nao suportam escrita com seek (FUSE): gravar em memoria e enviar via API.
buffer = io.BytesIO()
with pd.ExcelWriter(buffer, engine="openpyxl") as writer:
    for nome, df in abas.items():
        df.to_excel(writer, sheet_name=nome, index=False)

w = WorkspaceClient()
w.files.upload(ARQUIVO, buffer.getvalue(), overwrite=True)

print(f"Arquivo gravado: {ARQUIVO}")
print(f"{len(abas)} abas:\n")
for nome, df in abas.items():
    print(f"  {nome:26s} {len(df):>6,} linhas")

print("\nPara baixar: Catalog -> workspace -> gold -> Volumes -> exports")
print("             clique no arquivo e use a opção de download.")