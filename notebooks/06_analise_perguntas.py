# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# MAGIC %md
# MAGIC # 06 — Análise: respostas às perguntas de negócio
# MAGIC
# MAGIC Cada pergunta formulada na etapa de objetivo, respondida sobre o esquema
# MAGIC estrela e os agregados da camada Gold.
# MAGIC
# MAGIC ## Duas regras de controle
# MAGIC
# MAGIC **Comparar dentro do mesmo contexto.** Uma média simples de bandeira branca
# MAGIC contra bandeirada compara revendas de municípios diferentes: se as brancas se
# MAGIC concentram em cidades baratas, o desconto aparente é geografia disfarçada de
# MAGIC bandeira. Aqui a comparação é pareada dentro do mesmo município e mês.
# MAGIC
# MAGIC **Neutralizar a composição da amostra.** A abrangência da pesquisa da ANP foi
# MAGIC de 95 para 411 municípios ao longo da série. Toda análise temporal usa o painel
# MAGIC balanceado de 25 capitais.
# MAGIC
# MAGIC ## Distinção que organiza as respostas
# MAGIC
# MAGIC | Tipo de dispersão | A família pode agir? | Perguntas |
# MAGIC |---|---|---|
# MAGIC | **Acionável** — dentro da mesma cidade | Sim: basta ligar para outra revenda | 2, 3, 4 |
# MAGIC | **Estrutural** — entre regiões e no tempo | Não | 1, 5, 6 |

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

GOLD = "workspace.gold"
falhas = []

def conferir(rotulo, obtido, esperado, tol=0.0):
    ok = abs(float(obtido) - float(esperado)) <= tol
    if not ok:
        falhas.append(rotulo)
    print(f"  {'OK  ' if ok else 'ERRO'} {rotulo:34s} {obtido:>9}  (esperado {esperado})")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Qual a variação de preço entre regiões e UFs?
# MAGIC
# MAGIC Sobre o painel de capitais. A média regional é a média das capitais da região,
# MAGIC não a média das coletas: caso contrário Manaus, com 7.547 coletas, dominaria o
# MAGIC Norte inteiro.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH por_capital AS (
# MAGIC   SELECT regiao_nome, municipio, avg(preco_medio) AS preco
# MAGIC   FROM workspace.gold.agg_preco_capital_mes
# MAGIC   GROUP BY regiao_nome, municipio
# MAGIC )
# MAGIC SELECT
# MAGIC   regiao_nome                                          AS regiao,
# MAGIC   count(*)                                             AS capitais,
# MAGIC   round(avg(preco), 2)                                 AS preco_medio,
# MAGIC   round(100 * (avg(preco) / min(avg(preco)) OVER () - 1), 1) AS vs_mais_barata_pct
# MAGIC FROM por_capital
# MAGIC GROUP BY regiao_nome
# MAGIC ORDER BY preco_medio

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Capitais extremas e amplitude
# MAGIC WITH por_capital AS (
# MAGIC   SELECT municipio, uf_sigla, regiao_nome, avg(preco_medio) AS preco
# MAGIC   FROM workspace.gold.agg_preco_capital_mes
# MAGIC   GROUP BY municipio, uf_sigla, regiao_nome
# MAGIC )
# MAGIC SELECT municipio, uf_sigla, regiao_nome,
# MAGIC        round(preco, 2)                                        AS preco_medio,
# MAGIC        round(100 * (preco / min(preco) OVER () - 1), 1)       AS acima_da_minima_pct,
# MAGIC        round(100 * preco / 109.0, 1)                          AS pct_linha_extrema_pobreza
# MAGIC FROM por_capital
# MAGIC ORDER BY preco

# COMMAND ----------

# MAGIC %md
# MAGIC ### Estabilidade da hierarquia regional
# MAGIC
# MAGIC A ordem entre regiões se mantém ao longo do período, ou muda?

# COMMAND ----------

meses = [r[0] for r in spark.table(f"{GOLD}.agg_indice_p13_nacional")
         .select("ano_mes").orderBy("ano_mes").collect()]
primeiros, ultimos = meses[:6], meses[-6:]

def ranking(lista):
    return (spark.table(f"{GOLD}.agg_preco_capital_mes")
            .filter(F.col("ano_mes").isin(lista))
            .groupBy("regiao_nome", "municipio").agg(F.avg("preco_medio").alias("p"))
            .groupBy("regiao_nome").agg(F.avg("p").alias("preco"))
            .toPandas().set_index("regiao_nome")["preco"])

r1, r2 = ranking(primeiros), ranking(ultimos)
comp = r1.rank().to_frame("posto_inicio").join(r2.rank().to_frame("posto_fim"))
comp["preco_inicio"] = r1.round(2)
comp["preco_fim"] = r2.round(2)
print(comp.sort_values("posto_inicio").to_string())
print(f"\nCorrelação de postos (Spearman): "
      f"{comp['posto_inicio'].corr(comp['posto_fim'], method='spearman'):.3f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — Quanto varia o preço dentro da mesma cidade, na mesma semana?
# MAGIC
# MAGIC Este é o **ruído acionável**: a parcela da dispersão que o consumidor pode
# MAGIC contornar sem sair do bairro.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   count(*)                                             AS grupos_municipio_semana,
# MAGIC   round(percentile(coef_variacao_pct, 0.5), 1)         AS cv_mediano_pct,
# MAGIC   round(percentile(coef_variacao_pct, 0.9), 1)         AS cv_p90_pct,
# MAGIC   round(percentile(spread_pct, 0.5), 1)                AS spread_mediano_pct,
# MAGIC   round(percentile(spread_pct, 0.9), 1)                AS spread_p90_pct,
# MAGIC   round(percentile(economia_rs, 0.5), 2)               AS economia_mediana_rs,
# MAGIC   round(percentile(economia_rs, 0.9), 2)               AS economia_p90_rs
# MAGIC FROM workspace.gold.agg_dispersao_municipio_semana

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Onde pesquisar preço rende mais
# MAGIC SELECT regiao_nome                                      AS regiao,
# MAGIC        count(*)                                         AS grupos,
# MAGIC        round(percentile(spread_pct, 0.5), 1)            AS spread_mediano_pct,
# MAGIC        round(percentile(economia_rs, 0.5), 2)           AS economia_mediana_rs
# MAGIC FROM workspace.gold.agg_dispersao_municipio_semana
# MAGIC GROUP BY regiao_nome
# MAGIC ORDER BY economia_mediana_rs DESC

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Bandeira branca é mais barata?
# MAGIC
# MAGIC Comparação **pareada**: só municípios-mês onde os dois tipos coexistem. Isso
# MAGIC remove a geografia da conta e deixa apenas o efeito da bandeira.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH pares AS (
# MAGIC   SELECT l.municipio, l.uf_sigla, l.regiao_nome, t.ano_mes,
# MAGIC          avg(CASE WHEN b.flag_bandeira_branca     THEN f.valor_venda END) AS branca,
# MAGIC          avg(CASE WHEN NOT b.flag_bandeira_branca THEN f.valor_venda END) AS bandeirada
# MAGIC   FROM workspace.gold.fato_coleta_preco f
# MAGIC   JOIN workspace.gold.dim_tempo      t ON f.sk_tempo    = t.sk_tempo
# MAGIC   JOIN workspace.gold.dim_localidade l ON f.sk_local    = l.sk_local
# MAGIC   JOIN workspace.gold.dim_bandeira   b ON f.sk_bandeira = b.sk_bandeira
# MAGIC   GROUP BY l.municipio, l.uf_sigla, l.regiao_nome, t.ano_mes
# MAGIC   HAVING branca IS NOT NULL AND bandeirada IS NOT NULL
# MAGIC )
# MAGIC SELECT
# MAGIC   count(*)                                              AS pares_municipio_mes,
# MAGIC   round(avg(branca - bandeirada), 2)                    AS diferenca_media_rs,
# MAGIC   round(percentile(branca - bandeirada, 0.5), 2)        AS diferenca_mediana_rs,
# MAGIC   round(100 * avg(branca - bandeirada) / avg(bandeirada), 1) AS diferenca_pct,
# MAGIC   round(100 * avg(CASE WHEN branca < bandeirada THEN 1.0 ELSE 0.0 END), 1)
# MAGIC                                                         AS pct_pares_branca_mais_barata
# MAGIC FROM pares

# COMMAND ----------

# MAGIC %sql
# MAGIC -- O efeito é regional: forte no Sul e Sudeste, ausente no Norte, invertido no Nordeste
# MAGIC WITH pares AS (
# MAGIC   SELECT l.regiao_nome, l.municipio, t.ano_mes,
# MAGIC          avg(CASE WHEN b.flag_bandeira_branca     THEN f.valor_venda END) AS branca,
# MAGIC          avg(CASE WHEN NOT b.flag_bandeira_branca THEN f.valor_venda END) AS bandeirada
# MAGIC   FROM workspace.gold.fato_coleta_preco f
# MAGIC   JOIN workspace.gold.dim_tempo      t ON f.sk_tempo    = t.sk_tempo
# MAGIC   JOIN workspace.gold.dim_localidade l ON f.sk_local    = l.sk_local
# MAGIC   JOIN workspace.gold.dim_bandeira   b ON f.sk_bandeira = b.sk_bandeira
# MAGIC   GROUP BY l.regiao_nome, l.municipio, t.ano_mes
# MAGIC   HAVING branca IS NOT NULL AND bandeirada IS NOT NULL
# MAGIC )
# MAGIC SELECT regiao_nome                                       AS regiao,
# MAGIC        count(*)                                          AS pares,
# MAGIC        round(avg(branca - bandeirada), 2)                AS diferenca_media_rs
# MAGIC FROM pares
# MAGIC GROUP BY regiao_nome
# MAGIC ORDER BY diferenca_media_rs

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Mais revendas na cidade significa preço menor?
# MAGIC
# MAGIC Usa 2026, período de cobertura estável (cerca de 410 municípios por mês).
# MAGIC
# MAGIC A correlação é de **Spearman** (sobre postos, não sobre valores), porque a
# MAGIC relação não precisa ser linear para existir. Empates recebem posto médio, como
# MAGIC manda a definição — muitos municípios têm exatamente o mesmo número de revendas.

# COMMAND ----------

mun = (spark.table(f"{GOLD}.fato_coleta_preco")
       .join(spark.table(f"{GOLD}.dim_tempo").select("sk_tempo", "ano"), "sk_tempo")
       .join(spark.table(f"{GOLD}.dim_localidade")
             .select("sk_local", "municipio", "uf_sigla", "regiao_nome"), "sk_local")
       .filter("ano = 2026")
       .groupBy("municipio", "uf_sigla", "regiao_nome")
       .agg(F.countDistinct("sk_revenda").alias("revendas"),
            F.avg("valor_venda").alias("preco"))
       .filter("revendas >= 3"))

def posto_medio(df, coluna, particao=None):
    """Posto com correção de empates: min_rank + (n_empatados - 1) / 2."""
    w_ord = Window.orderBy(coluna) if particao is None else \
            Window.partitionBy(particao).orderBy(coluna)
    w_val = Window.partitionBy(coluna) if particao is None else \
            Window.partitionBy(particao, coluna)
    return F.rank().over(w_ord) + (F.count("*").over(w_val) - 1) / 2.0

nac = (mun
       .withColumn("p_rev", posto_medio(mun, "revendas"))
       .withColumn("p_pre", posto_medio(mun, "preco")))
rho_nac = nac.select(F.corr("p_rev", "p_pre")).first()[0]

reg = (mun
       .withColumn("p_rev", posto_medio(mun, "revendas", "regiao_nome"))
       .withColumn("p_pre", posto_medio(mun, "preco", "regiao_nome")))

print(f"  municípios analisados (2026, >= 3 revendas): {mun.count():,}")
print(f"  correlação de Spearman nacional: {rho_nac:+.3f}\n")

display(reg.groupBy("regiao_nome")
        .agg(F.count("*").alias("municipios"),
             F.round(F.corr("p_rev", "p_pre"), 3).alias("spearman_revendas_x_preco"))
        .orderBy("spearman_revendas_x_preco"))

# COMMAND ----------

# MAGIC %md
# MAGIC A correlação nacional é fraca, mas **dentro de cada região ela aparece**. A
# MAGIC região confunde o agregado: o Norte tem poucas revendas por município e preços
# MAGIC altos por razões logísticas, não por falta de concorrência. Controlar pela
# MAGIC região separa os dois efeitos.

# COMMAND ----------

display(mun.withColumn("faixa",
        F.when(F.col("revendas") < 6, "1. menos de 6")
         .when(F.col("revendas") < 11, "2. de 6 a 10")
         .when(F.col("revendas") < 21, "3. de 11 a 20")
         .otherwise("4. 21 ou mais"))
        .groupBy("faixa")
        .agg(F.count("*").alias("municipios"),
             F.round(F.avg("preco"), 2).alias("preco_medio"))
        .orderBy("faixa"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Como o preço evoluiu, e houve quebras de nível?

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT ano_mes, capitais, preco_medio_painel, indice_base_100,
# MAGIC        variacao_mensal_pct, variacao_acumulada_pct,
# MAGIC        pct_linha_extrema_pobreza, pct_piso_bolsa_familia
# MAGIC FROM workspace.gold.agg_indice_p13_nacional
# MAGIC ORDER BY ano_mes

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Maiores altas mensais: candidatas a quebra de nível
# MAGIC SELECT ano_mes, preco_medio_painel, variacao_mensal_pct
# MAGIC FROM workspace.gold.agg_indice_p13_nacional
# MAGIC WHERE variacao_mensal_pct IS NOT NULL
# MAGIC ORDER BY variacao_mensal_pct DESC
# MAGIC LIMIT 5

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — As regiões se movem juntas?
# MAGIC
# MAGIC Correlação das variações mensais entre regiões, sobre o painel de capitais.
# MAGIC
# MAGIC Com 25 variações mensais a amostra é pequena: os números são **descritivos**,
# MAGIC não inferenciais. Não se deve ler significância estatística aqui.

# COMMAND ----------

import pandas as pd

reg_mes = (spark.table(f"{GOLD}.agg_preco_capital_mes")
           .groupBy("ano_mes", "regiao_nome", "municipio")
           .agg(F.avg("preco_medio").alias("p"))
           .groupBy("ano_mes", "regiao_nome").agg(F.avg("p").alias("preco"))
           .toPandas()
           .pivot(index="ano_mes", columns="regiao_nome", values="preco")
           .sort_index())

variacao = reg_mes.pct_change().dropna()

print("Correlação das variações mensais entre regiões:\n")
print(variacao.corr().round(2).to_string())

print("\n\nCorrelação com defasagem de 1 mês (linha lidera coluna):\n")
lag = pd.DataFrame({a: {b: variacao[a].corr(variacao[b].shift(-1))
                        for b in variacao.columns} for a in variacao.columns})
print(lag.round(2).to_string())

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 7 — Como varia a margem bruta da revenda?
# MAGIC
# MAGIC **Não respondível.** A pergunta permanece no objetivo, conforme orientação do
# MAGIC enunciado, com a limitação documentada.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT count(*)                                              AS registros,
# MAGIC        count(valor_compra)                                   AS com_valor_compra,
# MAGIC        round(100 * count(valor_compra) / count(*), 2)        AS pct_preenchido
# MAGIC FROM workspace.gold.fato_coleta_preco

# COMMAND ----------

# MAGIC %md
# MAGIC A margem bruta exigiria `valor_venda − valor_compra`. O campo existe no layout
# MAGIC da ANP mas está vazio em todos os 271.945 registros: os metadados oficiais
# MAGIC registram que a série de preço de distribuição está disponível apenas até
# MAGIC agosto de 2020.
# MAGIC
# MAGIC A verificação é dupla — documental (metadados da fonte) e empírica (contagem
# MAGIC acima) —, e a impossibilidade é uma característica da fonte, não uma falha do
# MAGIC pipeline.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Conferência com a implementação de referência

# COMMAND ----------

falhas = []
cap = spark.table(f"{GOLD}.agg_preco_capital_mes")
por_cap = cap.groupBy("municipio").agg(F.avg("preco_medio").alias("p"))
disp = spark.table(f"{GOLD}.agg_dispersao_municipio_semana")
ind = spark.table(f"{GOLD}.agg_indice_p13_nacional")

pares = spark.sql("""
  WITH pares AS (
    SELECT l.municipio, t.ano_mes,
           avg(CASE WHEN b.flag_bandeira_branca     THEN f.valor_venda END) AS branca,
           avg(CASE WHEN NOT b.flag_bandeira_branca THEN f.valor_venda END) AS bandeirada
    FROM workspace.gold.fato_coleta_preco f
    JOIN workspace.gold.dim_tempo      t ON f.sk_tempo    = t.sk_tempo
    JOIN workspace.gold.dim_localidade l ON f.sk_local    = l.sk_local
    JOIN workspace.gold.dim_bandeira   b ON f.sk_bandeira = b.sk_bandeira
    GROUP BY l.municipio, t.ano_mes
    HAVING branca IS NOT NULL AND bandeirada IS NOT NULL)
  SELECT count(*) AS n, avg(branca - bandeirada) AS dif FROM pares
""").first()

print("Pergunta 1")
conferir("capital mais barata (R$)", round(por_cap.agg(F.min("p")).first()[0], 2), 93.60, 0.01)
conferir("capital mais cara (R$)",   round(por_cap.agg(F.max("p")).first()[0], 2), 138.23, 0.01)

print("\nPergunta 2")
conferir("grupos município-semana", disp.count(), 24_645)
conferir("economia mediana (R$)",
         round(disp.select(F.expr("percentile(economia_rs, 0.5)")).first()[0], 2), 7.86, 0.01)

print("\nPergunta 3")
conferir("pares município-mês", pares["n"], 5_632)
conferir("diferença média branca (R$)", round(pares["dif"], 2), -1.65, 0.02)

print("\nPergunta 4")
conferir("municípios analisados", mun.count(), 414)
conferir("Spearman nacional", round(rho_nac, 3), -0.071, 0.02)

print("\nPergunta 5")
conferir("índice final", ind.orderBy(F.col("ano_mes").desc()).first()["indice_base_100"],
         110.50, 0.01)

print("\nPergunta 7")
conferir("valor_compra preenchido",
         spark.table(f"{GOLD}.fato_coleta_preco").filter("valor_compra IS NOT NULL").count(), 0)

if falhas:
    raise ValueError(f"Divergência da referência em: {falhas}")
print("\nAnálise conferida contra a implementação de referência.")