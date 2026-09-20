# Databricks notebook source
# MAGIC %md
# MAGIC # 05 — Gold: agregados analíticos
# MAGIC
# MAGIC Três tabelas derivadas do esquema estrela, cada uma desenhada para responder a
# MAGIC um tipo de pergunta. Mais uma tabela de referência com as constantes públicas.
# MAGIC
# MAGIC | Tabela | Grão | Responde |
# MAGIC |---|---|---|
# MAGIC | `ref_linhas_renda` | linha de referência | (constantes) |
# MAGIC | `agg_indice_p13_nacional` | mês | Quanto o botijão subiu no Brasil |
# MAGIC | `agg_preco_capital_mes` | mês × capital | Onde é caro, onde é barato |
# MAGIC | `agg_dispersao_municipio_semana` | município × semana | Quanto se economiza pesquisando |
# MAGIC
# MAGIC ## A armadilha da média simples
# MAGIC
# MAGIC Manaus tem 7.547 coletas na série; Florianópolis tem 609. Um `avg()` direto
# MAGIC sobre o fato faz Manaus pesar doze vezes mais que Florianópolis — e Manaus é
# MAGIC cara (R$ 125,43). O resultado seria uma média ponderada pela **intensidade da
# MAGIC pesquisa**, que é exatamente a variável instável que o painel existe para
# MAGIC neutralizar.
# MAGIC
# MAGIC Por isso o índice é calculado em **dois passos**: primeiro a média de cada
# MAGIC capital no mês, depois a média entre as capitais. Cada capital vale um voto.
# MAGIC É a lógica do cesto fixo, a mesma dos índices de preço tradicionais.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

GOLD = "workspace.gold"
MES_BASE = "2024-07"

fato = spark.table(f"{GOLD}.fato_coleta_preco")
dim_tempo = spark.table(f"{GOLD}.dim_tempo")
dim_local = spark.table(f"{GOLD}.dim_localidade")

base = (fato
    .join(dim_tempo.select("sk_tempo", "data_coleta", "ano_mes", "ano_semana"), "sk_tempo")
    .join(dim_local.select("sk_local", "municipio", "uf_sigla", "regiao_nome",
                           "flag_capital", "flag_painel_capitais"), "sk_local"))

print(f"Base analítica: {base.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Tabela de referência: linhas de renda
# MAGIC
# MAGIC Constantes públicas usadas para expressar o preço em termos de esforço
# MAGIC orçamentário. Todas permaneceram estáveis ao longo dos 26 meses da série — o
# MAGIC reajuste do piso do Bolsa Família para R$ 691 só passou a valer em outubro de
# MAGIC 2026, depois do fim da janela analisada.
# MAGIC
# MAGIC **Ressalva metodológica, e ela é obrigatória:** R$ 218 e R$ 109 são linhas de
# MAGIC **elegibilidade**, não renda observada. As colunas derivadas se chamam
# MAGIC `pct_linha_*`, nunca `pct_renda_*`. Dizer "percentual da renda" seria afirmar
# MAGIC algo que este trabalho não mediu.

# COMMAND ----------

ref = spark.createDataFrame(
    [
        ("linha_extrema_pobreza", 109.00, "pessoa",  "Renda per capita mensal, criterio de extrema pobreza do Cadastro Unico"),
        ("linha_pobreza",         218.00, "pessoa",  "Renda per capita mensal, criterio de pobreza do Cadastro Unico"),
        ("piso_bolsa_familia",    600.00, "familia", "Valor minimo mensal por familia beneficiaria, vigente no periodo analisado"),
    ],
    "referencia string, valor_mensal double, unidade string, descricao string",
)

ref.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.ref_linhas_renda")

display(ref)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. agg_preco_capital_mes
# MAGIC
# MAGIC Grão: mês × capital do painel. **Sem ponderação** — aqui cada capital é uma
# MAGIC observação, e ponderar distorceria a medida de dispersão geográfica.
# MAGIC
# MAGIC Carrega também o esforço orçamentário, expresso em múltiplos das linhas de
# MAGIC referência.

# COMMAND ----------

LINHA_EXTREMA, LINHA_POBREZA, PISO_BF = 109.00, 218.00, 600.00

agg_capital = (base.filter("flag_painel_capitais")
    .groupBy("ano_mes", "municipio", "uf_sigla", "regiao_nome")
    .agg(F.round(F.avg("valor_venda"), 2).alias("preco_medio"),
         F.round(F.expr("percentile(valor_venda, 0.5)"), 2).alias("preco_mediano"),
         F.min("valor_venda").alias("preco_min"),
         F.max("valor_venda").alias("preco_max"),
         F.count("*").alias("coletas"),
         F.countDistinct("sk_revenda").alias("revendas"))
    .withColumn("pct_linha_extrema_pobreza",
                F.round(100 * F.col("preco_medio") / F.lit(LINHA_EXTREMA), 1))
    .withColumn("pct_linha_pobreza",
                F.round(100 * F.col("preco_medio") / F.lit(LINHA_POBREZA), 1))
    .withColumn("pct_piso_bolsa_familia",
                F.round(100 * F.col("preco_medio") / F.lit(PISO_BF), 1)))

agg_capital.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.agg_preco_capital_mes")

print(f"agg_preco_capital_mes: {agg_capital.count():,} linhas "
      f"(25 capitais x 26 meses)")

display(agg_capital.groupBy("municipio", "uf_sigla", "regiao_nome")
        .agg(F.round(F.avg("preco_medio"), 2).alias("preco_medio_periodo"),
             F.round(F.avg("pct_linha_extrema_pobreza"), 1).alias("pct_extrema_pobreza"))
        .orderBy("preco_medio_periodo"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. agg_indice_p13_nacional
# MAGIC
# MAGIC Grão: mês. Construído a partir da tabela anterior — média entre capitais, com
# MAGIC peso igual, sobre o painel balanceado.
# MAGIC
# MAGIC Base: julho de 2024 = 100.
# MAGIC
# MAGIC **Sobre os pesos:** adota-se peso igual entre as 25 capitais. A alternativa
# MAGIC mais rigorosa seria ponderar por número de domicílios do Censo 2022, fixados no
# MAGIC período-base, o que tornaria o índice um Laspeyres próprio. Como o peso igual
# MAGIC sobrerrepresenta Norte e Nordeste em relação ao peso populacional, a
# MAGIC simplificação está registrada nas limitações do trabalho.
# MAGIC
# MAGIC **Sobre arredondamento:** o índice é calculado sobre as médias em precisão
# MAGIC cheia, não sobre os valores já arredondados da tabela anterior. Arredondar um
# MAGIC valor intermediário e depois dividir propaga o erro para o resultado — aqui a
# MAGIC diferença aparece na segunda casa decimal. Arredonda-se apenas na apresentação.

# COMMAND ----------

# O índice parte dos valores NÃO arredondados. Arredondar a média por capital e
# só então dividir propaga erro de arredondamento para o índice — a diferença
# aparece na segunda casa (110,49 em vez de 110,50). A regra é arredondar apenas
# na apresentação, nunca em valor intermediário de cálculo.
por_capital_exato = (base.filter("flag_painel_capitais")
    .groupBy("ano_mes", "municipio")
    .agg(F.avg("valor_venda").alias("preco"),
         F.count("*").alias("coletas")))

ind = (por_capital_exato
    .groupBy("ano_mes")
    .agg(F.avg("preco").alias("preco_exato"),
         F.countDistinct("municipio").alias("capitais"),
         F.sum("coletas").alias("coletas")))

valor_base = ind.filter(F.col("ano_mes") == MES_BASE).first()["preco_exato"]
janela = Window.orderBy("ano_mes")

agg_indice = (ind
    .withColumn("preco_medio_painel", F.round("preco_exato", 2))
    .withColumn("indice_base_100",
                F.round(100 * F.col("preco_exato") / F.lit(valor_base), 2))
    .withColumn("variacao_mensal_pct",
                F.round(100 * (F.col("preco_exato")
                               / F.lag("preco_exato").over(janela) - 1), 2))
    .withColumn("variacao_acumulada_pct",
                F.round(100 * (F.col("preco_exato") / F.lit(valor_base) - 1), 2))
    .withColumn("pct_linha_extrema_pobreza",
                F.round(100 * F.col("preco_exato") / F.lit(LINHA_EXTREMA), 1))
    .withColumn("pct_piso_bolsa_familia",
                F.round(100 * F.col("preco_exato") / F.lit(PISO_BF), 1))
    .drop("preco_exato"))

agg_indice.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.agg_indice_p13_nacional")

display(agg_indice.orderBy("ano_mes"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. agg_dispersao_municipio_semana
# MAGIC
# MAGIC Grão: município × semana ISO. Mede o **ruído acionável** — a dispersão que a
# MAGIC família consegue contornar telefonando para outra revenda da própria cidade.
# MAGIC
# MAGIC Restrito a grupos com pelo menos 5 coletas: com duas ou três revendas, o
# MAGIC coeficiente de variação é instável demais para significar algo.
# MAGIC
# MAGIC Usa todos os municípios, não só as capitais. A dispersão intramunicipal é
# MAGIC medida dentro de cada município, então a composição da amostra não a contamina.

# COMMAND ----------

agg_dispersao = (base
    .groupBy("municipio", "uf_sigla", "regiao_nome", "ano_semana")
    .agg(F.count("*").alias("coletas"),
         F.round(F.avg("valor_venda"), 2).alias("preco_medio"),
         F.round(F.stddev("valor_venda"), 2).alias("desvio_padrao"),
         F.min("valor_venda").alias("preco_min"),
         F.max("valor_venda").alias("preco_max"))
    .filter("coletas >= 5")
    .withColumn("coef_variacao_pct",
                F.round(100 * F.col("desvio_padrao") / F.col("preco_medio"), 2))
    .withColumn("spread_pct",
                F.round(100 * (F.col("preco_max") / F.col("preco_min") - 1), 2))
    .withColumn("economia_rs",
                F.round(F.col("preco_medio") - F.col("preco_min"), 2)))

agg_dispersao.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD}.agg_dispersao_municipio_semana")

resumo = agg_dispersao.select(
    F.count("*").alias("grupos"),
    F.round(F.expr("percentile(coef_variacao_pct, 0.5)"), 1).alias("cv_mediana"),
    F.round(F.expr("percentile(coef_variacao_pct, 0.9)"), 1).alias("cv_p90"),
    F.round(F.expr("percentile(spread_pct, 0.5)"), 1).alias("spread_mediano"),
    F.round(F.expr("percentile(spread_pct, 0.9)"), 1).alias("spread_p90"),
    F.round(F.expr("percentile(economia_rs, 0.5)"), 2).alias("economia_mediana"),
).first()

print(f"  grupos município-semana (>= 5 coletas) ... {resumo['grupos']:,}")
print(f"  coeficiente de variação .... mediana {resumo['cv_mediana']}%  "
      f"| p90 {resumo['cv_p90']}%")
print(f"  spread máx/mín ............. mediana {resumo['spread_mediano']}%  "
      f"| p90 {resumo['spread_p90']}%")
print(f"  economia vs. média ......... R$ {resumo['economia_mediana']} (mediana)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação contra a referência

# COMMAND ----------

primeiro = agg_indice.orderBy("ano_mes").first()
ultimo = agg_indice.orderBy(F.col("ano_mes").desc()).first()

checagens = [
    ("linhas em agg_indice",       agg_indice.count(),                26),
    ("linhas em agg_preco_capital", agg_capital.count(),             650),
    ("linhas em agg_dispersao",    agg_dispersao.count(),        24_645),
    ("preço painel jul/2024 (cent)", int(primeiro["preco_medio_painel"] * 100), 10_497),
    ("preço painel ago/2026 (cent)", int(ultimo["preco_medio_painel"] * 100),   11_598),
    ("índice final (x100)",        int(ultimo["indice_base_100"] * 100),  11_050),
    ("CV mediano (x10)",           int(resumo["cv_mediana"] * 10),           51),
    ("economia mediana (cent)",    int(resumo["economia_mediana"] * 100),   786),
]

falhas = [r for r, o, e in checagens if o != e]
for rotulo, obtido, esperado in checagens:
    print(f"  {'OK  ' if obtido == esperado else 'ERRO'} {rotulo:30s} "
          f"{obtido:>8,}  (esperado {esperado:,})")

if falhas:
    raise ValueError(f"Divergência da referência em: {falhas}")

print(f"\nVariação acumulada em 26 meses: {ultimo['variacao_acumulada_pct']}%")
print("Agregados validados.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Documentação no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.ref_linhas_renda IS
# MAGIC 'Tabela de referencia. Linhas oficiais de renda usadas para expressar o preco em esforco orcamentario. ATENCAO: sao criterios de elegibilidade de programas sociais, nao renda observada da populacao. Valores vigentes durante todo o periodo analisado.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.gold.agg_preco_capital_mes IS
# MAGIC 'Agregado. Grao: mes x capital do painel balanceado (25 capitais x 26 meses = 650 linhas). Sem ponderacao: cada capital e uma observacao. Responde onde o botijao e caro e onde e barato.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.gold.agg_indice_p13_nacional IS
# MAGIC 'Agregado. Grao: mes. Indice de preco do botijao P13 com base julho/2024 = 100, calculado em dois passos (media por capital, depois media entre capitais com peso igual) sobre o painel balanceado de 25 capitais. Indice nao oficial, construido para este MVP, inspirado na metodologia de cesto fixo dos indices de preco tradicionais. Peso igual sobrerrepresenta Norte e Nordeste frente ao peso populacional: ver limitacoes.';
# MAGIC
# MAGIC COMMENT ON TABLE workspace.gold.agg_dispersao_municipio_semana IS
# MAGIC 'Agregado. Grao: municipio x semana ISO, restrito a grupos com ao menos 5 coletas. Mede a dispersao de preco entre revendas da mesma cidade na mesma semana, isto e, a parcela da variacao que o consumidor pode contornar pesquisando.';
# MAGIC
# MAGIC ALTER TABLE workspace.gold.agg_indice_p13_nacional ALTER COLUMN preco_medio_painel COMMENT 'Media simples do preco medio das 25 capitais do painel no mes. Nao e a media das coletas: cada capital tem peso igual.';
# MAGIC ALTER TABLE workspace.gold.agg_preco_capital_mes ALTER COLUMN pct_linha_extrema_pobreza COMMENT 'Preco medio do botijao como percentual da linha de extrema pobreza (R$ 109 per capita/mes). Linha de elegibilidade, nao renda observada.';
# MAGIC ALTER TABLE workspace.gold.agg_dispersao_municipio_semana ALTER COLUMN economia_rs COMMENT 'Diferenca entre o preco medio e o menor preco do municipio na semana: quanto a familia economiza comprando na revenda mais barata da propria cidade.';

# COMMAND ----------

display(spark.sql(f"SHOW TABLES IN {GOLD}"))