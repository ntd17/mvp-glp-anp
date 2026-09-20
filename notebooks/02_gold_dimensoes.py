# Databricks notebook source
# MAGIC %md
# MAGIC # 02 — Gold: dimensões do esquema estrela
# MAGIC
# MAGIC Constrói as cinco dimensões que cercam o fato de coleta de preço.
# MAGIC
# MAGIC | Entrada | Saídas |
# MAGIC |---|---|
# MAGIC | `workspace.silver.preco_glp_coleta` | `gold.dim_tempo`, `gold.dim_localidade`, `gold.dim_revenda`, `gold.dim_bandeira`, `gold.dim_produto` |
# MAGIC
# MAGIC ## Por que esquema estrela
# MAGIC
# MAGIC A Silver é uma tabela ampla: cada linha repete o endereço da revenda, o nome do
# MAGIC município e a bandeira. Isso é adequado para limpeza, mas ruim para análise — a
# MAGIC mesma informação de uma revenda aparece repetida em até 577 linhas.
# MAGIC
# MAGIC O esquema estrela separa **o que foi medido** (o preço, no fato) de **o contexto
# MAGIC da medição** (quando, onde, por quem, sob qual marca). Isso torna as consultas
# MAGIC analíticas mais diretas, elimina redundância nas descrições e, principalmente,
# MAGIC dá um lugar único para atributos derivados como `flag_painel_capitais`, que
# MAGIC precisam ser calculados uma vez e reutilizados em várias análises.
# MAGIC
# MAGIC ## Chaves substitutas
# MAGIC
# MAGIC Cada dimensão recebe uma chave substituta (`sk_`) gerada por `row_number` sobre
# MAGIC uma ordenação determinística. Determinística é o ponto: rodar o notebook duas
# MAGIC vezes produz as mesmas chaves, o que não aconteceria com
# MAGIC `monotonically_increasing_id`.

# COMMAND ----------

from pyspark.sql import Window
from pyspark.sql import functions as F

ORIGEM = "workspace.silver.preco_glp_coleta"
SCHEMA_GOLD = "workspace.gold"

silver = spark.table(ORIGEM).filter("NOT flag_rejeitado")
N_MESES = silver.select("ano_mes").distinct().count()

print(f"Silver: {silver.count():,} registros válidos em {N_MESES} meses")

def com_sk(df, nome_sk, ordem):
    """Acrescenta chave substituta determinística a partir de uma ordenação fixa."""
    return df.withColumn(nome_sk, F.row_number().over(Window.orderBy(*ordem)))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. dim_tempo
# MAGIC
# MAGIC Grão: uma data de coleta.
# MAGIC
# MAGIC **Decisão de projeto:** a dimensão contém apenas as datas efetivamente
# MAGIC observadas, não um calendário completo. A pesquisa da ANP é semanal, então um
# MAGIC calendário de 2024-07-01 a 2026-08-31 teria 792 linhas, das quais cerca de 27%
# MAGIC nunca se ligariam a fato algum. Datas sem coleta não são informação ausente —
# MAGIC são dias em que a pesquisa simplesmente não ocorre.

# COMMAND ----------

dim_tempo = com_sk(
    silver.select("data_coleta", "ano", "mes", "ano_mes", "semana_iso", "ano_semana")
          .distinct()
          .withColumn("trimestre", F.quarter("data_coleta"))
          .withColumn("dia_semana", F.date_format("data_coleta", "EEEE")),
    "sk_tempo", ["data_coleta"]
).select("sk_tempo", "data_coleta", "ano", "trimestre", "mes", "ano_mes",
         "semana_iso", "ano_semana", "dia_semana")

dim_tempo.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.dim_tempo")

print(f"dim_tempo: {dim_tempo.count():,} linhas")
display(dim_tempo.orderBy("sk_tempo").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. dim_localidade
# MAGIC
# MAGIC Grão: um município.
# MAGIC
# MAGIC **Chave natural composta `(municipio, uf_sigla)`**, não apenas o nome. Não é
# MAGIC precaução teórica: VALENÇA existe na Bahia e no Rio de Janeiro, e ambas aparecem
# MAGIC nesta base. São 421 nomes distintos para 422 localidades.
# MAGIC
# MAGIC Dois atributos derivados carregam a resposta ao problema de cobertura variável
# MAGIC da amostra:
# MAGIC
# MAGIC - `meses_com_coleta` — em quantos dos 26 meses o município foi pesquisado.
# MAGIC - `flag_painel_capitais` — capital com cobertura em **todos** os meses. É o
# MAGIC   critério objetivo de inclusão no painel balanceado, aplicado aos dados em vez
# MAGIC   de uma lista escolhida a dedo.

# COMMAND ----------

dim_localidade = com_sk(
    silver.groupBy("municipio", "uf_sigla", "regiao_sigla", "regiao_nome")
          .agg(F.countDistinct("cnpj_revenda").alias("qtd_revendas_pesquisadas"),
               F.countDistinct("ano_mes").alias("meses_com_coleta"),
               F.count("*").alias("qtd_coletas"),
               F.max("flag_capital").alias("flag_capital"))
          .withColumn("flag_painel_capitais",
                      F.col("flag_capital") & (F.col("meses_com_coleta") == N_MESES)),
    "sk_local", ["uf_sigla", "municipio"]
).select("sk_local", "municipio", "uf_sigla", "regiao_sigla", "regiao_nome",
         "qtd_revendas_pesquisadas", "qtd_coletas", "meses_com_coleta",
         "flag_capital", "flag_painel_capitais")

dim_localidade.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.dim_localidade")

print(f"dim_localidade: {dim_localidade.count():,} linhas")
print(f"  capitais ................ {dim_localidade.filter('flag_capital').count()}")
print(f"  no painel balanceado .... {dim_localidade.filter('flag_painel_capitais').count()}")

display(dim_localidade.filter("flag_capital AND NOT flag_painel_capitais")
        .select("municipio", "uf_sigla", "meses_com_coleta"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. dim_revenda
# MAGIC
# MAGIC Grão: um CNPJ.
# MAGIC
# MAGIC O CNPJ normalizado na Silver é a chave natural. Sem aquela normalização esta
# MAGIC dimensão teria 8.844 linhas em vez de 5.073, e qualquer métrica baseada em
# MAGIC contagem de estabelecimentos estaria inflada em 74%.
# MAGIC
# MAGIC Atributos cadastrais mudam ao longo do tempo (razão social, endereço). Adota-se
# MAGIC a **última ocorrência observada** — equivalente a uma dimensão de tipo 1, que
# MAGIC sobrescreve o histórico. Para este MVP basta: a análise trata de preço, não da
# MAGIC evolução cadastral das revendas.

# COMMAND ----------

ult = Window.partitionBy("cnpj_revenda").orderBy(F.col("data_coleta").desc())

dim_revenda = com_sk(
    silver.withColumn("rn", F.row_number().over(ult))
          .filter("rn = 1")
          .select("cnpj_revenda", "revenda_nome", "logradouro", "numero",
                  "complemento", "bairro", "cep", "municipio", "uf_sigla"),
    "sk_revenda", ["cnpj_revenda"]
).select("sk_revenda", "cnpj_revenda", "revenda_nome", "logradouro", "numero",
         "complemento", "bairro", "cep", "municipio", "uf_sigla")

dim_revenda.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.dim_revenda")

print(f"dim_revenda: {dim_revenda.count():,} linhas")
display(dim_revenda.orderBy("sk_revenda").limit(5))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. dim_bandeira
# MAGIC
# MAGIC Grão: uma bandeira.
# MAGIC
# MAGIC Pelos metadados da ANP, a revenda bandeirada exibe a marca comercial de uma
# MAGIC distribuidora e comercializa apenas o produto dela; a bandeira branca opta por
# MAGIC não exibir marca alguma. O `flag_bandeira_branca` isola essa distinção, que é
# MAGIC objeto direto de uma das perguntas de negócio.

# COMMAND ----------

dim_bandeira = com_sk(
    silver.groupBy("bandeira")
          .agg(F.count("*").alias("qtd_coletas"),
               F.countDistinct("cnpj_revenda").alias("qtd_revendas"))
          .withColumn("flag_bandeira_branca", F.col("bandeira") == F.lit("BRANCA")),
    "sk_bandeira", ["bandeira"]
).select("sk_bandeira", "bandeira", "flag_bandeira_branca",
         "qtd_revendas", "qtd_coletas")

dim_bandeira.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.dim_bandeira")

print(f"dim_bandeira: {dim_bandeira.count():,} linhas")
display(dim_bandeira.orderBy(F.col("qtd_coletas").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. dim_produto
# MAGIC
# MAGIC Grão: um produto e sua unidade de medida.
# MAGIC
# MAGIC Esta base contém um único produto (GLP, em `R$ / 13 kg`), então a dimensão tem
# MAGIC uma linha só. Ela existe mesmo assim por duas razões: mantém a integridade do
# MAGIC esquema estrela, e é o ponto de extensão natural caso o pipeline passe a ingerir
# MAGIC também gasolina, etanol ou diesel — que a ANP publica na mesma série, no mesmo
# MAGIC layout.

# COMMAND ----------

dim_produto = com_sk(
    silver.groupBy("produto", "unidade_medida")
          .agg(F.count("*").alias("qtd_coletas")),
    "sk_produto", ["produto", "unidade_medida"]
).select("sk_produto", "produto", "unidade_medida", "qtd_coletas")

dim_produto.write.format("delta").mode("overwrite").option("overwriteSchema", "true") \
    .saveAsTable(f"{SCHEMA_GOLD}.dim_produto")

print(f"dim_produto: {dim_produto.count():,} linha(s)")
display(dim_produto)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação

# COMMAND ----------

checagens = [
    ("dim_tempo",       spark.table(f"{SCHEMA_GOLD}.dim_tempo").count(),        577),
    ("dim_localidade",  spark.table(f"{SCHEMA_GOLD}.dim_localidade").count(),   422),
    ("dim_revenda",     spark.table(f"{SCHEMA_GOLD}.dim_revenda").count(),    5_073),
    ("dim_bandeira",    spark.table(f"{SCHEMA_GOLD}.dim_bandeira").count(),      14),
    ("dim_produto",     spark.table(f"{SCHEMA_GOLD}.dim_produto").count(),        1),
    ("capitais no painel",
     spark.table(f"{SCHEMA_GOLD}.dim_localidade").filter("flag_painel_capitais").count(), 25),
]

falhas = [r for r, o, e in checagens if o != e]
for rotulo, obtido, esperado in checagens:
    print(f"  {'OK  ' if obtido == esperado else 'ERRO'} {rotulo:22s} "
          f"{obtido:>7,}  (esperado {esperado:,})")

# Unicidade das chaves naturais: nenhuma dimensão pode ter chave repetida.
unicidade = [
    ("dim_tempo", ["data_coleta"]),
    ("dim_localidade", ["municipio", "uf_sigla"]),
    ("dim_revenda", ["cnpj_revenda"]),
    ("dim_bandeira", ["bandeira"]),
    ("dim_produto", ["produto", "unidade_medida"]),
]
print()
for tabela, chave in unicidade:
    d = spark.table(f"{SCHEMA_GOLD}.{tabela}")
    dup = d.count() - d.select(*chave).distinct().count()
    if dup:
        falhas.append(f"{tabela} (chave duplicada)")
    print(f"  {'OK  ' if dup == 0 else 'ERRO'} chave única em {tabela:18s} "
          f"{'sem duplicatas' if dup == 0 else f'{dup} duplicadas'}")

if falhas:
    raise ValueError(f"Divergência em: {falhas}")

print("\nDimensões validadas.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Documentação no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.dim_tempo IS
# MAGIC 'Dimensao de tempo. Grao: uma data de coleta. Contem apenas datas observadas, nao um calendario completo, pois a pesquisa da ANP e semanal. 577 datas entre 2024-07-01 e 2026-08-31.';
# MAGIC COMMENT ON TABLE workspace.gold.dim_localidade IS
# MAGIC 'Dimensao de localidade. Grao: um municipio, identificado pela chave natural composta municipio + uf_sigla (VALENCA existe em BA e RJ). Inclui atributos de cobertura da pesquisa e o criterio de inclusao no painel balanceado de capitais.';
# MAGIC COMMENT ON TABLE workspace.gold.dim_revenda IS
# MAGIC 'Dimensao de revenda. Grao: um CNPJ normalizado (14 digitos). Atributos cadastrais refletem a ultima ocorrencia observada (tipo 1). 5.073 revendas.';
# MAGIC COMMENT ON TABLE workspace.gold.dim_bandeira IS
# MAGIC 'Dimensao de bandeira. Grao: uma marca comercial de distribuidora, ou BRANCA quando a revenda opta por nao exibir marca. 14 valores.';
# MAGIC COMMENT ON TABLE workspace.gold.dim_produto IS
# MAGIC 'Dimensao de produto. Grao: produto e unidade de medida. Uma linha nesta base (GLP em R$ / 13 kg); existe para integridade do esquema e como ponto de extensao para outros combustiveis da mesma serie da ANP.';
# MAGIC
# MAGIC ALTER TABLE workspace.gold.dim_localidade ALTER COLUMN meses_com_coleta COMMENT 'Numero de meses distintos, dos 26 da serie, em que o municipio foi pesquisado. Varia de 1 a 26: a abrangencia da pesquisa da ANP mudou ao longo do periodo.';
# MAGIC ALTER TABLE workspace.gold.dim_localidade ALTER COLUMN flag_painel_capitais COMMENT 'Verdadeiro para capitais presentes em todos os 26 meses. Criterio objetivo de inclusao no painel balanceado usado no indice temporal; 25 das 27 capitais qualificam.';
# MAGIC ALTER TABLE workspace.gold.dim_localidade ALTER COLUMN qtd_revendas_pesquisadas COMMENT 'Revendas distintas pesquisadas no municipio ao longo da serie. Usada como proxy de concorrencia local.';
# MAGIC ALTER TABLE workspace.gold.dim_revenda ALTER COLUMN cnpj_revenda COMMENT 'Chave natural: CNPJ com 14 digitos, sem pontuacao, normalizado na camada Silver.';