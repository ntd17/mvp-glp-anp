# Databricks notebook source
# MAGIC %md
# MAGIC # 03 — Gold: tabela fato
# MAGIC
# MAGIC Liga a Silver às cinco dimensões e materializa o centro do esquema estrela.
# MAGIC
# MAGIC | Entradas | Saída |
# MAGIC |---|---|
# MAGIC | `silver.preco_glp_coleta` + as cinco `gold.dim_*` | `gold.fato_coleta_preco` |
# MAGIC
# MAGIC ## Grão
# MAGIC
# MAGIC **Uma coleta de preço, de um produto, em uma revenda, em uma data.**
# MAGIC
# MAGIC Declarar o grão antes de construir não é formalidade. Ele determina o que pode
# MAGIC ser somado e o que não pode: `valor_venda` é um preço, portanto uma medida
# MAGIC **não aditiva**. Somar preços de revendas diferentes não produz nada com
# MAGIC significado — só médias, medianas e dispersões fazem sentido sobre esta medida.
# MAGIC
# MAGIC ## O risco desta etapa
# MAGIC
# MAGIC Todo join é uma chance de perder linhas (chave ausente na dimensão) ou de
# MAGIC duplicá-las (chave repetida na dimensão). As duas falhas são silenciosas: o
# MAGIC notebook roda, a tabela é criada, e o erro só aparece na análise, como um número
# MAGIC estranho que ninguém sabe explicar.
# MAGIC
# MAGIC Por isso a seção 3 verifica quatro coisas: contagem preservada, nenhuma chave
# MAGIC nula, nenhum órfão, e soma dos preços idêntica à da origem.

# COMMAND ----------

from pyspark.sql import functions as F

SILVER = "workspace.silver.preco_glp_coleta"
GOLD = "workspace.gold"
FATO = f"{GOLD}.fato_coleta_preco"
TOTAL_ESPERADO = 271_945

silver = spark.table(SILVER).filter("NOT flag_rejeitado")

dim_tempo = spark.table(f"{GOLD}.dim_tempo")
dim_local = spark.table(f"{GOLD}.dim_localidade")
dim_revenda = spark.table(f"{GOLD}.dim_revenda")
dim_bandeira = spark.table(f"{GOLD}.dim_bandeira")
dim_produto = spark.table(f"{GOLD}.dim_produto")

origem_linhas = silver.count()
origem_soma = silver.agg(F.sum("valor_venda")).first()[0]

print(f"Silver: {origem_linhas:,} registros")
print(f"Soma de controle (valor_venda): {origem_soma:,.2f}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Construção do fato
# MAGIC
# MAGIC Todos os joins são `inner`. É uma escolha deliberada: um `left join` mascararia
# MAGIC chaves ausentes preenchendo com nulo, enquanto o `inner` faz a linha desaparecer
# MAGIC — e a checagem de contagem da seção 3 detecta o sumiço imediatamente.
# MAGIC
# MAGIC Prefere-se falhar alto a seguir em silêncio.

# COMMAND ----------

fato = (silver
    .join(dim_tempo.select("sk_tempo", "data_coleta"),
          on="data_coleta", how="inner")
    .join(dim_local.select("sk_local", "municipio", "uf_sigla"),
          on=["municipio", "uf_sigla"], how="inner")
    .join(dim_revenda.select("sk_revenda", "cnpj_revenda"),
          on="cnpj_revenda", how="inner")
    .join(dim_bandeira.select("sk_bandeira", "bandeira"),
          on="bandeira", how="inner")
    .join(dim_produto.select("sk_produto", "produto", "unidade_medida"),
          on=["produto", "unidade_medida"], how="inner")
    .select(
        "sk_tempo", "sk_local", "sk_revenda", "sk_bandeira", "sk_produto",
        F.col("valor_venda").alias("valor_venda"),
        F.col("valor_compra").alias("valor_compra"),
        "_arquivo_origem",
    )
    .withColumn("_data_processamento_gold", F.current_timestamp())
)

display(fato.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Persistência
# MAGIC
# MAGIC Sem particionamento. Com 272 mil linhas e cerca de 15 MB, particionar por mês
# MAGIC criaria 26 arquivos pequenos e pioraria a leitura — o clássico problema de
# MAGIC *small files*. O Delta já mantém estatísticas por arquivo que resolvem a
# MAGIC filtragem nesta escala. Particionamento passa a valer na casa das dezenas de
# MAGIC milhões de linhas.

# COMMAND ----------

(fato.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(FATO))

print(f"Tabela gravada: {FATO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Validação de integridade
# MAGIC
# MAGIC Quatro testes, do mais grosseiro ao mais fino.

# COMMAND ----------

f = spark.table(FATO)
falhas = []

# --- Teste 1: contagem preservada ---------------------------------------------
total = f.count()
ok = total == TOTAL_ESPERADO == origem_linhas
if not ok:
    falhas.append("contagem")
print("[1] Contagem")
print(f"  {'OK  ' if ok else 'ERRO'} {total:,} linhas no fato "
      f"(origem {origem_linhas:,}, esperado {TOTAL_ESPERADO:,})")
if total > origem_linhas:
    print(f"       {total - origem_linhas:,} linhas A MAIS: dimensão com chave duplicada")
elif total < origem_linhas:
    print(f"       {origem_linhas - total:,} linhas A MENOS: chave ausente em dimensão")

# --- Teste 2: nenhuma chave nula ----------------------------------------------
print("\n[2] Chaves não nulas")
SKS = ["sk_tempo", "sk_local", "sk_revenda", "sk_bandeira", "sk_produto"]
nulos = f.select([F.sum(F.col(c).isNull().cast("int")).alias(c) for c in SKS]).first()
for c in SKS:
    if nulos[c]:
        falhas.append(f"{c} nulo")
    print(f"  {'OK  ' if nulos[c] == 0 else 'ERRO'} {c:12s} {nulos[c]:,} nulos")

# --- Teste 3: integridade referencial -----------------------------------------
print("\n[3] Integridade referencial (nenhum órfão)")
referencias = [
    ("sk_tempo", dim_tempo), ("sk_local", dim_local), ("sk_revenda", dim_revenda),
    ("sk_bandeira", dim_bandeira), ("sk_produto", dim_produto),
]
for sk, dim in referencias:
    orfaos = f.join(dim.select(sk), on=sk, how="left_anti").count()
    if orfaos:
        falhas.append(f"{sk} órfão")
    print(f"  {'OK  ' if orfaos == 0 else 'ERRO'} {sk:12s} {orfaos:,} órfãos")

# --- Teste 4: soma de controle ------------------------------------------------
print("\n[4] Soma de controle")
soma = f.agg(F.sum("valor_venda")).first()[0]
ok = soma == origem_soma
if not ok:
    falhas.append("soma de controle")
print(f"  {'OK  ' if ok else 'ERRO'} fato {soma:,.2f}  |  origem {origem_soma:,.2f}")

if falhas:
    raise ValueError(f"Integridade comprometida: {falhas}")

print("\nFato validado: nenhuma linha perdida, duplicada ou órfã.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Prova de que a estrela funciona
# MAGIC
# MAGIC A consulta abaixo atravessa o fato e três dimensões ao mesmo tempo. Se o modelo
# MAGIC está correto, ela responde a uma pergunta de negócio sem tocar na Silver.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   l.regiao_nome                                   AS regiao,
# MAGIC   count(*)                                        AS coletas,
# MAGIC   count(DISTINCT f.sk_revenda)                    AS revendas,
# MAGIC   round(avg(f.valor_venda), 2)                    AS preco_medio,
# MAGIC   round(avg(CASE WHEN b.flag_bandeira_branca THEN f.valor_venda END), 2) AS branca,
# MAGIC   round(avg(CASE WHEN NOT b.flag_bandeira_branca THEN f.valor_venda END), 2) AS bandeirada
# MAGIC FROM workspace.gold.fato_coleta_preco f
# MAGIC JOIN workspace.gold.dim_localidade l ON f.sk_local = l.sk_local
# MAGIC JOIN workspace.gold.dim_bandeira   b ON f.sk_bandeira = b.sk_bandeira
# MAGIC GROUP BY l.regiao_nome
# MAGIC ORDER BY preco_medio

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Evolução mensal no painel balanceado
# MAGIC
# MAGIC Segunda demonstração, agora usando o `flag_painel_capitais` da dimensão de
# MAGIC localidade — o atributo que neutraliza a variação de composição da amostra.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   t.ano_mes,
# MAGIC   count(DISTINCT l.municipio)  AS capitais,
# MAGIC   round(avg(f.valor_venda), 2) AS preco_medio
# MAGIC FROM workspace.gold.fato_coleta_preco f
# MAGIC JOIN workspace.gold.dim_tempo      t ON f.sk_tempo = t.sk_tempo
# MAGIC JOIN workspace.gold.dim_localidade l ON f.sk_local = l.sk_local
# MAGIC WHERE l.flag_painel_capitais
# MAGIC GROUP BY t.ano_mes
# MAGIC ORDER BY t.ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Documentação no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.gold.fato_coleta_preco IS
# MAGIC 'Tabela fato do esquema estrela. Grao: uma coleta de preco, de um produto, em uma revenda, em uma data. 271.945 registros entre 2024-07 e 2026-08. Medida valor_venda e NAO ADITIVA: e um preco, portanto admite media, mediana e dispersao, nunca soma. Derivada de workspace.silver.preco_glp_coleta.';
# MAGIC
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN sk_tempo    COMMENT 'Chave estrangeira para gold.dim_tempo.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN sk_local    COMMENT 'Chave estrangeira para gold.dim_localidade.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN sk_revenda  COMMENT 'Chave estrangeira para gold.dim_revenda.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN sk_bandeira COMMENT 'Chave estrangeira para gold.dim_bandeira.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN sk_produto  COMMENT 'Chave estrangeira para gold.dim_produto.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN valor_venda COMMENT 'Medida. Preco de venda ao consumidor final, em reais por botijao de 13 kg. Nao aditiva. Faixa observada: 70,00 a 170,00.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN valor_compra COMMENT 'Medida descontinuada. Preco de distribuicao, nulo em 100 por cento dos registros: a ANP publica a serie apenas ate agosto de 2020. Mantida no modelo para fidelidade a fonte e para o caso de a serie ser retomada.';
# MAGIC ALTER TABLE workspace.gold.fato_coleta_preco ALTER COLUMN _arquivo_origem COMMENT 'Controle: arquivo CSV de origem, propagado desde a Bronze. Permite rastrear qualquer registro ate o arquivo publicado pela ANP.';

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {FATO}"))
