# Databricks notebook source
# MAGIC %md
# MAGIC # 04 — Qualidade de Dados
# MAGIC
# MAGIC Perfilagem formal dos atributos, executada sobre a camada **Bronze** — isto é,
# MAGIC antes de qualquer transformação. Medir o defeito no estado em que ele chegou é
# MAGIC o que permite afirmar depois que a correção teve efeito, e quanto.
# MAGIC
# MAGIC Este notebook **não escreve nada**. Ele apenas mede e reporta. As correções
# MAGIC vivem no `01_bronze_para_silver`; aqui está a evidência que as justifica.
# MAGIC
# MAGIC Cinco dimensões avaliadas: completude, consistência, unicidade, acurácia e
# MAGIC outliers. Mais duas verificações específicas deste conjunto: higiene de texto e
# MAGIC estabilidade da cobertura amostral.

# COMMAND ----------

from pyspark.sql import functions as F

BRONZE = "workspace.bronze.preco_glp_raw"
b = spark.table(BRONZE)

COLUNAS = [c for c in b.columns if not c.startswith("_")]
TOTAL = b.count()

print(f"Bronze: {TOTAL:,} registros, {len(COLUNAS)} colunas de dados\n")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Completude e cardinalidade
# MAGIC
# MAGIC Para cada atributo: quantos nulos, em que proporção, e quantos valores
# MAGIC distintos. A cardinalidade revela tanto colunas constantes quanto candidatas
# MAGIC a chave.

# COMMAND ----------

perfil = b.select([
    F.struct(
        F.lit(c).alias("coluna"),
        F.sum(F.col(c).isNull().cast("int")).alias("nulos"),
        F.round(100 * F.avg(F.col(c).isNull().cast("int")), 2).alias("pct_nulo"),
        F.countDistinct(F.col(c)).alias("distintos"),
    ).alias(c) for c in COLUNAS
]).first()

perfil_df = spark.createDataFrame(
    [(perfil[c]["coluna"], perfil[c]["nulos"], float(perfil[c]["pct_nulo"]),
      perfil[c]["distintos"]) for c in COLUNAS],
    "coluna string, nulos long, pct_nulo double, distintos long"
)

display(perfil_df.orderBy(F.col("pct_nulo").desc()))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Consistência de formato
# MAGIC
# MAGIC Cada atributo é confrontado com o padrão que deveria seguir.

# COMMAND ----------

consistencia = b.select(
    F.sum(F.to_date("data_coleta", "dd/MM/yyyy").isNull().cast("int")).alias("data_invalida"),
    F.sum(F.regexp_replace("valor_venda", ",", ".").cast("double").isNull().cast("int")).alias("preco_nao_numerico"),
    F.sum((~F.col("cep").rlike(r"^\d{5}-\d{3}$")).cast("int")).alias("cep_fora_padrao"),
    F.sum((~F.trim("cnpj_revenda").rlike(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$")).cast("int")).alias("cnpj_fora_padrao"),
    F.countDistinct("produto").alias("produtos_distintos"),
    F.countDistinct("unidade_medida").alias("unidades_distintas"),
    F.countDistinct("regiao_sigla").alias("regioes"),
    F.countDistinct("uf_sigla").alias("ufs"),
).first()

for k in consistencia.asDict():
    print(f"  {k:22s} {consistencia[k]:>8,}")

print(f"\n  produtos: {[r[0] for r in b.select('produto').distinct().collect()]}")
print(f"  unidades: {[r[0] for r in b.select('unidade_medida').distinct().collect()]}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. O defeito crítico: formato do CNPJ
# MAGIC
# MAGIC Os CNPJs fora do padrão da seção anterior não estão espalhados: concentram-se
# MAGIC em um único arquivo. A consulta abaixo mostra onde.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   _arquivo_origem,
# MAGIC   CASE WHEN trim(cnpj_revenda) RLIKE '^[0-9]{2}\\.[0-9]{3}\\.[0-9]{3}/[0-9]{4}-[0-9]{2}$'
# MAGIC        THEN 'formatado' ELSE 'apenas digitos' END AS formato,
# MAGIC   CASE WHEN cnpj_revenda <> trim(cnpj_revenda)
# MAGIC        THEN 'com espaco' ELSE 'sem espaco' END      AS espacos,
# MAGIC   count(*) AS registros
# MAGIC FROM workspace.bronze.preco_glp_raw
# MAGIC GROUP BY 1, 2, 3
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### Impacto sobre a contagem de revendas
# MAGIC
# MAGIC O CNPJ é a chave natural da entidade "revenda". Com dois formatos concorrentes,
# MAGIC o mesmo estabelecimento passa a existir como duas entidades distintas conforme
# MAGIC o semestre em que foi pesquisado.

# COMMAND ----------

bruto = b.select("cnpj_revenda").distinct().count()
normalizado = b.select(
    F.lpad(F.regexp_replace("cnpj_revenda", r"\D", ""), 14, "0").alias("cnpj")
).distinct().count()

print(f"  CNPJs distintos, string crua ...... {bruto:,}")
print(f"  CNPJs distintos, normalizados ..... {normalizado:,}")
print(f"  entidades duplicadas .............. {bruto - normalizado:,} "
      f"({100 * (bruto - normalizado) / bruto:.1f}%)")
print(f"\n  Sem a normalização, dim_revenda teria {bruto:,} linhas em vez de "
      f"{normalizado:,} — inflação de {100 * bruto / normalizado - 100:.0f}%.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Higiene de texto
# MAGIC
# MAGIC Espaços nas bordas fragmentam agrupamentos: `' XEREM'` e `'XEREM'` são chaves
# MAGIC diferentes em qualquer `GROUP BY`.

# COMMAND ----------

TEXTO = ["regiao_sigla", "uf_sigla", "municipio", "revenda_nome", "cnpj_revenda",
         "logradouro", "numero", "complemento", "bairro", "cep", "produto",
         "unidade_medida", "bandeira"]

espacos = b.select([
    F.sum((F.col(c) != F.trim(F.col(c))).cast("int")).alias(c) for c in TEXTO
]).first()

achou = False
for c in TEXTO:
    n = espacos[c] or 0
    if n:
        achou = True
        preenchidos = TOTAL - (perfil[c]["nulos"] if c in COLUNAS else 0)
        print(f"  {c:16s} {n:>8,} de {preenchidos:,} ({100 * n / preenchidos:.1f}%)")
if not achou:
    print("  nenhum campo afetado")

so_espaco = b.filter(
    F.col("complemento").isNotNull() & (F.trim("complemento") == "")
).count()
print(f"\n  'complemento' contendo apenas espaços: {so_espaco} "
      "(tratados como nulo na Silver)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Unicidade
# MAGIC
# MAGIC A chave de negócio é CNPJ + data + produto: uma revenda tem um preço por
# MAGIC produto em cada coleta.

# COMMAND ----------

chave = ["cnpj_revenda", "data_coleta", "produto"]
distintas = b.select(*chave).distinct().count()
dup = TOTAL - distintas

print(f"  registros ........................ {TOTAL:,}")
print(f"  combinações distintas da chave ... {distintas:,}")
print(f"  {'OK  ' if dup == 0 else 'ERRO'} duplicatas ................. {dup:,}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Acurácia e outliers
# MAGIC
# MAGIC Critério de Tukey: valores fora de 1,5 vezes o intervalo interquartil.

# COMMAND ----------

v = b.withColumn("preco", F.regexp_replace("valor_venda", ",", ".").cast("double"))
q1, q3 = v.approxQuantile("preco", [0.25, 0.75], 0.0)
iqr = q3 - q1
lim_inf, lim_sup = q1 - 1.5 * iqr, q3 + 1.5 * iqr

est = v.select(
    F.min("preco").alias("min"), F.round(F.avg("preco"), 2).alias("media"),
    F.expr("percentile(preco, 0.5)").alias("mediana"), F.max("preco").alias("max"),
    F.round(F.stddev("preco"), 2).alias("desvio"),
).first()

fora = v.filter((F.col("preco") < lim_inf) | (F.col("preco") > lim_sup)).count()
nao_positivos = v.filter(F.col("preco").isNull() | (F.col("preco") <= 0)).count()

print(f"  min {est['min']:.2f} | p25 {q1:.2f} | mediana {est['mediana']:.2f} | "
      f"p75 {q3:.2f} | max {est['max']:.2f}")
print(f"  média {est['media']:.2f} | desvio {est['desvio']:.2f}")
print(f"\n  limites de Tukey: [{lim_inf:.2f}, {lim_sup:.2f}]")
print(f"  fora dos limites .......... {fora:,} ({100 * fora / TOTAL:.2f}%)")
print(f"  nulos ou não positivos .... {nao_positivos:,}")
print("\n  DECISÃO: manter. Os extremos observados (R$ 70,00 e R$ 170,00) são preços")
print("  plausíveis para um botijão de 13 kg, e a amplitude regional confirmada na")
print("  análise mostra que valores altos refletem geografia real, não erro de coleta.")
print("  Removê-los eliminaria justamente o sinal que o trabalho se propõe a medir.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Campo descontinuado na fonte

# COMMAND ----------

preenchidos = b.filter(F.col("valor_compra").isNotNull()).count()
print(f"  valor_compra preenchido: {preenchidos:,} de {TOTAL:,}")
print("\n  Os metadados oficiais da ANP registram que a série de preço de distribuição")
print("  está disponível apenas até agosto de 2020. A coluna existe no layout mas vem")
print("  vazia em todo o período analisado, o que inviabiliza o cálculo da margem")
print("  bruta da revenda — uma das perguntas de negócio formuladas no objetivo.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 8. Estabilidade da cobertura amostral
# MAGIC
# MAGIC Este não é um defeito de conteúdo, e sim de **comparabilidade**. Se o conjunto
# MAGIC de municípios pesquisados muda mês a mês, a média nacional simples mistura
# MAGIC variação de preço com variação de composição da amostra.

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT
# MAGIC   date_format(to_date(data_coleta, 'dd/MM/yyyy'), 'yyyy-MM')      AS ano_mes,
# MAGIC   count(*)                                                        AS coletas,
# MAGIC   count(DISTINCT municipio)                                       AS municipios,
# MAGIC   count(DISTINCT cnpj_revenda)                                    AS revendas,
# MAGIC   round(avg(replace(valor_venda, ',', '.')), 2)                   AS preco_medio
# MAGIC FROM workspace.bronze.preco_glp_raw
# MAGIC GROUP BY 1
# MAGIC ORDER BY 1

# COMMAND ----------

# MAGIC %md
# MAGIC ### O artefato de composição em flagrante
# MAGIC
# MAGIC Em janeiro de 2025 a média nacional **cai**, exatamente quando o número de
# MAGIC municípios pesquisados quase dobra. A comparação com o painel balanceado de
# MAGIC capitais mostra que a queda era entrada de municípios baratos na amostra, não
# MAGIC barateamento do produto.

# COMMAND ----------

# MAGIC %sql
# MAGIC WITH bruta AS (
# MAGIC   SELECT t.ano_mes,
# MAGIC          round(avg(f.valor_venda), 2)  AS media_bruta,
# MAGIC          count(DISTINCT l.municipio)   AS municipios
# MAGIC   FROM workspace.gold.fato_coleta_preco f
# MAGIC   JOIN workspace.gold.dim_tempo      t ON f.sk_tempo = t.sk_tempo
# MAGIC   JOIN workspace.gold.dim_localidade l ON f.sk_local = l.sk_local
# MAGIC   GROUP BY t.ano_mes
# MAGIC ),
# MAGIC painel AS (
# MAGIC   SELECT t.ano_mes, round(avg(f.valor_venda), 2) AS media_painel
# MAGIC   FROM workspace.gold.fato_coleta_preco f
# MAGIC   JOIN workspace.gold.dim_tempo      t ON f.sk_tempo = t.sk_tempo
# MAGIC   JOIN workspace.gold.dim_localidade l ON f.sk_local = l.sk_local
# MAGIC   WHERE l.flag_painel_capitais
# MAGIC   GROUP BY t.ano_mes
# MAGIC )
# MAGIC SELECT b.ano_mes, b.municipios, b.media_bruta, p.media_painel,
# MAGIC        round(b.media_bruta - p.media_painel, 2) AS diferenca
# MAGIC FROM bruta b JOIN painel p USING (ano_mes)
# MAGIC ORDER BY b.ano_mes

# COMMAND ----------

# MAGIC %md
# MAGIC ## 9. Conferência com a implementação de referência

# COMMAND ----------

checagens = [
    ("registros",                      TOTAL,          271_945),
    ("complemento nulo",               perfil["complemento"]["nulos"], 201_513),
    ("valor_compra preenchido",        preenchidos,    0),
    ("CNPJ com espaço",                espacos["cnpj_revenda"], 218_522),
    ("CNPJ fora do padrão",            consistencia["cnpj_fora_padrao"], 53_423),
    ("CEP fora do padrão",             consistencia["cep_fora_padrao"], 0),
    ("duplicatas na chave",            dup,            0),
    ("outliers de Tukey",              fora,           269),
    ("CNPJs distintos (cru)",          bruto,          8_844),
    ("CNPJs distintos (normalizado)",  normalizado,    5_073),
]

falhas = [r for r, o, e in checagens if o != e]
for rotulo, obtido, esperado in checagens:
    print(f"  {'OK  ' if obtido == esperado else 'ERRO'} {rotulo:30s} "
          f"{obtido:>8,}  (esperado {esperado:,})")

if falhas:
    raise ValueError(f"Divergência da referência em: {falhas}")

print("\nPerfilagem conferida contra a implementação de referência.")
