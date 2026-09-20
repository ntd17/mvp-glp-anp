# Databricks notebook source
# MAGIC %md
# MAGIC # 01 — Bronze para Silver
# MAGIC
# MAGIC Transforma a réplica bruta em uma tabela tipada, padronizada e enriquecida com
# MAGIC atributos derivados.
# MAGIC
# MAGIC | Entrada | Saída |
# MAGIC |---|---|
# MAGIC | `workspace.bronze.preco_glp_raw` | `workspace.silver.preco_glp_coleta` |
# MAGIC
# MAGIC **Transformações aplicadas**
# MAGIC
# MAGIC | # | Transformação | Motivo |
# MAGIC |---|---|---|
# MAGIC | 1 | `valor_venda` para `decimal(10,2)` | Vírgula decimal impede cálculo |
# MAGIC | 2 | `data_coleta` para `date` | Formato `dd/MM/yyyy` é texto |
# MAGIC | 3 | `trim` + `upper` nos textos | 219.558 valores com espaços nas bordas |
# MAGIC | 4 | `cnpj_revenda` para 14 dígitos | Formato divergente entre arquivos |
# MAGIC | 5 | `complemento` vazio para nulo | 199 registros contêm apenas espaços |
# MAGIC | 6 | Derivação temporal | Suporte às análises por mês e semana |
# MAGIC | 7 | Flags de capital e bandeira branca | Sustentam o painel e a comparação |
# MAGIC
# MAGIC **Princípio:** nenhuma linha é descartada. Registros problemáticos recebem
# MAGIC `flag_rejeitado = true` e permanecem na tabela, preservando a rastreabilidade.
# MAGIC A filtragem, quando necessária, acontece na camada Gold.

# COMMAND ----------

from itertools import chain

from pyspark.sql import functions as F

ORIGEM = "workspace.bronze.preco_glp_raw"
DESTINO = "workspace.silver.preco_glp_coleta"
TOTAL_ESPERADO = 271_945

# Capitais estaduais e Distrito Federal, grafadas como aparecem na base da ANP.
CAPITAIS = {
    "AC": "RIO BRANCO", "AL": "MACEIO", "AP": "MACAPA", "AM": "MANAUS",
    "BA": "SALVADOR", "CE": "FORTALEZA", "DF": "BRASILIA", "ES": "VITORIA",
    "GO": "GOIANIA", "MA": "SAO LUIS", "MT": "CUIABA", "MS": "CAMPO GRANDE",
    "MG": "BELO HORIZONTE", "PA": "BELEM", "PB": "JOAO PESSOA", "PR": "CURITIBA",
    "PE": "RECIFE", "PI": "TERESINA", "RJ": "RIO DE JANEIRO", "RN": "NATAL",
    "RS": "PORTO ALEGRE", "RO": "PORTO VELHO", "RR": "BOA VISTA",
    "SC": "FLORIANOPOLIS", "SP": "SAO PAULO", "SE": "ARACAJU", "TO": "PALMAS",
}

REGIOES = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste",
           "SE": "Sudeste", "S": "Sul"}

bronze = spark.table(ORIGEM)
print(f"Bronze: {bronze.count():,} registros")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Diagnóstico antes da transformação
# MAGIC
# MAGIC Mede o estado do CNPJ na origem. O número obtido aqui é a evidência de que a
# MAGIC normalização da etapa seguinte tem efeito real — e de quanto.

# COMMAND ----------

antes = bronze.select("cnpj_revenda").distinct().count()

por_formato = (bronze
    .withColumn("formato", F.when(
        F.trim("cnpj_revenda").rlike(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$"), "formatado")
        .otherwise("apenas digitos"))
    .groupBy("_arquivo_origem", "formato")
    .agg(F.count("*").alias("registros"))
    .orderBy("_arquivo_origem"))

print(f"CNPJs distintos na Bronze (string crua): {antes:,}\n")
display(por_formato)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Tipagem e padronização
# MAGIC
# MAGIC Sobre o CNPJ: `regexp_replace` remove tudo que não é dígito e `lpad` restaura
# MAGIC eventuais zeros à esquerda perdidos. O resultado é independente do formato de
# MAGIC origem, o que unifica a mesma revenda em uma única chave.
# MAGIC
# MAGIC Sobre o `upper`: além do `trim`, a caixa é padronizada para que agrupamentos por
# MAGIC município, bairro ou bandeira não se fragmentem por diferença de grafia.

# COMMAND ----------

CAMPOS_TEXTO = ["regiao_sigla", "uf_sigla", "municipio", "revenda_nome",
                "logradouro", "numero", "complemento", "bairro",
                "produto", "unidade_medida", "bandeira"]

silver = bronze

for campo in CAMPOS_TEXTO:
    silver = silver.withColumn(
        campo,
        F.nullif(F.upper(F.trim(F.col(campo))), F.lit(""))  # vazio vira nulo
    )

silver = (silver
    .withColumn("cnpj_revenda",
                F.lpad(F.regexp_replace(F.col("cnpj_revenda"), r"\D", ""), 14, "0"))
    .withColumn("cep", F.regexp_replace(F.col("cep"), r"\D", ""))
    .withColumn("valor_venda",
                F.regexp_replace(F.col("valor_venda"), ",", ".").cast("decimal(10,2)"))
    .withColumn("valor_compra",
                F.regexp_replace(F.col("valor_compra"), ",", ".").cast("decimal(10,2)"))
    .withColumn("data_coleta", F.to_date(F.col("data_coleta"), "dd/MM/yyyy"))
)

display(silver.select("cnpj_revenda", "municipio", "valor_venda",
                      "data_coleta", "bandeira").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Atributos derivados
# MAGIC
# MAGIC `ano_semana` usa `YEAROFWEEK`, não o ano civil. Na virada do ano, um dia de
# MAGIC 31 de dezembro pode pertencer à semana 1 do ano seguinte pelo padrão ISO; usar
# MAGIC o ano civil criaria uma semana fantasma.
# MAGIC
# MAGIC `flag_capital` compara o município com a capital da respectiva UF, via mapa
# MAGIC literal. É o atributo que sustenta o painel balanceado da camada Gold.

# COMMAND ----------

mapa_capitais = F.create_map([F.lit(v) for v in chain(*CAPITAIS.items())])
mapa_regioes = F.create_map([F.lit(v) for v in chain(*REGIOES.items())])

silver = (silver
    .withColumn("ano", F.year("data_coleta"))
    .withColumn("mes", F.month("data_coleta"))
    .withColumn("ano_mes", F.date_format("data_coleta", "yyyy-MM"))
    .withColumn("semana_iso", F.weekofyear("data_coleta"))
    .withColumn("ano_semana",
                F.concat_ws("-S",
                            F.expr("extract(YEAROFWEEK FROM data_coleta)"),
                            F.lpad(F.weekofyear("data_coleta").cast("string"), 2, "0")))
    .withColumn("regiao_nome", mapa_regioes[F.col("regiao_sigla")])
    .withColumn("flag_bandeira_branca", F.col("bandeira") == F.lit("BRANCA"))
    .withColumn("flag_capital",
                F.coalesce(F.col("municipio") == mapa_capitais[F.col("uf_sigla")],
                           F.lit(False)))
    .withColumn("flag_rejeitado",
                F.col("valor_venda").isNull()
                | F.col("data_coleta").isNull()
                | (F.col("valor_venda") <= 0))
    .withColumn("_data_processamento_silver", F.current_timestamp())
)

display(silver.select("municipio", "uf_sigla", "regiao_nome", "ano_mes",
                      "ano_semana", "flag_capital", "flag_bandeira_branca",
                      "flag_rejeitado").limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistência

# COMMAND ----------

(silver.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(DESTINO))

print(f"Tabela gravada: {DESTINO}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação
# MAGIC
# MAGIC Confere os resultados contra a implementação de referência. Qualquer divergência
# MAGIC interrompe o notebook.

# COMMAND ----------

s = spark.table(DESTINO)

total = s.count()
rejeitados = s.filter("flag_rejeitado").count()
depois = s.select("cnpj_revenda").distinct().count()
municipios = s.select("municipio").distinct().count()
capitais = s.filter("flag_capital").select("municipio").distinct().count()
meses = s.select("ano_mes").distinct().count()
compra = s.filter(F.col("valor_compra").isNotNull()).count()

checagens = [
    ("registros preservados",      total,      TOTAL_ESPERADO),
    ("registros rejeitados",       rejeitados, 0),
    ("CNPJs distintos (Silver)",   depois,     5_073),
    ("municípios distintos",       municipios, 421),
    ("capitais identificadas",     capitais,   27),
    ("meses distintos",            meses,      26),
    ("valor_compra preenchido",    compra,     0),
]

falhas = []
for rotulo, obtido, esperado in checagens:
    ok = obtido == esperado
    if not ok:
        falhas.append(rotulo)
    print(f"  {'OK  ' if ok else 'ERRO'} {rotulo:28s} {obtido:>8,}  (esperado {esperado:,})")

print(f"\nCNPJs: {antes:,} na Bronze -> {depois:,} na Silver")
print(f"Entidades duplicadas eliminadas: {antes - depois:,} "
      f"({100 * (antes - depois) / antes:.1f}%)")

if falhas:
    raise ValueError(f"Divergência da referência em: {falhas}")

print("\nSilver validada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Perfil dos dados tipados

# COMMAND ----------

display(s.filter("NOT flag_rejeitado").select(
    F.min("data_coleta").alias("data_min"),
    F.max("data_coleta").alias("data_max"),
    F.round(F.min("valor_venda"), 2).alias("preco_min"),
    F.round(F.avg("valor_venda"), 2).alias("preco_medio"),
    F.round(F.max("valor_venda"), 2).alias("preco_max"),
    F.countDistinct("bandeira").alias("bandeiras"),
))

# COMMAND ----------

display(s.groupBy("regiao_nome")
        .agg(F.count("*").alias("coletas"),
             F.countDistinct("municipio").alias("municipios"),
             F.round(F.avg("valor_venda"), 2).alias("preco_medio"))
        .orderBy("preco_medio"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 7. Documentação no Unity Catalog

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.silver.preco_glp_coleta IS
# MAGIC 'Camada Silver. Coletas de preco de GLP P13 tipadas, padronizadas e enriquecidas, periodo 2024-07 a 2026-08. Granularidade: uma coleta de preco em uma revenda em uma data. Nenhum registro e descartado: problemas sao sinalizados por flag_rejeitado. Derivada de workspace.bronze.preco_glp_raw.';
# MAGIC
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN cnpj_revenda COMMENT 'CNPJ da revenda normalizado para 14 digitos, sem pontuacao. Corrige a divergencia de formato entre arquivos de origem, que inflava a contagem de revendas de 5.073 para 8.844.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN valor_venda COMMENT 'Preco de venda ao consumidor final, em reais por botijao de 13 kg. Faixa observada: 70,00 a 170,00. Convertido de texto com virgula decimal.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN valor_compra COMMENT 'Preco de distribuicao. Descontinuado pela ANP em agosto de 2020: nulo em 100 por cento dos registros deste periodo.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN data_coleta COMMENT 'Data da coleta do preco. Faixa: 2024-07-01 a 2026-08-31.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN ano_mes COMMENT 'Competencia no formato yyyy-MM. 26 valores distintos.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN ano_semana COMMENT 'Semana ISO no formato yyyy-Sww, usando YEAROFWEEK para evitar inconsistencia na virada do ano.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN regiao_nome COMMENT 'Nome da regiao geografica por extenso. Dominio: Norte, Nordeste, Centro-Oeste, Sudeste, Sul.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN flag_capital COMMENT 'Verdadeiro quando o municipio e a capital da respectiva UF. Sustenta o painel balanceado da camada Gold.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN flag_bandeira_branca COMMENT 'Verdadeiro quando a revenda nao exibe marca comercial de distribuidora (bandeira BRANCA).';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN flag_rejeitado COMMENT 'Verdadeiro quando o registro falha em validacao basica: preco nulo ou nao positivo, ou data invalida. Zero ocorrencias neste conjunto.';
# MAGIC ALTER TABLE workspace.silver.preco_glp_coleta ALTER COLUMN _data_processamento_silver COMMENT 'Controle: timestamp da execucao desta transformacao.';