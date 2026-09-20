# Databricks notebook source
# MAGIC %md
# MAGIC # 00 — Ingestão Bronze
# MAGIC
# MAGIC Lê os seis CSVs da Série Histórica de Preços da ANP a partir do Volume do Unity
# MAGIC Catalog e os persiste como uma única tabela Delta na camada Bronze.
# MAGIC
# MAGIC **Princípio da camada Bronze:** os dados são preservados como chegaram da fonte.
# MAGIC Nenhum valor é convertido, limpo ou descartado aqui. As únicas adições são colunas
# MAGIC de controle (proveniência), e a única alteração estrutural é a padronização dos
# MAGIC nomes das colunas para snake_case — necessária porque nomes como `Regiao - Sigla`
# MAGIC exigiriam escape em toda consulta SQL posterior. Os **valores** permanecem intactos.
# MAGIC
# MAGIC | Entrada | Saída |
# MAGIC |---|---|
# MAGIC | `/Volumes/workspace/bronze/raw_anp/*.csv` | `workspace.bronze.preco_glp_raw` |
# MAGIC
# MAGIC Registros esperados: **271.945**

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

VOLUME = "/Volumes/workspace/bronze/raw_anp"
TABELA = "workspace.bronze.preco_glp_raw"
FONTE = "ANP - Serie Historica de Precos de Combustiveis e de GLP"
URL_FONTE = ("https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/"
             "serie-historica-de-precos-de-combustiveis")

TOTAL_ESPERADO = 271_945

# Contagens por arquivo, conforme o manifesto gerado na etapa de coleta.
ESPERADO_POR_ARQUIVO = {
    "glp_2024S2.csv": 38_425,
    "glp_2025S1.csv": 71_780,
    "glp_2025S2.csv": 53_423,
    "glp_2026S1.csv": 80_187,
    "glp_2026M07.csv": 13_775,
    "glp_2026M08.csv": 14_355,
}

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Verificação do cabeçalho
# MAGIC
# MAGIC O schema será declarado explicitamente e aplicado por **posição**, não por nome.
# MAGIC Isso torna a leitura imune ao BOM do UTF-8 que a ANP deixa no início do arquivo,
# MAGIC mas cria uma dependência: a ordem das colunas precisa ser idêntica nos seis
# MAGIC arquivos. A célula abaixo verifica essa premissa antes de qualquer leitura.
# MAGIC
# MAGIC Se a ANP mudar o layout em uma publicação futura, o notebook falha aqui — de
# MAGIC forma visível e com mensagem clara, em vez de produzir dados desalinhados em
# MAGIC silêncio.

# COMMAND ----------

CABECALHO_ESPERADO = [
    "Regiao - Sigla", "Estado - Sigla", "Municipio", "Revenda", "CNPJ da Revenda",
    "Nome da Rua", "Numero Rua", "Complemento", "Bairro", "Cep", "Produto",
    "Data da Coleta", "Valor de Venda", "Valor de Compra", "Unidade de Medida",
    "Bandeira",
]

arquivos = sorted(f.path for f in dbutils.fs.ls(VOLUME) if f.name.endswith(".csv"))
print(f"{len(arquivos)} arquivo(s) encontrado(s) no Volume\n")

divergencias = []
for caminho in arquivos:
    nome = caminho.rsplit("/", 1)[-1]
    linha = spark.read.text(caminho).first()[0].replace("\ufeff", "")
    colunas = [c.strip().strip('"') for c in linha.split(";")]
    if colunas == CABECALHO_ESPERADO:
        print(f"  OK   {nome}")
    else:
        divergencias.append(nome)
        print(f"  FALHA {nome}")
        print(f"        ausentes: {[c for c in CABECALHO_ESPERADO if c not in colunas]}")
        print(f"        novas:    {[c for c in colunas if c not in CABECALHO_ESPERADO]}")

if divergencias:
    raise ValueError(
        f"Layout divergente em: {divergencias}. "
        "Revise CABECALHO_ESPERADO antes de prosseguir."
    )

print(f"\nCabeçalho idêntico nos {len(arquivos)} arquivos. Premissa posicional validada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Schema explícito, todo em texto
# MAGIC
# MAGIC Todas as 16 colunas são declaradas como `string`. Duas razões:
# MAGIC
# MAGIC 1. **Fidelidade.** `Valor de Venda` chega como `"125,00"` e `Data da Coleta` como
# MAGIC    `"01/07/2024"`. Converter aqui seria transformar o dado, o que é papel da Silver.
# MAGIC 2. **Falha visível.** Sem `inferSchema`, uma mudança de tipo na origem não é
# MAGIC    silenciosamente reinterpretada. E a inferência exigiria uma passada extra sobre
# MAGIC    os 45 MB, sem benefício algum aqui.

# COMMAND ----------

COLUNAS = [
    "regiao_sigla", "uf_sigla", "municipio", "revenda_nome", "cnpj_revenda",
    "logradouro", "numero", "complemento", "bairro", "cep", "produto",
    "data_coleta", "valor_venda", "valor_compra", "unidade_medida", "bandeira",
]

schema = StructType([StructField(c, StringType(), True) for c in COLUNAS])

print(f"{len(COLUNAS)} colunas declaradas como string:")
for origem, destino in zip(CABECALHO_ESPERADO, COLUNAS):
    print(f"  {origem:22s} -> {destino}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Leitura e proveniência
# MAGIC
# MAGIC Três colunas de controle são acrescentadas, todas prefixadas com `_` para
# MAGIC distingui-las dos dados da fonte:
# MAGIC
# MAGIC - `_arquivo_origem` — de qual CSV cada linha veio, via `_metadata.file_name`.
# MAGIC   É o que permite rastrear uma anomalia até o arquivo de origem.
# MAGIC - `_data_ingestao` — quando a carga rodou.
# MAGIC - `_fonte` — identificação textual da origem dos dados.

# COMMAND ----------

bronze = (
    spark.read
    .option("header", True)          # pula a linha de cabeçalho
    .option("sep", ";")
    .option("encoding", "UTF-8")
    .schema(schema)                  # aplicado por posição
    .csv(VOLUME)
    .withColumn("_arquivo_origem", F.col("_metadata.file_name"))
    .withColumn("_data_ingestao", F.current_timestamp())
    .withColumn("_fonte", F.lit(FONTE))
)

display(bronze.limit(10))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Persistência como tabela Delta
# MAGIC
# MAGIC `overwrite` torna o notebook idempotente: rodar duas vezes produz o mesmo
# MAGIC resultado, sem duplicar registros. O Delta preserva as versões anteriores
# MAGIC (time travel), então nada é perdido de verdade.

# COMMAND ----------

(bronze.write
 .format("delta")
 .mode("overwrite")
 .option("overwriteSchema", "true")
 .saveAsTable(TABELA))

print(f"Tabela gravada: {TABELA}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Validação da carga
# MAGIC
# MAGIC Confere a contagem total e por arquivo contra o manifesto produzido na coleta.
# MAGIC Se algum número divergir, a carga perdeu ou duplicou linhas e o notebook para.

# COMMAND ----------

lida = spark.table(TABELA)
total = lida.count()

print(f"total na tabela ...... {total:,}")
print(f"total esperado ....... {TOTAL_ESPERADO:,}")
print(f"situação ............. {'OK' if total == TOTAL_ESPERADO else 'DIVERGENTE'}\n")

por_arquivo = {r["_arquivo_origem"]: r["n"] for r in
               lida.groupBy("_arquivo_origem").agg(F.count("*").alias("n")).collect()}

erros = []
for nome, esperado in sorted(ESPERADO_POR_ARQUIVO.items()):
    obtido = por_arquivo.get(nome, 0)
    ok = obtido == esperado
    if not ok:
        erros.append(nome)
    print(f"  {'OK  ' if ok else 'ERRO'} {nome:18s} {obtido:>7,} (esperado {esperado:,})")

if total != TOTAL_ESPERADO or erros:
    raise ValueError(f"Carga divergente do manifesto. Arquivos com erro: {erros or 'nenhum'}")

print("\nCarga conferida contra o manifesto da coleta.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Documentação no Unity Catalog
# MAGIC
# MAGIC Os comentários abaixo alimentam o catálogo de dados da plataforma. Eles passam a
# MAGIC aparecer na interface do Catalog Explorer, e é de lá que sai o screenshot da
# MAGIC etapa de Modelagem e Catálogo de Dados.

# COMMAND ----------

# MAGIC %sql
# MAGIC COMMENT ON TABLE workspace.bronze.preco_glp_raw IS
# MAGIC 'Camada Bronze. Replica fiel da Serie Historica de Precos de Combustiveis e de GLP da ANP (produto GLP P13), periodo 2024-07 a 2026-08. Todas as colunas em texto, sem conversao ou limpeza. Granularidade: uma coleta de preco em uma revenda em uma data. Fonte: pesquisa semanal de precos (LPC) realizada por empresa contratada, em cumprimento ao art. 8o da Lei no 9.478/1997.';
# MAGIC
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN regiao_sigla   COMMENT 'Sigla da regiao geografica da revenda pesquisada. Dominio: N, NE, CO, SE, S.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN uf_sigla       COMMENT 'Sigla da Unidade Federativa da revenda pesquisada. 27 valores distintos.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN municipio      COMMENT 'Nome do municipio da revenda pesquisada, em caixa alta e sem acentos. 421 municipios distintos de cerca de 5.570 no pais.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN revenda_nome   COMMENT 'Razao social da revenda pesquisada.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN cnpj_revenda   COMMENT 'CNPJ da revenda. ATENCAO: formato inconsistente entre arquivos. Cinco arquivos publicam formatado e com espaco a esquerda; glp_2025S2.csv publica apenas digitos. Normalizado na camada Silver.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN logradouro     COMMENT 'Nome do logradouro da revenda pesquisada.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN numero         COMMENT 'Numero do logradouro. Texto, pois admite valores como S/N.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN complemento    COMMENT 'Complemento do endereco. 74,1 por cento de nulos.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN bairro         COMMENT 'Bairro da revenda pesquisada.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN cep            COMMENT 'CEP no formato NNNNN-NNN. Texto, para preservar zeros a esquerda.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN produto        COMMENT 'Combustivel pesquisado. Valor unico nesta base: GLP.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN data_coleta    COMMENT 'Data da coleta do preco, no formato dd/MM/yyyy. Convertida para date na camada Silver.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN valor_venda    COMMENT 'Preco de venda ao consumidor final praticado pela revenda, em reais por botijao de 13 kg. Virgula decimal. Convertido para decimal na camada Silver.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN valor_compra   COMMENT 'Preco de distribuicao (venda da distribuidora para a revenda). DESCONTINUADO: conforme os metadados oficiais da ANP, a serie esta disponivel apenas ate agosto de 2020. Nulo em 100 por cento dos registros deste periodo.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN unidade_medida COMMENT 'Unidade de medida do preco. Valor unico nesta base: R$ / 13 kg.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN bandeira       COMMENT 'Bandeira da revenda: marca comercial da distribuidora exibida, ou BRANCA quando a revenda opta por nao exibir marca. 14 valores distintos.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN _arquivo_origem COMMENT 'Controle: nome do arquivo CSV de origem da linha.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN _data_ingestao  COMMENT 'Controle: timestamp da execucao da carga.';
# MAGIC ALTER TABLE workspace.bronze.preco_glp_raw ALTER COLUMN _fonte          COMMENT 'Controle: identificacao textual da fonte dos dados.';

# COMMAND ----------

display(spark.sql(f"DESCRIBE TABLE EXTENDED {TABELA}"))