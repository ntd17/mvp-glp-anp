#!/usr/bin/env python3
"""
Implementação de REFERÊNCIA do pipeline de GLP P13 (ANP).

NÃO é a entrega do MVP. É o gabarito: roda em pandas, local, e produz os
números que o pipeline no Databricks deve reproduzir. Se o Databricks bater
com a saída daqui, a implementação em Spark está correta.

A estrutura de camadas e o esquema estrela são idênticos aos que serão
construídos no Databricks, para que a tradução pandas -> PySpark seja direta.

Uso:
    python referencia_pipeline_glp.py --dados /caminho/para/os/csvs
"""

import argparse
import glob
import os
import sys

import numpy as np
import pandas as pd

pd.set_option("display.width", 200)
pd.set_option("display.max_columns", 50)

# Capitais estaduais + DF, como aparecem grafadas na base da ANP (maiúsculas, sem acento).
CAPITAIS = {
    "AC": "RIO BRANCO", "AL": "MACEIO", "AP": "MACAPA", "AM": "MANAUS",
    "BA": "SALVADOR", "CE": "FORTALEZA", "DF": "BRASILIA", "ES": "VITORIA",
    "GO": "GOIANIA", "MA": "SAO LUIS", "MT": "CUIABA", "MS": "CAMPO GRANDE",
    "MG": "BELO HORIZONTE", "PA": "BELEM", "PB": "JOAO PESSOA", "PR": "CURITIBA",
    "PE": "RECIFE", "PI": "TERESINA", "RJ": "RIO DE JANEIRO", "RN": "NATAL",
    "RS": "PORTO ALEGRE", "RO": "PORTO VELHO", "RR": "BOA VISTA",
    "SC": "FLORIANOPOLIS", "SP": "SAO PAULO", "SE": "ARACAJU", "TO": "PALMAS",
}

REGIAO_NOME = {"N": "Norte", "NE": "Nordeste", "CO": "Centro-Oeste",
               "SE": "Sudeste", "S": "Sul"}

# Pesos do índice nacional. Por ora, peso igual entre capitais do painel.
# TODO: substituir por número de domicílios do Censo 2022 (IBGE/SIDRA tabela 4712),
# fixados no período-base — isso torna o índice um Laspeyres próprio.
PESOS = None  # None = peso igual

RENOMEIO = {
    "Regiao - Sigla": "regiao_sigla",
    "Estado - Sigla": "uf_sigla",
    "Municipio": "municipio",
    "Revenda": "revenda_nome",
    "CNPJ da Revenda": "cnpj_revenda",
    "Nome da Rua": "logradouro",
    "Numero Rua": "numero",
    "Complemento": "complemento",
    "Bairro": "bairro",
    "Cep": "cep",
    "Produto": "produto",
    "Data da Coleta": "data_coleta",
    "Valor de Venda": "valor_venda",
    "Valor de Compra": "valor_compra",
    "Unidade de Medida": "unidade_medida",
    "Bandeira": "bandeira",
}


def secao(titulo):
    print("\n" + "=" * 78)
    print(titulo)
    print("=" * 78)


# ---------------------------------------------------------------- BRONZE ----
def carregar_bronze(pasta):
    """Lê os CSVs sem transformar nada, apenas acrescentando proveniência."""
    arquivos = sorted(glob.glob(os.path.join(pasta, "glp_*.csv")))
    if not arquivos:
        sys.exit(f"nenhum arquivo glp_*.csv encontrado em {pasta}")

    partes = []
    for caminho in arquivos:
        d = pd.read_csv(caminho, sep=";", dtype=str, encoding="utf-8")
        # Remove o BOM que o UTF-8 deixa colado no nome da primeira coluna.
        d.columns = [c.replace("\ufeff", "").strip() for c in d.columns]
        d["_arquivo_origem"] = os.path.basename(caminho)
        d["_data_ingestao"] = pd.Timestamp.now("UTC")
        d["_fonte"] = "ANP - Serie Historica de Precos de Combustiveis"
        partes.append(d)

    bronze = pd.concat(partes, ignore_index=True)
    print(f"bronze: {len(bronze):,} registros de {len(arquivos)} arquivos")
    return bronze


# ------------------------------------------------------ QUALIDADE (BRONZE) ---
def qualidade(bronze):
    """Perfilagem de cada atributo. Gera a tabela da seção de Qualidade de Dados."""
    secao("QUALIDADE DE DADOS — perfilagem do Bronze")

    colunas = [c for c in bronze.columns if not c.startswith("_")]
    perfil = pd.DataFrame({
        "nulos": bronze[colunas].isna().sum(),
        "pct_nulo": (100 * bronze[colunas].isna().mean()).round(2),
        "distintos": bronze[colunas].nunique(),
    })
    perfil["exemplo"] = [bronze[c].dropna().iloc[0] if bronze[c].notna().any() else "—"
                         for c in colunas]
    print("\n[Completude e cardinalidade]")
    print(perfil.to_string())

    print("\n[Consistência]")
    dt = pd.to_datetime(bronze["Data da Coleta"], format="%d/%m/%Y", errors="coerce")
    print(f"  datas fora do formato dd/MM/yyyy ........ {dt.isna().sum()}")
    print(f"  período ................................. {dt.min().date()} a {dt.max().date()}")
    v = pd.to_numeric(bronze["Valor de Venda"].str.replace(",", ".", regex=False),
                      errors="coerce")
    print(f"  valores de venda não numéricos .......... {v.isna().sum()}")
    print(f"  produtos distintos ...................... {list(bronze['Produto'].unique())}")
    print(f"  unidades distintas ...................... {list(bronze['Unidade de Medida'].unique())}")
    cep_ok = bronze["Cep"].str.match(r"^\d{5}-\d{3}$", na=False)
    print(f"  CEPs fora do padrão NNNNN-NNN ........... {(~cep_ok).sum()}")
    cnpj_trim = bronze["CNPJ da Revenda"].str.strip()
    cnpj_ok = cnpj_trim.str.match(r"^\d{2}\.\d{3}\.\d{3}/\d{4}-\d{2}$", na=False)
    print(f"  CNPJs fora do padrão (após trim) ........ {(~cnpj_ok).sum()}")

    print("\n[Higiene de texto — espaços nas bordas]")
    achou = False
    for c in colunas:
        s = bronze[c].dropna()
        n = int((s != s.str.strip()).sum())
        if n:
            achou = True
            print(f"  {c:20s} {n:>7,} de {len(s):,} ({100*n/len(s):.1f}%)")
    if not achou:
        print("  nenhum campo afetado")
    so_espaco = int((bronze["Complemento"].fillna("").str.strip() == "").sum()
                    - bronze["Complemento"].isna().sum())
    print(f"  'Complemento' contendo apenas espaços ... {so_espaco} (tratar como nulo)")

    print("\n[Unicidade]")
    chave = ["CNPJ da Revenda", "Data da Coleta", "Produto"]
    dup = bronze.duplicated(subset=chave, keep=False).sum()
    print(f"  linhas em chave duplicada (CNPJ+data+produto) ... {dup}")

    print("\n[Acurácia e outliers — Valor de Venda]")
    q1, q3 = v.quantile(0.25), v.quantile(0.75)
    lim_inf, lim_sup = q1 - 1.5 * (q3 - q1), q3 + 1.5 * (q3 - q1)
    print(f"  min {v.min():.2f} | p25 {q1:.2f} | mediana {v.median():.2f} "
          f"| p75 {q3:.2f} | max {v.max():.2f}")
    print(f"  limites de Tukey: [{lim_inf:.2f}, {lim_sup:.2f}]")
    print(f"  fora dos limites ........................ {((v < lim_inf) | (v > lim_sup)).sum()} "
          f"({100*((v < lim_inf) | (v > lim_sup)).mean():.2f}%)")
    print(f"  valores <= 0 ou nulos ................... {((v <= 0) | v.isna()).sum()}")

    print("\n[Campo descontinuado]")
    print(f"  Valor de Compra preenchidos ............. {bronze['Valor de Compra'].notna().sum()}"
          "  (metadados ANP: série disponível até ago/2020)")


# ---------------------------------------------------------------- SILVER ----
def construir_silver(bronze):
    """Tipagem, padronização e derivações. Nada é descartado em silêncio."""
    s = bronze.rename(columns=RENOMEIO).copy()

    s["data_coleta"] = pd.to_datetime(s["data_coleta"], format="%d/%m/%Y", errors="coerce")
    s["valor_venda"] = pd.to_numeric(s["valor_venda"].str.replace(",", ".", regex=False),
                                     errors="coerce")
    s["valor_compra"] = pd.to_numeric(
        s["valor_compra"].astype(str).str.replace(",", ".", regex=False), errors="coerce")

    for c in ["municipio", "revenda_nome", "bairro", "logradouro", "bandeira",
              "produto", "unidade_medida", "regiao_sigla", "uf_sigla"]:
        s[c] = s[c].astype(str).str.strip().str.upper().replace({"NAN": None})

    s["cnpj_revenda"] = s["cnpj_revenda"].str.replace(r"\D", "", regex=True).str.zfill(14)
    s["cep"] = s["cep"].astype(str).str.replace(r"\D", "", regex=True)

    s["ano"] = s["data_coleta"].dt.year
    s["mes"] = s["data_coleta"].dt.month
    s["ano_mes"] = s["data_coleta"].dt.to_period("M").astype(str)
    s["semana_iso"] = s["data_coleta"].dt.isocalendar().week.astype(int)
    s["ano_semana"] = (s["data_coleta"].dt.isocalendar().year.astype(str) + "-S"
                       + s["data_coleta"].dt.isocalendar().week.astype(str).str.zfill(2))

    s["regiao_nome"] = s["regiao_sigla"].map(REGIAO_NOME)
    s["flag_bandeira_branca"] = s["bandeira"].eq("BRANCA")
    s["flag_capital"] = [CAPITAIS.get(u) == m for u, m in zip(s["uf_sigla"], s["municipio"])]

    # Marcação de registro rejeitado em vez de descarte silencioso.
    s["flag_rejeitado"] = s["valor_venda"].isna() | s["data_coleta"].isna() | (s["valor_venda"] <= 0)

    print(f"silver: {len(s):,} registros | rejeitados: {int(s['flag_rejeitado'].sum())}")
    return s[~s["flag_rejeitado"]].copy()


# ------------------------------------------------------------------ GOLD ----
def construir_gold(silver):
    """Esquema estrela: um fato e cinco dimensões."""
    secao("MODELAGEM — esquema estrela (Gold)")

    dim_tempo = (silver[["data_coleta", "ano", "mes", "ano_mes", "semana_iso", "ano_semana"]]
                 .drop_duplicates("data_coleta").sort_values("data_coleta").reset_index(drop=True))
    dim_tempo.insert(0, "sk_tempo", dim_tempo.index + 1)

    dim_local = (silver.groupby(["municipio", "uf_sigla", "regiao_sigla", "regiao_nome"],
                                as_index=False)
                 .agg(qtd_revendas_pesquisadas=("cnpj_revenda", "nunique"),
                      meses_com_coleta=("ano_mes", "nunique")))
    dim_local["flag_capital"] = [CAPITAIS.get(u) == m
                                 for u, m in zip(dim_local["uf_sigla"], dim_local["municipio"])]
    n_meses = silver["ano_mes"].nunique()
    dim_local["flag_painel_capitais"] = (dim_local["flag_capital"]
                                         & dim_local["meses_com_coleta"].eq(n_meses))
    dim_local.insert(0, "sk_local", range(1, len(dim_local) + 1))

    dim_revenda = (silver.sort_values("data_coleta")
                   .groupby("cnpj_revenda", as_index=False)
                   .agg(revenda_nome=("revenda_nome", "last"), logradouro=("logradouro", "last"),
                        bairro=("bairro", "last"), cep=("cep", "last"),
                        municipio=("municipio", "last"), uf_sigla=("uf_sigla", "last")))
    dim_revenda.insert(0, "sk_revenda", range(1, len(dim_revenda) + 1))

    dim_bandeira = (silver.groupby("bandeira", as_index=False)
                    .agg(qtd_coletas=("valor_venda", "size")))
    dim_bandeira["flag_bandeira_branca"] = dim_bandeira["bandeira"].eq("BRANCA")
    dim_bandeira.insert(0, "sk_bandeira", range(1, len(dim_bandeira) + 1))

    dim_produto = (silver.groupby(["produto", "unidade_medida"], as_index=False)
                   .agg(qtd_coletas=("valor_venda", "size")))
    dim_produto.insert(0, "sk_produto", range(1, len(dim_produto) + 1))

    fato = (silver
            .merge(dim_tempo[["sk_tempo", "data_coleta"]], on="data_coleta")
            .merge(dim_local[["sk_local", "municipio", "uf_sigla"]], on=["municipio", "uf_sigla"])
            .merge(dim_revenda[["sk_revenda", "cnpj_revenda"]], on="cnpj_revenda")
            .merge(dim_bandeira[["sk_bandeira", "bandeira"]], on="bandeira")
            .merge(dim_produto[["sk_produto", "produto", "unidade_medida"]],
                   on=["produto", "unidade_medida"])
            [["sk_tempo", "sk_local", "sk_revenda", "sk_bandeira", "sk_produto",
              "valor_venda", "valor_compra", "_arquivo_origem"]])

    for nome, t in [("dim_tempo", dim_tempo), ("dim_localidade", dim_local),
                    ("dim_revenda", dim_revenda), ("dim_bandeira", dim_bandeira),
                    ("dim_produto", dim_produto), ("fato_coleta_preco", fato)]:
        print(f"  {nome:22s} {len(t):>8,} linhas  |  {len(t.columns)} colunas")

    assert len(fato) == len(silver), "o fato perdeu linhas em algum join"
    print("  join do fato conferido: nenhuma linha perdida")
    return dim_tempo, dim_local, dim_revenda, dim_bandeira, dim_produto, fato


def painel_capitais(silver):
    """Capitais com cobertura em todos os meses da série."""
    cap = silver[silver["flag_capital"]]
    meses = silver["ano_mes"].nunique()
    completas = cap.groupby("municipio")["ano_mes"].nunique()
    return sorted(completas[completas == meses].index), sorted(completas[completas < meses].index)


def serie_indice(silver, municipios, pesos=None):
    """Média por capital, depois agregação entre capitais com pesos fixos."""
    s = silver[silver["municipio"].isin(municipios)]
    por_cap = s.groupby(["ano_mes", "municipio"])["valor_venda"].mean().reset_index()
    if pesos:
        por_cap["w"] = por_cap["municipio"].map(pesos).fillna(0)
    else:
        por_cap["w"] = 1.0
    agg = por_cap.groupby("ano_mes").apply(
        lambda g: np.average(g["valor_venda"], weights=g["w"]), include_groups=False)
    return agg.sort_index()


# -------------------------------------------------------------- ANÁLISES ----
def analises(silver):
    painel, fora = painel_capitais(silver)
    idx = serie_indice(silver, painel, PESOS)
    base = idx.iloc[0]

    secao("PERGUNTA 1 — variação de preço entre regiões e UFs")
    cap = silver[silver["municipio"].isin(painel)]
    por_reg = (cap.groupby(["regiao_nome", "municipio"])["valor_venda"].mean()
               .groupby("regiao_nome").agg(preco_medio="mean", capitais="size")
               .sort_values("preco_medio"))
    por_reg["vs_mais_barata_%"] = (100 * (por_reg["preco_medio"] / por_reg["preco_medio"].min() - 1))
    print(por_reg.round(2).to_string())

    primeiro, ultimo = sorted(cap["ano_mes"].unique())[:6], sorted(cap["ano_mes"].unique())[-6:]
    r1 = cap[cap["ano_mes"].isin(primeiro)].groupby("regiao_nome")["valor_venda"].mean().rank()
    r2 = cap[cap["ano_mes"].isin(ultimo)].groupby("regiao_nome")["valor_venda"].mean().rank()
    print(f"\n  correlação de postos entre 1os e últimos 6 meses: {r1.corr(r2, method='spearman'):.3f}")

    print("\n  Capitais extremas:")
    m = cap.groupby(["municipio", "uf_sigla", "regiao_nome"])["valor_venda"].mean().sort_values()
    print(m.head(5).round(2).to_string())
    print("  ...")
    print(m.tail(5).round(2).to_string())
    print(f"\n  amplitude: {100*(m.max()/m.min()-1):.1f}%  "
          f"(R$ {m.min():.2f} a R$ {m.max():.2f})")

    secao("PERGUNTA 2 — dispersão dentro do mesmo município e semana")
    g = (silver.groupby(["municipio", "uf_sigla", "ano_semana"])["valor_venda"]
         .agg(n="size", media="mean", dp="std", minimo="min", maximo="max").reset_index())
    g = g[g["n"] >= 5]
    g["cv_%"] = 100 * g["dp"] / g["media"]
    g["spread_%"] = 100 * (g["maximo"] / g["minimo"] - 1)
    g["economia_rs"] = g["media"] - g["minimo"]
    print(f"  grupos município-semana com >= 5 coletas: {len(g):,}")
    print(f"  coeficiente de variação — mediana {g['cv_%'].median():.1f}% | "
          f"p90 {g['cv_%'].quantile(0.9):.1f}%")
    print(f"  spread máx/mín        — mediana {g['spread_%'].median():.1f}% | "
          f"p90 {g['spread_%'].quantile(0.9):.1f}%")
    print(f"  economia ao comprar no mais barato vs média: "
          f"R$ {g['economia_rs'].median():.2f} (mediana)")

    secao("PERGUNTA 3 — bandeira branca versus bandeirada")
    silver2 = silver.copy()
    tipo = np.where(silver2["flag_bandeira_branca"], "branca", "bandeirada")
    silver2["tipo"] = tipo
    par = (silver2.groupby(["municipio", "uf_sigla", "ano_mes", "tipo"])["valor_venda"]
           .mean().unstack("tipo").dropna())
    par["dif"] = par["branca"] - par["bandeirada"]
    print(f"  pares município-mês com os dois tipos: {len(par):,}")
    print(f"  diferença média (branca - bandeirada): R$ {par['dif'].mean():+.2f} "
          f"({100*par['dif'].mean()/par['bandeirada'].mean():+.1f}%)")
    print(f"  mediana: R$ {par['dif'].median():+.2f} | "
          f"% de pares em que a branca é mais barata: {100*(par['dif'] < 0).mean():.1f}%")

    por_reg_b = (par.reset_index()
                 .merge(silver[["municipio", "regiao_nome"]].drop_duplicates(), on="municipio")
                 .groupby("regiao_nome")["dif"].agg(["mean", "size"]).round(2))
    print("\n  por região:")
    print(por_reg_b.to_string())

    secao("PERGUNTA 4 — concorrência local e preço")
    rec = silver[silver["ano"] == 2026]
    mun = (rec.groupby(["municipio", "uf_sigla", "regiao_nome"])
           .agg(revendas=("cnpj_revenda", "nunique"), preco=("valor_venda", "mean")).reset_index())
    mun = mun[mun["revendas"] >= 3]
    print(f"  municípios analisados (2026, >= 3 revendas): {len(mun):,}")
    print(f"  correlação de Spearman revendas x preço: "
          f"{mun['revendas'].corr(mun['preco'], method='spearman'):+.3f}")
    q = pd.qcut(mun["revendas"], 4, labels=False, duplicates="drop")
    nomes = {i: n for i, n in enumerate(["Q1 (menos)", "Q2", "Q3", "Q4 (mais)"][: q.nunique()])}
    mun["quartil"] = q.map(nomes)
    print("\n  preço médio por quartil de nº de revendas:")
    print(mun.groupby("quartil", observed=True)
          .agg(preco=("preco", "mean"), municipios=("preco", "size")).round(2).to_string())
    print("\n  mesma correlação, dentro de cada região:")
    print(mun.groupby("regiao_nome")
          .apply(lambda d: d["revendas"].corr(d["preco"], method="spearman"),
                 include_groups=False).round(3).to_string())

    secao("PERGUNTA 5 — evolução temporal e quebras de nível")
    bruta = silver.groupby("ano_mes")["valor_venda"].mean()
    comp = pd.DataFrame({"media_bruta": bruta, "painel_capitais": idx})
    comp["indice"] = (100 * idx / base).round(2)
    comp["var_mes_%"] = (100 * idx.pct_change()).round(2)
    comp["municipios_na_amostra"] = silver.groupby("ano_mes")["municipio"].nunique()
    print(comp.round(2).to_string())
    print(f"\n  variação acumulada — bruta: {100*(bruta.iloc[-1]/bruta.iloc[0]-1):.2f}%  |  "
          f"painel: {100*(idx.iloc[-1]/idx.iloc[0]-1):.2f}%")
    print("\n  maiores altas mensais do painel:")
    print(comp["var_mes_%"].nlargest(3).to_string())

    secao("PERGUNTA 6 — as regiões se movem juntas?")
    reg_m = (silver[silver["municipio"].isin(painel)]
             .groupby(["ano_mes", "regiao_nome"])["valor_venda"].mean().unstack())
    var = reg_m.pct_change().dropna()
    print("  correlação das variações mensais entre regiões:")
    print(var.corr().round(2).to_string())
    print("\n  correlação com defasagem de 1 mês (linha lidera coluna):")
    lag = pd.DataFrame({a: {b: var[a].corr(var[b].shift(-1)) for b in var.columns}
                        for a in var.columns}).round(2)
    print(lag.to_string())

    secao("PERGUNTA 7 — margem bruta da revenda")
    print(f"  registros com Valor de Compra preenchido: "
          f"{int(silver['valor_compra'].notna().sum())} de {len(silver):,}")
    print("  NÃO RESPONDÍVEL. Os metadados oficiais da ANP registram que a série de")
    print("  preço de distribuição está disponível apenas até agosto de 2020.")

    secao("PAINEL DE CAPITAIS")
    print(f"  capitais com cobertura completa ({silver['ano_mes'].nunique()} meses): {len(painel)}")
    print(f"  {', '.join(painel)}")
    print(f"  excluídas por cobertura incompleta: {', '.join(fora) if fora else 'nenhuma'}")


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dados", default=".", help="pasta com os arquivos glp_*.csv")
    args = ap.parse_args()

    bronze = carregar_bronze(args.dados)
    qualidade(bronze)
    silver = construir_silver(bronze)
    construir_gold(silver)
    analises(silver)

    secao("FIM")
    print("Compare estes números com a saída do pipeline no Databricks.")


if __name__ == "__main__":
    main()
