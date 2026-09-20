#!/usr/bin/env python3
"""
Coleta dos dados brutos de preços de GLP P13 — ANP / Série Histórica de Preços.

Baixa os arquivos que cobrem jul/2024 a ago/2026, preservando os bytes exatamente
como recebidos (nenhuma transformação), e gera um manifesto de proveniência com
hash SHA-256, tamanho, encoding detectado e cabeçalho de cada arquivo.

O manifesto é a evidência da etapa de Coleta: cole a tabela de MANIFEST.md
direto no README do MVP.

Uso:
    python coleta_anp_glp.py                 # salva em ./dados_brutos
    python coleta_anp_glp.py --out /caminho  # salva onde você quiser

Sem dependências externas — roda com Python 3.8+ puro.

Fonte: https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/serie-historica-de-precos-de-combustiveis
Base legal: Lei nº 9.478/1997 (art. 8º), Lei nº 12.527/2011 (LAI),
Decreto nº 8.777/2016 (Política de Dados Abertos).
"""

import argparse
import csv
import hashlib
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

BASE = "https://www.gov.br/anp/pt-br/centrais-de-conteudo/dados-abertos/arquivos/shpc"

# (nome_local, url, periodo_coberto)
# Os semestrais cobrem jul/2024 a jun/2026; os mensais completam jul e ago/2026.
ARQUIVOS = [
    ("glp_2024S2.csv", f"{BASE}/dsas/glp/glp-2024-02.csv", "2024-07 a 2024-12"),
    ("glp_2025S1.csv", f"{BASE}/dsas/glp/glp-2025-01.csv", "2025-01 a 2025-06"),
    ("glp_2025S2.csv", f"{BASE}/dsas/glp/glp-2025-02.csv", "2025-07 a 2025-12"),
    ("glp_2026S1.csv", f"{BASE}/dsas/glp/glp-2026-01.csv", "2026-01 a 2026-06"),
    ("glp_2026M07.csv", f"{BASE}/dsan/2026/07-dados-abertos-precos-glp.csv", "2026-07"),
    ("glp_2026M08.csv", f"{BASE}/dsan/2026/08-dados-abertos-precos-2026-08-glp.csv", "2026-08"),
]

# Dicionário de metadados oficial da ANP — fonte primária para o catálogo de dados.
METADADOS = (
    "metadados_anp.pdf",
    f"{BASE}/metadados-serie-historica-precos-combustiveis-1.pdf",
    "documentação",
)

# O portal gov.br rejeita User-Agent padrão do urllib em algumas rotas.
UA = "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36"

TENTATIVAS = 4
TIMEOUT = 120


def baixar(url, destino, tentativas=TENTATIVAS):
    """Baixa a URL para `destino` preservando os bytes originais."""
    ultimo_erro = None
    for n in range(1, tentativas + 1):
        try:
            req = Request(url, headers={"User-Agent": UA, "Accept": "*/*"})
            with urlopen(req, timeout=TIMEOUT) as resp:
                dados = resp.read()
            destino.write_bytes(dados)
            return len(dados)
        except (HTTPError, URLError, TimeoutError, OSError) as e:
            ultimo_erro = e
            espera = 2 ** n
            print(f"    tentativa {n}/{tentativas} falhou ({e}); aguardando {espera}s")
            time.sleep(espera)
    raise RuntimeError(f"não foi possível baixar {url}: {ultimo_erro}")


def sha256(caminho, bloco=1 << 20):
    h = hashlib.sha256()
    with caminho.open("rb") as f:
        for pedaco in iter(lambda: f.read(bloco), b""):
            h.update(pedaco)
    return h.hexdigest()


def detectar_encoding(bytes_iniciais):
    """Confirma a hipótese de ISO-8859-1 (latin-1) versus UTF-8.

    Todo byte é válido em latin-1, então o teste útil é o inverso: se o trecho
    NÃO decodifica como UTF-8, é indício forte de latin-1 (o caso da ANP).
    """
    try:
        bytes_iniciais.decode("utf-8")
        return "utf-8 (decodificou sem erro)"
    except UnicodeDecodeError:
        return "ISO-8859-1 / latin-1 (falhou como utf-8)"


def inspecionar(caminho):
    """Lê o arquivo sem transformá-lo e extrai fatos para o manifesto."""
    amostra = caminho.read_bytes()[:200_000]
    encoding = detectar_encoding(amostra)
    leitura = "utf-8" if encoding.startswith("utf-8") else "ISO-8859-1"

    with caminho.open("r", encoding=leitura, errors="replace", newline="") as f:
        cabecalho = f.readline().rstrip("\r\n")
        primeira_linha = f.readline().rstrip("\r\n")
        n_linhas = 2 + sum(1 for _ in f)

    delimitador = max(";,\t|", key=cabecalho.count)
    colunas = [c.strip().strip('"') for c in cabecalho.split(delimitador)]

    return {
        "encoding": encoding,
        "delimitador": repr(delimitador),
        "n_colunas": len(colunas),
        "n_linhas_arquivo": n_linhas,
        "n_registros": n_linhas - 1,
        "colunas": colunas,
        "exemplo_linha": primeira_linha[:200],
    }


def comparar_cabecalhos(resultados):
    """Detecta divergência de schema entre os arquivos (schema drift)."""
    csvs = [r for r in resultados if r["nome"].endswith(".csv") and r.get("colunas")]
    if not csvs:
        return []

    referencia = csvs[0]
    avisos = []
    for r in csvs[1:]:
        if r["colunas"] != referencia["colunas"]:
            faltando = [c for c in referencia["colunas"] if c not in r["colunas"]]
            novas = [c for c in r["colunas"] if c not in referencia["colunas"]]
            avisos.append({
                "arquivo": r["nome"],
                "referencia": referencia["nome"],
                "ausentes_em_relacao_a_referencia": faltando,
                "novas_em_relacao_a_referencia": novas,
                "mesma_ordem": False,
            })
    return avisos


def escrever_manifesto(resultados, avisos, pasta):
    """Grava manifest.csv (legível por máquina) e MANIFEST.md (colável no README)."""
    campos = [
        "nome", "url", "periodo", "baixado_em_utc", "bytes",
        "sha256", "encoding", "delimitador", "n_colunas", "n_registros",
    ]
    with (pasta / "manifest.csv").open("w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos, extrasaction="ignore")
        w.writeheader()
        for r in resultados:
            w.writerow(r)

    linhas = [
        "# Manifesto de coleta — Preços de GLP P13 (ANP)",
        "",
        f"Coleta executada em **{datetime.now(timezone.utc):%Y-%m-%d %H:%M} UTC**.",
        "",
        "Fonte: ANP — Série Histórica de Preços de Combustíveis e de GLP.",
        "Pesquisa semanal de preços realizada em cumprimento ao art. 8º da Lei nº 9.478/1997.",
        "",
        "| Arquivo | Período | Registros | Colunas | Bytes | Encoding | SHA-256 (12 pri.) |",
        "|---|---|---:|---:|---:|---|---|",
    ]
    for r in resultados:
        linhas.append(
            f"| `{r['nome']}` | {r['periodo']} | {r.get('n_registros', '—')} | "
            f"{r.get('n_colunas', '—')} | {r['bytes']:,} | {r.get('encoding', '—')} | "
            f"`{r['sha256'][:12]}` |"
        )

    linhas += ["", "## Esquema observado", ""]
    for r in resultados:
        if r.get("colunas"):
            linhas.append(f"**`{r['nome']}`** ({r['n_colunas']} colunas)")
            linhas.append("")
            linhas.append("```")
            linhas.append(" | ".join(r["colunas"]))
            linhas.append("```")
            linhas.append("")

    linhas += ["## Divergência de esquema entre arquivos", ""]
    if avisos:
        linhas.append("Divergências encontradas — tratar explicitamente na carga Bronze → Silver:")
        linhas.append("")
        for a in avisos:
            linhas.append(f"- `{a['arquivo']}` difere de `{a['referencia']}`")
            if a["ausentes_em_relacao_a_referencia"]:
                linhas.append(f"  - ausentes: {a['ausentes_em_relacao_a_referencia']}")
            if a["novas_em_relacao_a_referencia"]:
                linhas.append(f"  - novas: {a['novas_em_relacao_a_referencia']}")
    else:
        linhas.append("Nenhuma. Todos os CSVs apresentam o mesmo cabeçalho, na mesma ordem.")
    linhas.append("")

    (pasta / "MANIFEST.md").write_text("\n".join(linhas), encoding="utf-8")


def main():
    ap = argparse.ArgumentParser(description="Coleta dos CSVs de GLP P13 da ANP.")
    ap.add_argument("--out", default="dados_brutos", help="pasta de destino")
    ap.add_argument("--sem-metadados", action="store_true", help="não baixar o PDF de metadados")
    args = ap.parse_args()

    pasta = Path(args.out)
    pasta.mkdir(parents=True, exist_ok=True)

    alvos = list(ARQUIVOS)
    if not args.sem_metadados:
        alvos.append(METADADOS)

    resultados = []
    falhas = []

    for nome, url, periodo in alvos:
        destino = pasta / nome
        print(f"[{nome}] {url}")
        try:
            tamanho = baixar(url, destino)
        except RuntimeError as e:
            print(f"    FALHOU: {e}")
            falhas.append(nome)
            continue

        registro = {
            "nome": nome,
            "url": url,
            "periodo": periodo,
            "baixado_em_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
            "bytes": tamanho,
            "sha256": sha256(destino),
        }
        if nome.endswith(".csv"):
            registro.update(inspecionar(destino))
            print(f"    {registro['n_registros']:,} registros, "
                  f"{registro['n_colunas']} colunas, {registro['encoding']}")
        else:
            print(f"    {tamanho:,} bytes")

        resultados.append(registro)

    avisos = comparar_cabecalhos(resultados)
    escrever_manifesto(resultados, avisos, pasta)

    print("\n" + "=" * 60)
    print(f"{len(resultados)} arquivo(s) em {pasta.resolve()}")
    if avisos:
        print(f"ATENÇÃO: {len(avisos)} arquivo(s) com cabeçalho divergente — ver MANIFEST.md")
    else:
        print("Cabeçalhos idênticos em todos os CSVs.")
    if falhas:
        print(f"FALHAS: {', '.join(falhas)}")
        return 1
    print("Manifesto: manifest.csv e MANIFEST.md")
    return 0


if __name__ == "__main__":
    sys.exit(main())