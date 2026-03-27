import json
import logging
import os
import re
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Iterable, List
from urllib import parse, request

import pandas as pd
import psycopg2
from dotenv import load_dotenv
from psycopg2.extras import execute_batch


ERRO_DATA_INVALIDA = (
    "S\u00d3 \u00c9 POSSIVEL PROCESSAR INFORMA\u00c7AO DE PRODUTOS REFERENTE A UM DIA"
)
COLUNAS_OBRIGATORIAS_REPORT = [
    "Store",
    "PLU",
    "PLU (Items)",
    "Name",
    "Type",
    "Qty",
    "Price",
    "Total",
]
COLUNA_STORE_CODE = "Store Code"
COLUNAS_TEXTO = ["store", "plu", "plu_item", "nome_produto", "tipo"]
NOME_TABELA = "relatorio_produtos_3s"
SCHEMA_PADRAO = "public"


@dataclass
class ConfiguracaoBanco:
    host: str
    port: int
    database: str
    user: str
    password: str
    schema: str


def obter_variavel_ambiente(*nomes: str) -> str | None:
    for nome in nomes:
        valor = os.getenv(nome)
        if valor:
            return valor
    return None


def configurar_logs(logs_dir: Path) -> Path:
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_file = logs_dir / f"etl_produtos_3s_{datetime.now():%Y%m%d}.log"

    formatter = logging.Formatter(
        fmt="%(asctime)s | %(levelname)s | %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    file_handler = logging.FileHandler(log_file, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    return log_file


def send_telegram_message(message: str) -> None:
    token = os.getenv("TELEGRAM_BOT_TOKEN") or os.getenv("TELEGRAM_TOKEN")
    chat_id = os.getenv("TELEGRAM_CHAT_ID")
    if not token or not chat_id:
        logging.info("TELEGRAM_IGNORADO | motivo=token_ou_chat_id_ausente")
        return

    url = f"https://api.telegram.org/bot{token}/sendMessage"
    payload = parse.urlencode({"chat_id": chat_id, "text": message}).encode("utf-8")
    req = request.Request(url, data=payload, method="POST")
    req.add_header("Content-Type", "application/x-www-form-urlencoded")

    try:
        with request.urlopen(req, timeout=20) as resp:
            body = resp.read().decode("utf-8", errors="ignore")
            data = json.loads(body)
            if not data.get("ok"):
                raise RuntimeError(f"Resposta Telegram invalida: {body}")
        logging.info("TELEGRAM_SUCESSO")
    except Exception as exc:
        logging.error("TELEGRAM_FALHA | erro=%s", f"{type(exc).__name__}: {exc}")


def montar_mensagem_telegram_resumo(
    *,
    erros: int,
    total_arquivos: int,
    total_lojas: int,
    vazios: int,
    total_linhas_inseridas: int,
    sucessos: int,
) -> str:
    status = (
        "✅ ETL de produtos concluído com sucesso"
        if erros == 0
        else "⚠️ ETL de produtos concluído com pendências"
    )
    detalhe_final = (
        "📌 Todas as lojas foram inseridas/atualizadas corretamente no banco de dados."
        if erros == 0
        else (
            f"📌 {sucessos} relatórios foram inseridos/atualizados no banco e "
            f"{erros} apresentaram problema."
        )
    )

    return (
        f"{status}\n"
        f"🕒 Quando: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
        f"📄 Relatórios encontrados: {total_arquivos}\n"
        f"🏬 Lojas processadas: {total_lojas}\n"
        f"📭 Arquivos sem dados: {vazios}\n"
        f"📥 Linhas inseridas no processamento: {total_linhas_inseridas}\n"
        f"{detalhe_final}"
    )


def carregar_configuracao() -> tuple[ConfiguracaoBanco, Path, Path]:
    load_dotenv(dotenv_path=Path.cwd() / ".env")

    variaveis_obrigatorias = {
        "DB_HOST/POSTGRES_HOST": obter_variavel_ambiente("DB_HOST", "POSTGRES_HOST"),
        "DB_PORT/POSTGRES_PORT": obter_variavel_ambiente("DB_PORT", "POSTGRES_PORT"),
        "POSTGRES_DB/DB_NAME": obter_variavel_ambiente("POSTGRES_DB", "DB_NAME"),
        "POSTGRES_USER/DB_USER": obter_variavel_ambiente("POSTGRES_USER", "DB_USER"),
        "POSTGRES_PASSWORD/DB_PASSWORD": obter_variavel_ambiente(
            "POSTGRES_PASSWORD", "DB_PASSWORD"
        ),
    }

    ausentes = [nome for nome, valor in variaveis_obrigatorias.items() if not valor]
    if ausentes:
        raise ValueError(
            "Variáveis de ambiente obrigatórias ausentes: "
            + ", ".join(sorted(ausentes))
        )

    downloads_dir = Path(os.getenv("DOWNLOADS_DIR", Path.cwd() / "downloads")).resolve()
    logs_dir = Path(os.getenv("LOGS_DIR", Path.cwd() / "logs")).resolve()
    config = ConfiguracaoBanco(
        host=variaveis_obrigatorias["DB_HOST/POSTGRES_HOST"],
        port=int(variaveis_obrigatorias["DB_PORT/POSTGRES_PORT"]),
        database=variaveis_obrigatorias["POSTGRES_DB/DB_NAME"],
        user=variaveis_obrigatorias["POSTGRES_USER/DB_USER"],
        password=variaveis_obrigatorias["POSTGRES_PASSWORD/DB_PASSWORD"],
        schema=os.getenv("DB_SCHEMA", SCHEMA_PADRAO),
    )
    return config, downloads_dir, logs_dir


def conectar_banco(config: ConfiguracaoBanco):
    return psycopg2.connect(
        host=config.host,
        port=config.port,
        dbname=config.database,
        user=config.user,
        password=config.password,
        options=f"-c search_path={config.schema}",
    )


def nome_tabela_qualificada(schema: str) -> str:
    return f"{schema}.{NOME_TABELA}"


def localizar_arquivos(downloads_dir: Path) -> List[Path]:
    if not downloads_dir.exists():
        logging.warning("Pasta de downloads não encontrada: %s", downloads_dir)
        return []

    arquivos = sorted(
        arquivo
        for arquivo in downloads_dir.glob("*.xlsx")
        if not arquivo.name.startswith("~$")
    )
    logging.info("Arquivos .xlsx encontrados: %s", len(arquivos))
    return arquivos


def extrair_data_informacao(caminho_arquivo: Path) -> date:
    df_source_data = pd.read_excel(
        caminho_arquivo,
        sheet_name="Source Data",
        header=None,
        engine="openpyxl",
        dtype=str,
    )
    df_source_data = df_source_data.fillna("")

    for _, linha in df_source_data.iterrows():
        valores = [str(valor).strip() for valor in linha.tolist() if str(valor).strip()]
        for indice, valor in enumerate(valores):
            if valor == "Selected Dates":
                if indice + 1 >= len(valores):
                    raise ValueError("Campo 'Selected Dates' sem valor correspondente.")
                return validar_selected_dates(valores[indice + 1])

    raise ValueError("Campo 'Selected Dates' não encontrado na aba 'Source Data'.")


def validar_selected_dates(valor: str) -> date:
    texto = " ".join(str(valor).split())
    partes = re.split(r"\s+at\u00e9\s+", texto, maxsplit=1, flags=re.IGNORECASE)
    if len(partes) != 2:
        raise ValueError(
            "Valor do campo 'Selected Dates' fora do formato esperado: "
            f"{valor!r}"
        )

    data_inicial_str, data_final_str = [parte.strip() for parte in partes]
    data_inicial = datetime.strptime(data_inicial_str, "%m/%d/%Y").date()
    data_final = datetime.strptime(data_final_str, "%m/%d/%Y").date()

    if data_inicial != data_final:
        raise ValueError(ERRO_DATA_INVALIDA)

    return data_inicial


def ler_relatorio(caminho_arquivo: Path) -> pd.DataFrame:
    df_report = pd.read_excel(
        caminho_arquivo,
        sheet_name="Report",
        engine="openpyxl",
    )
    df_report.columns = [str(coluna).strip() for coluna in df_report.columns]

    if len(df_report.columns) == 1:
        nome_coluna = str(df_report.columns[0]).strip().lower()
        primeiro_valor = None if df_report.empty else df_report.iloc[0, 0]
        primeiro_texto = str(primeiro_valor).strip().lower()
        if nome_coluna.startswith("unnamed") and primeiro_texto == "no data found":
            raise ValueError("ARQUIVO SEM DADOS")

    faltantes = [coluna for coluna in COLUNAS_OBRIGATORIAS_REPORT if coluna not in df_report.columns]
    if faltantes:
        raise ValueError(
            "Colunas obrigatórias ausentes na aba 'Report': " + ", ".join(faltantes)
        )

    colunas_selecionadas = list(COLUNAS_OBRIGATORIAS_REPORT)
    if COLUNA_STORE_CODE in df_report.columns:
        colunas_selecionadas.append(COLUNA_STORE_CODE)

    return df_report[colunas_selecionadas].copy()


def converter_numero(valor):
    if pd.isna(valor):
        return None
    if isinstance(valor, (int, float)):
        return valor

    texto = str(valor).strip()
    if not texto:
        return None

    texto = texto.replace("R$", "").replace(" ", "")
    if "," in texto and "." in texto:
        texto = texto.replace(".", "").replace(",", ".")
    elif "," in texto:
        texto = texto.replace(",", ".")

    try:
        return float(texto)
    except ValueError:
        return None


def normalizar_store_code(valor) -> str | None:
    if pd.isna(valor):
        return None

    if isinstance(valor, int):
        return str(valor).zfill(4)

    if isinstance(valor, float):
        if valor.is_integer():
            return str(int(valor)).zfill(4)
        return str(valor).strip()

    texto = " ".join(str(valor).split())
    if not texto:
        return None

    if texto.isdigit():
        return texto.zfill(4)

    return texto


def normalizar_dados(df_report: pd.DataFrame, data_informacao: date) -> pd.DataFrame:
    df = df_report.rename(
        columns={
            "Store": "store",
            "PLU": "plu",
            "PLU (Items)": "plu_item",
            "Name": "nome_produto",
            "Type": "tipo",
            "Qty": "quantidade",
            "Price": "preco_unitario",
            "Total": "valor_total",
        }
    ).copy()

    # Remove linhas completamente vazias antes de aplicar qualquer regra.
    df = df.dropna(how="all")

    if COLUNA_STORE_CODE in df.columns:
        df[COLUNA_STORE_CODE] = df[COLUNA_STORE_CODE].map(normalizar_store_code)
        df["store"] = df.apply(
            lambda linha: (
                f"{linha[COLUNA_STORE_CODE]} - {linha['store']}"
                if pd.notna(linha.get(COLUNA_STORE_CODE)) and pd.notna(linha.get("store"))
                else linha.get("store")
            ),
            axis=1,
        )

    for coluna in COLUNAS_TEXTO:
        df[coluna] = (
            df[coluna]
            .replace({pd.NA: None})
            .map(lambda valor: " ".join(str(valor).split()) if pd.notna(valor) else None)
        )

    df["plu_item"] = df["plu_item"].replace({"": None, "-": None})
    df["tipo"] = df["tipo"].replace({"": None})

    for coluna in ["quantidade", "preco_unitario", "valor_total"]:
        df[coluna] = df[coluna].map(converter_numero)

    colunas_obrigatorias = ["store", "plu", "nome_produto", "quantidade", "preco_unitario", "valor_total"]
    df = df.dropna(subset=colunas_obrigatorias, how="any")

    df["data_informacao"] = data_informacao
    df["data_informacao"] = pd.to_datetime(df["data_informacao"]).dt.date

    df = df[
        [
            "data_informacao",
            "store",
            "plu",
            "plu_item",
            "nome_produto",
            "tipo",
            "quantidade",
            "preco_unitario",
            "valor_total",
        ]
    ].reset_index(drop=True)

    return df


def deletar_registros_existentes(conn, data_informacao: date, stores: Iterable[str], tabela: str) -> int:
    lojas = sorted({store for store in stores if store})
    if not lojas:
        return 0

    with conn.cursor() as cursor:
        cursor.execute(
            f"""
            DELETE FROM {tabela}
            WHERE data_informacao = %s
              AND store = ANY(%s)
            """,
            (data_informacao, lojas),
        )
        return cursor.rowcount


def inserir_registros(conn, df: pd.DataFrame, tabela: str) -> int:
    if df.empty:
        return 0

    registros = [
        (
            linha.data_informacao,
            linha.store,
            linha.plu,
            linha.plu_item,
            linha.nome_produto,
            linha.tipo,
            linha.quantidade,
            linha.preco_unitario,
            linha.valor_total,
        )
        for linha in df.itertuples(index=False)
    ]

    with conn.cursor() as cursor:
        execute_batch(
            cursor,
            f"""
            INSERT INTO {tabela} (
                data_informacao,
                store,
                plu,
                plu_item,
                nome_produto,
                tipo,
                quantidade,
                preco_unitario,
                valor_total
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            registros,
            page_size=500,
        )

    return len(registros)


def processar_arquivo(conn, caminho_arquivo: Path, tabela: str) -> tuple[int, List[str]]:
    logging.info("Iniciando processamento do arquivo: %s", caminho_arquivo.name)

    data_informacao = extrair_data_informacao(caminho_arquivo)
    df_report = ler_relatorio(caminho_arquivo)
    df_normalizado = normalizar_dados(df_report, data_informacao)

    stores = df_normalizado["store"].dropna().unique().tolist()
    registros_deletados = deletar_registros_existentes(conn, data_informacao, stores, tabela)
    registros_inseridos = inserir_registros(conn, df_normalizado, tabela)

    logging.info(
        "Arquivo processado com sucesso: %s | data=%s | lojas=%s | deletados=%s | inseridos=%s",
        caminho_arquivo.name,
        data_informacao.isoformat(),
        len(stores),
        registros_deletados,
        registros_inseridos,
    )
    return registros_inseridos, sorted(stores)


def main() -> None:
    try:
        config, downloads_dir, logs_dir = carregar_configuracao()
        log_file = configurar_logs(logs_dir)
        logging.info("Log do ETL: %s", log_file)
    except Exception as exc:
        logging.basicConfig(level=logging.ERROR, format="%(asctime)s | %(levelname)s | %(message)s")
        logging.error("Falha ao carregar configuração: %s", exc)
        raise

    arquivos = localizar_arquivos(downloads_dir)
    if not arquivos:
        logging.info("Nenhum arquivo encontrado para processamento.")
        return

    lojas_processadas: set[str] = set()
    sucessos = 0
    erros = 0
    vazios = 0
    total_linhas_inseridas = 0
    tabela = nome_tabela_qualificada(config.schema)

    try:
        conn = conectar_banco(config)
    except Exception as exc:
        logging.error("ERRO_CONEXAO_DB | erro=%s", f"{type(exc).__name__}: {exc}")
        send_telegram_message(
            (
                "Atualizacao dos produtos: nao foi possivel concluir a insercao no banco de dados.\n"
                f"Quando: {datetime.now().strftime('%d/%m/%Y %H:%M')}\n"
                "O sistema nao conseguiu conectar ao banco de dados.\n"
                "A equipe tecnica ja pode verificar o motivo no log."
            )
        )
        raise

    with conn:
        for arquivo in arquivos:
            try:
                linhas_inseridas, lojas_arquivo = processar_arquivo(conn, arquivo, tabela)
                conn.commit()
                sucessos += 1
                total_linhas_inseridas += linhas_inseridas
                lojas_processadas.update(lojas_arquivo)

                logging.info(
                    "COMMIT_SUCESSO | arquivo=%s | lojas=%s | linhas=%s",
                    arquivo.name,
                    ",".join(lojas_arquivo),
                    linhas_inseridas,
                )
            except Exception as exc:
                conn.rollback()
                if str(exc) == "ARQUIVO SEM DADOS":
                    vazios += 1
                    logging.warning(
                        "ARQUIVO_SEM_DADOS | arquivo=%s",
                        arquivo.name,
                    )
                else:
                    erros += 1
                    logging.error(
                        "COMMIT_FALHA | arquivo=%s | erro=%s",
                        arquivo.name,
                        f"{type(exc).__name__}: {exc}",
                    )

    logging.info("Resumo final do ETL")
    logging.info("Arquivos processados com sucesso: %s", sucessos)
    logging.info("Arquivos sem dados: %s", vazios)
    logging.info("Arquivos com erro: %s", erros)
    logging.info("Total de linhas inseridas: %s", total_linhas_inseridas)

    total_arquivos = len(arquivos)
    total_lojas = len(lojas_processadas)
    send_telegram_message(
        montar_mensagem_telegram_resumo(
            erros=erros,
            total_arquivos=total_arquivos,
            total_lojas=total_lojas,
            vazios=vazios,
            total_linhas_inseridas=total_linhas_inseridas,
            sucessos=sucessos,
        )
    )


if __name__ == "__main__":
    main()
