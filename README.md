# ETL Relatórios de Produtos 3S

Script em Python para localizar relatórios `.xlsx` na pasta de downloads do projeto, validar o período selecionado, tratar os dados da aba `Report` e gravar os registros na tabela PostgreSQL `relatorio_produtos_3s`.

## Dependências

Instale com:

```bash
pip install -r requirements.txt
```

## Configuração

1. Crie um arquivo `.env` a partir do `.env.example`.
2. Ajuste as credenciais do PostgreSQL.
3. Coloque os arquivos `.xlsx` dentro da pasta configurada em `DOWNLOADS_DIR`.
4. Os logs serão gravados na pasta configurada em `LOGS_DIR`.
5. Se quiser alerta no Telegram, preencha `TELEGRAM_BOT_TOKEN` e `TELEGRAM_CHAT_ID`.

Exemplo:

```env
DB_HOST=177.126.247.194
DB_PORT=5434
POSTGRES_DB=3s_db
POSTGRES_USER=admin
POSTGRES_PASSWORD=123456
DOWNLOADS_DIR=C:\Users\FINANCEIRO\Documents\boh-produtos\downloads
LOGS_DIR=./logs
DB_SCHEMA=public
TELEGRAM_BOT_TOKEN=
TELEGRAM_CHAT_ID=
```

## Execução

```bash
python etl_produtos_3s.py
```

## Regras implementadas

- Processa todos os arquivos `.xlsx` encontrados na pasta de downloads.
- Ignora arquivos temporários do Excel que começam com `~$`.
- Lê a aba `Source Data` para encontrar o campo `Selected Dates`.
- Só processa arquivos cujo período seja de um único dia.
- Se a data inicial e final forem diferentes, lança exatamente este erro:

```text
SÓ É POSSIVEL PROCESSAR INFORMAÇAO DE PRODUTOS REFERENTE A UM DIA
```

- Lê a aba `Report` e mapeia apenas as colunas existentes na tabela.
- Quando o arquivo tiver `Store Code` e `Store`, monta `store` como `0001 - Matriz`.
- Remove espaços extras em campos de texto.
- Converte `PLU (Items)` vazio ou `-` para `NULL`.
- Converte `Qty`, `Price` e `Total` para número.
- Ignora linhas vazias.
- Antes do insert, faz `DELETE` por `data_informacao` e pelas lojas distintas presentes no arquivo.
- Em caso de erro em um arquivo, registra o erro e continua com os próximos.
- Grava logs no terminal e em arquivo dentro da pasta `logs`.
- Envia resumo final para o Telegram no mesmo estilo do bot de referência, quando configurado.

## Logs esperados

O script escreve logs simples no terminal, por exemplo:

```text
2026-03-26 10:00:00 | INFO | Arquivos .xlsx encontrados: 3
2026-03-26 10:00:01 | INFO | Iniciando processamento do arquivo: relatorio_loja_0001.xlsx
2026-03-26 10:00:02 | INFO | Arquivo processado com sucesso: relatorio_loja_0001.xlsx | data=2026-03-26 | lojas=1 | deletados=12 | inseridos=18
2026-03-26 10:00:03 | ERROR | Erro ao processar arquivo relatorio_loja_0002.xlsx: SÓ É POSSIVEL PROCESSAR INFORMAÇAO DE PRODUTOS REFERENTE A UM DIA
2026-03-26 10:00:05 | INFO | Resumo final do ETL
2026-03-26 10:00:05 | INFO | Arquivos processados com sucesso: 1
2026-03-26 10:00:05 | INFO | Arquivos com erro: 1
2026-03-26 10:00:05 | INFO | Total de linhas inseridas: 18
```
