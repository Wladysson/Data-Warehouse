# Apache Flume

O Apache Flume é responsável pela **ingestão contínua dos eventos JSON gerados pela pipeline**, atuando como camada de desacoplamento entre a geração dos dados e o armazenamento no Hadoop.

## Responsabilidade

O agente Flume realiza o seguinte fluxo:

```text
Python Data Generator
        │
        ▼
data/raw/events.jsonl
        │
        ▼
Flume Source
        │
        ▼
File Channel
        │
        ▼
HDFS Sink
        │
        ▼
HDFS /data/raw/events/
```

## Source

O `Exec Source` utiliza `tail -F` para acompanhar continuamente o arquivo de eventos:

```text
data/raw/events.jsonl
```

Dessa forma, novos eventos produzidos pelo gerador são capturados pelo Flume sem necessidade de reinicialização do agente.

## Channel

O `File Channel` fornece persistência local dos eventos enquanto eles aguardam processamento pelo sink.

Essa configuração reduz o risco de perda de eventos quando o HDFS estiver temporariamente indisponível.

## Sink

O `HDFS Sink` persiste os eventos brutos no HDFS utilizando particionamento temporal:

```text
/data/raw/events/YYYY/MM/DD/HH/
```

Os arquivos são armazenados no formato JSON textual, preservando os eventos originais para posterior processamento.

## Rotação dos arquivos

O sink utiliza rotação baseada em:

* intervalo de tempo;
* tamanho máximo do arquivo;
* quantidade de eventos processados por lote.

A rotação temporal ocorre a cada 60 segundos, enquanto o tamanho máximo configurado é de 128 MB.

## Integração com a pipeline

Os dados persistidos pelo Flume representam a **camada Raw do Data Lake**.

Posteriormente:

* **Flink** utiliza os eventos para processamento em streaming;
* **Spark** utiliza os dados históricos para processamento batch;
* **Hive** recebe os dados consolidados para análise;
* **HBase** recebe os resultados de baixa latência produzidos pelo processamento de streaming.

O Flume, portanto, não realiza transformação de negócio dos eventos. Sua responsabilidade é garantir a **ingestão contínua e confiável dos dados brutos** no ecossistema Hadoop.
