# Orchestration

## Visão Geral

O módulo `orchestration` é responsável pela **coordenação da execução dos componentes da pipeline**.

Ele organiza a inicialização, execução e gerenciamento das etapas de processamento, conectando infraestrutura, streaming e batch.

```text
Orchestration
      │
      ├── Infrastructure
      ├── Streaming
      └── Batch
```

## Responsabilidades

O módulo concentra operações relacionadas a:

- inicialização da pipeline;
- execução dos serviços;
- execução dos jobs;
- gerenciamento do ambiente;
- sequência operacional dos componentes;
- parada da pipeline;
- validação do ambiente.

## Fluxo de execução

A execução segue conceitualmente:

```text
Infraestrutura
      │
      ▼
Data Generator
      │
      ▼
Flume
      │
      ▼
Flink
      │
      ├────► HBase
      │
      ▼
Spark
      │
      ├────► HDFS
      └────► Hive
```

## Docker Compose

A infraestrutura é coordenada através do Docker Compose.

Inicialização:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verificação:

```bash
docker compose -f docker/docker-compose.yml ps
```

Parada:

```bash
docker compose -f docker/docker-compose.yml down
```

## Streaming

O processamento contínuo depende da infraestrutura estar disponível antes da execução do job.

A sequência operacional deve garantir:

```text
Containers
   │
   ▼
Flink
   │
   ▼
Streaming Job
```

## Batch

O processamento histórico é executado separadamente do fluxo contínuo.

Exemplo:

```bash
docker exec -it ecommerce-spark spark-submit \
  --master local[*] \
  --deploy-mode client \
  /app/src/batch/spark_job.py
```

A separação permite executar processamento batch sem alterar o fluxo contínuo do streaming.

## Gerenciamento

O módulo também pode ser utilizado para:

- iniciar o ambiente;
- reiniciar serviços;
- executar jobs;
- acompanhar execução;
- interromper a pipeline;
- limpar recursos.

## Ordem operacional

A infraestrutura deve estar disponível antes dos componentes que dependem dela.

```text
1. Infraestrutura
2. Storage
3. Ingestion
4. Streaming
5. Batch
6. Monitoring
```

A ordem exata pode variar conforme as dependências definidas no Docker Compose.

## Diagnóstico

Quando um job não inicia, deve-se verificar primeiro:

```bash
docker compose -f docker/docker-compose.yml ps
```

Depois:

```bash
docker compose -f docker/docker-compose.yml logs
```

E, quando necessário, os logs específicos do serviço.

## Papel na arquitetura

O `orchestration` não substitui os motores de processamento.

Sua função é coordenar:

```text
Orchestration
      │
      ├── Docker
      ├── Flink
      ├── Spark
      └── Storage
```

## Resumo

O módulo `orchestration` organiza a execução operacional da pipeline, garantindo que infraestrutura, serviços e jobs possam ser iniciados, acompanhados e encerrados de maneira coordenada.