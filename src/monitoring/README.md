# Monitoring

## Visão Geral

O módulo `monitoring` concentra os mecanismos relacionados ao **acompanhamento da execução da pipeline de Big Data**.

Seu objetivo é fornecer informações sobre o estado dos componentes, processamento dos eventos, execução dos jobs e ocorrência de falhas.

```text
Pipeline
   │
   ▼
Monitoring
   │
   ├── Métricas
   ├── Logs
   ├── Alertas
   └── Status
```

## Responsabilidades

O módulo acompanha principalmente:

- execução da pipeline;
- processamento de eventos;
- disponibilidade dos serviços;
- execução de jobs;
- ocorrência de erros;
- métricas operacionais;
- geração de alertas.

## Monitoramento dos componentes

A arquitetura possui diferentes pontos que podem ser observados:

| Componente | Informações relevantes |
|---|---|
| Generator | Eventos produzidos |
| Flume | Ingestão e transporte |
| Flink | Operadores e processamento |
| Spark | Jobs, stages e tasks |
| HDFS | Armazenamento |
| HBase | Persistência NoSQL |
| Hive | Consultas e metadados |

## Logs

Os logs são utilizados para identificar:

- inicialização de serviços;
- execução de jobs;
- processamento;
- exceções;
- falhas de comunicação;
- problemas de configuração.

No ambiente Docker:

```bash
docker compose -f docker/docker-compose.yml ps
```

Os logs podem ser consultados individualmente:

```bash
docker logs -f <container>
```

## Métricas

As métricas podem ser utilizadas para acompanhar aspectos como:

- volume de eventos;
- throughput;
- processamento;
- latência;
- erros;
- disponibilidade;
- execução de jobs.

No Flink, informações adicionais podem ser observadas pela interface Web disponibilizada pelo serviço.

No Spark, a interface de execução permite acompanhar Jobs, Stages, Tasks e Executors.

## Alertas

Os alertas têm como objetivo indicar situações que exigem atenção operacional, como:

- falha de processamento;
- serviço indisponível;
- erro de comunicação;
- interrupção de um job;
- acúmulo de eventos;
- falhas na persistência.

## Diagnóstico

O monitoramento deve permitir relacionar uma falha ao componente responsável.

```text
Erro
 │
 ├── Ingestion?
 ├── Streaming?
 ├── Batch?
 ├── Storage?
 └── Infrastructure?
```

Essa identificação reduz o tempo necessário para diagnosticar problemas.

## Integração

O módulo se relaciona com toda a arquitetura:

```text
Generator ─┐
Flume ─────┤
Flink ─────┤
Spark ─────┼──► Monitoring
HDFS ──────┤
HBase ─────┤
Hive ──────┘
```

## Docker

A execução dos componentes pode ser acompanhada com:

```bash
docker compose -f docker/docker-compose.yml ps
```

E os logs:

```bash
docker compose -f docker/docker-compose.yml logs -f
```

## Objetivo operacional

O monitoring deve fornecer visibilidade suficiente para responder:

- a pipeline está executando?
- os eventos estão sendo processados?
- os jobs terminaram corretamente?
- existem erros?
- os serviços estão disponíveis?
- os dados estão sendo persistidos?

## Resumo

O módulo `monitoring` fornece visibilidade operacional sobre a pipeline, utilizando métricas, logs, status e alertas para acompanhar a execução dos componentes e facilitar diagnóstico e validação.