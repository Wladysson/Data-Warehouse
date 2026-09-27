# Config

## Visão Geral

O diretório `configs` concentra as **configurações dos serviços e componentes da infraestrutura Big Data**.

Seu objetivo é separar parâmetros de infraestrutura e execução do código da aplicação.

```text
configs/
├── environments/
├── flume/
├── flink/
├── spark/
├── hadoop/
├── hbase/
└── hive/
```

## Responsabilidades

As configurações definem parâmetros utilizados pelos diferentes componentes da pipeline, como:

- serviços;
- portas;
- hosts;
- parâmetros de execução;
- integração entre componentes;
- ambientes;
- processamento;
- armazenamento.

## Ambientes

O diretório `environments` permite organizar configurações específicas para diferentes contextos de execução.

Conceitualmente:

```text
environments/
├── dev
├── staging
└── prod
```

A separação evita misturar parâmetros de ambientes diferentes.

## Flume

As configurações do Flume definem elementos relacionados à ingestão:

```text
Source
  │
  ▼
Channel
  │
  ▼
Sink
```

Entre os parâmetros podem estar:

- source;
- channel;
- sink;
- caminhos;
- transporte;
- serialização.

## Flink

As configurações do Flink concentram parâmetros relacionados ao processamento streaming.

Podem incluir:

- execução;
- paralelismo;
- checkpoints;
- estado;
- integração com sinks;
- parâmetros do ambiente.

## Spark

As configurações do Spark concentram parâmetros relacionados ao processamento batch, incluindo:

- execução;
- recursos;
- master;
- parâmetros do job;
- integração com HDFS/Hive.

## Hadoop

As configurações Hadoop definem parâmetros necessários aos componentes do ecossistema HDFS, principalmente:

- NameNode;
- DataNode;
- filesystem;
- endereços;
- portas;
- parâmetros do cluster.

## HBase

As configurações HBase definem parâmetros de conexão e execução relacionados ao armazenamento NoSQL.

Podem incluir:

- host;
- porta;
- namespace;
- tabelas;
- conexão Thrift.

No ambiente utilizado pelo projeto, a integração Thrift utiliza a porta `9090` quando habilitada.

## Hive

As configurações Hive abrangem os componentes:

```text
HiveServer2
      │
      ▼
Metastore
      │
      ▼
PostgreSQL
```

Podem incluir parâmetros de:

- conexão;
- Metastore;
- HiveServer2;
- banco de metadados;
- armazenamento.

## Separação de configuração e código

A arquitetura evita inserir parâmetros de infraestrutura diretamente no código sempre que possível.

```text
Código
  │
  └── lógica

Config
  │
  └── ambiente / infraestrutura
```

Essa separação facilita alteração de ambientes e manutenção.

## Docker

As configurações são utilizadas pelos serviços definidos no Docker Compose.

O ambiente pode ser iniciado com:

```bash
docker compose -f docker/docker-compose.yml up -d
```

## Boas práticas

As configurações devem:

- evitar credenciais expostas;
- utilizar variáveis de ambiente quando apropriado;
- manter parâmetros organizados por serviço;
- separar ambientes;
- evitar duplicação;
- permanecer versionadas quando não contiverem segredos.

## Relação com a Pipeline

```text
configs
   │
   ├── Flume
   ├── Flink
   ├── Spark
   ├── Hadoop
   ├── HBase
   └── Hive
        │
        ▼
   Infraestrutura
        │
        ▼
     Pipeline
```

## Resumo

O módulo `config` centraliza os parâmetros necessários para execução dos serviços da arquitetura Big Data, mantendo a configuração separada da lógica de processamento e facilitando a execução da pipeline em diferentes ambientes.