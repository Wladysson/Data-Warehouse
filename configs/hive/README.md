# Apache Hive

O Apache Hive é responsável pela camada de **Data Warehouse analítico** da pipeline de Big Data. Ele fornece uma camada SQL sobre os dados armazenados no HDFS, permitindo consultas analíticas sobre as informações processadas pelo Apache Spark.

Na arquitetura deste projeto, o Hive representa a camada analítica consolidada:

```text
Python Generator
       │
       ▼
    Apache Flume
       │
       ▼
      HDFS
       │
       ▼
 Apache Spark
       │
       ├── Raw → Clean
       │
       └── Clean → Curated
                    │
                    ▼
                 Apache Hive
                    │
                    ▼
             Consultas SQL analíticas
```

## Responsabilidades

O Hive é utilizado para:

* disponibilizar os dados processados através de SQL;
* atuar como Data Warehouse da solução;
* organizar tabelas fato e dimensão;
* armazenar dados analíticos no HDFS;
* trabalhar com tabelas particionadas;
* disponibilizar metadados através do Hive Metastore;
* permitir consultas sobre a camada Curated;
* integrar-se ao processamento batch realizado pelo Spark.

## Hive Metastore

O Hive Metastore mantém os metadados das tabelas utilizadas pelo Data Warehouse.

Nesta arquitetura, o serviço é disponibilizado através de:

```text
thrift://hive-metastore:9083
```

O Metastore mantém informações como:

* bancos de dados;
* tabelas;
* colunas;
* tipos de dados;
* partições;
* localização física dos dados;
* propriedades das tabelas.

O armazenamento físico continua sendo responsabilidade do HDFS.

Portanto:

```text
Hive Metastore
      │
      │ metadados
      ▼
   Tabelas Hive
      │
      │ dados
      ▼
     HDFS
```

## Warehouse

O warehouse principal está configurado em:

```text
hdfs://namenode:9000/user/hive/warehouse
```

As tabelas externas podem utilizar:

```text
hdfs://namenode:9000/data/warehouse
```

Essa separação permite trabalhar com dados gerenciados pelo Hive e dados externos cujo ciclo de vida é controlado pela própria pipeline.

## Particionamento

A configuração habilita particionamento dinâmico:

```text
hive.exec.dynamic.partition=true
hive.exec.dynamic.partition.mode=nonstrict
```

O particionamento será utilizado principalmente para organizar dados temporais.

Exemplo:

```text
event_date=2026-09-07/
event_date=2026-09-08/
event_date=2026-09-09/
```

Para dados de maior granularidade, também podem ser utilizadas partições por hora:

```text
event_date=2026-09-07/hour=10/
event_date=2026-09-07/hour=11/
event_date=2026-09-07/hour=12/
```

Isso reduz a quantidade de dados que precisa ser lida durante consultas filtradas por período.

## Formato dos dados

As tabelas gerenciadas pelo Hive utilizam **Parquet** como formato padrão.

A compressão configurada é:

```text
SNAPPY
```

A combinação:

```text
Hive + Parquet + Snappy + HDFS
```

é adequada para workloads analíticos porque permite armazenamento colunar, compressão e leitura seletiva de colunas.

## Integração com Spark

O Spark utiliza o Hive Metastore para acessar o catálogo analítico.

O fluxo batch é:

```text
HDFS Raw
   │
   ▼
Spark ETL
   │
   ▼
HDFS Clean
   │
   ▼
Spark Transformations
   │
   ▼
HDFS Curated
   │
   ▼
Hive Tables
```

O Spark executa as transformações pesadas, joins, agregações e operações de ETL.

O Hive disponibiliza o resultado consolidado para consultas SQL.

## Modelo analítico

O Data Warehouse será estruturado posteriormente através dos scripts SQL localizados em:

```text
sql/hive/ddl/
```

A organização prevista utiliza:

### Tabelas fato

Representam eventos mensuráveis da operação:

```text
fact_sales
fact_clicks
fact_carts
fact_deliveries
```

### Tabelas dimensão

Representam entidades utilizadas para análise:

```text
dim_customer
dim_product
dim_category
dim_date
```

O relacionamento entre fatos e dimensões permitirá análises como:

* vendas por produto;
* vendas por categoria;
* comportamento de clientes;
* conversão de cliques em pedidos;
* abandono de carrinho;
* desempenho logístico;
* tempo médio de entrega;
* volume de vendas por período.

## Integração com HDFS

O Hive não substitui o HDFS.

O HDFS continua sendo a camada de armazenamento distribuído:

```text
HDFS
 │
 ├── Raw
 ├── Clean
 └── Curated
```

O Hive fornece a camada de metadados e consulta:

```text
Hive
 │
 ├── Database
 ├── Tables
 ├── Partitions
 └── Views
```

Essa separação mantém clara a responsabilidade de cada componente.

## Integração com a pipeline

O Hive representa o estágio final do processamento batch:

```text
                ┌───────────────┐
                │ Python        │
                │ Generator     │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Apache Flume  │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ HDFS Raw      │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Apache Spark   │
                │ ETL / RDD /SQL │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ HDFS Curated  │
                └───────┬───────┘
                        │
                        ▼
                ┌───────────────┐
                │ Apache Hive   │
                │ Data Warehouse│
                └───────────────┘
```

Paralelamente, o fluxo de streaming utiliza Apache Flink para processamento em tempo real e Apache HBase para disponibilização de alertas de baixa latência.

Assim, Hive permanece responsável pela **análise histórica e consolidada**, enquanto HBase atende consultas operacionais de baixa latência.

## Arquivos relacionados

A configuração do Hive é complementada por:

```text
configs/hive/
├── hive-site.xml
├── hive-env.sh
└── README.md
```

As estruturas das tabelas serão definidas posteriormente em:

```text
sql/hive/ddl/
```

e a integração programática será implementada em:

```text
src/storage/hive/
src/batch/hive/
```

## Papel na arquitetura

O Apache Hive fecha a camada analítica da pipeline:

```text
Geração
   ↓
Ingestão
   ↓
Armazenamento Raw
   ↓
Processamento Batch
   ↓
Curated
   ↓
Data Warehouse
   ↓
Análise SQL
```

Dessa forma, a arquitetura atende ao requisito de transformar os eventos gerados continuamente em dados históricos organizados e consultáveis para análise do comportamento de vendas e logística do e-commerce.
