# Apache Spark

O Apache Spark é responsável pelo **processamento batch dos dados históricos** armazenados no HDFS.

Nesta pipeline, o Spark executa as etapas de ETL, transformações, agregações e joins necessárias para transformar os eventos brutos em dados estruturados para análise.

## Responsabilidade

O processamento batch segue o fluxo:

```text
HDFS - Dados Raw
        │
        ▼
┌─────────────────┐
│      Spark      │
│                 │
│   RDDs          │
│   Spark SQL     │
│   ETL           │
│   Transformações│
│   Joins         │
│   Agregações    │
└────────┬────────┘
         │
         ▼
HDFS - Clean / Curated
         │
         ▼
┌─────────────────┐
│      Hive       │
│ Data Warehouse  │
└─────────────────┘
```

## RDDs

O projeto utiliza RDDs para demonstrar o processamento distribuído de coleções de eventos.

As implementações relacionadas aos RDDs estarão em:

```text
src/batch/rdds/
```

As transformações serão realizadas de forma distribuída sobre os dados históricos.

## Spark SQL

O Spark SQL será utilizado para trabalhar com dados estruturados e realizar operações como:

* seleção;
* filtragem;
* agregação;
* agrupamento;
* ordenação;
* joins;
* criação de DataFrames.

As operações SQL serão utilizadas principalmente nas etapas de transformação e consolidação dos dados.

## Wide Dependencies

As operações de processamento batch incluem transformações que exigem redistribuição dos dados entre partições.

Exemplos utilizados na pipeline:

* `groupBy`;
* `reduceByKey`;
* `join`;
* agregações;
* operações de agrupamento.

Essas operações podem gerar **shuffle**, demonstrando o conceito de wide dependency no processamento distribuído.

## ETL

O Spark executará o processo de transformação:

```text
Raw
 │
 ▼
Clean
 │
 ▼
Curated
```

### Raw

Contém os eventos originais ingeridos pela pipeline.

### Clean

Contém dados tratados, normalizados e validados.

### Curated

Contém dados preparados para análise e carregamento no Data Warehouse.

A implementação do ETL ficará em:

```text
src/batch/etl/
```

## Joins

O processamento batch realiza joins entre diferentes conjuntos de dados para produzir informações consolidadas.

Os joins serão implementados em:

```text
src/batch/joins/
```

Entre os relacionamentos previstos estão:

* pedidos e clientes;
* produtos e categorias;
* vendas e entregas.

Essas operações permitem demonstrar o processamento distribuído de dados relacionados.

## Hive

O Spark será integrado ao Hive através do Hive Metastore.

A configuração permite que o Spark trabalhe com tabelas Hive e utilize o warehouse localizado no HDFS:

```text
/user/hive/warehouse
```

Os dados consolidados serão organizados posteriormente como um Data Warehouse para consultas analíticas.

## Particionamento

A configuração habilita o particionamento dinâmico do Hive.

Isso permite que os dados consolidados sejam organizados por dimensões temporais, como:

```text
ano
mês
dia
```

O particionamento reduz a quantidade de dados lidos durante consultas analíticas que possuem filtros temporais.

## Formato dos dados

O processamento utiliza Parquet como formato analítico, permitindo armazenamento colunar e compressão dos dados.

A compressão configurada é:

```text
SNAPPY
```

## Execução

A configuração principal do Spark está em:

```text
spark-defaults.conf
```

O job batch será implementado posteriormente em:

```text
src/batch/spark_job.py
```

A sessão Spark será centralizada em:

```text
src/batch/spark_session.py
```

Dessa forma, a configuração de infraestrutura permanece separada da lógica de processamento.

## Integração com a pipeline

O Spark representa a camada de **processamento histórico e consolidação analítica**.

Sua responsabilidade é diferente do Flink:

```text
Flink
→ processamento em tempo real
→ Event Time
→ Watermarks
→ Sliding Windows
→ Alertas

Spark
→ processamento batch
→ histórico
→ ETL
→ RDDs
→ Spark SQL
→ Joins
→ Agregações
```

O resultado final do processamento batch será disponibilizado no Hive para consultas analíticas.
