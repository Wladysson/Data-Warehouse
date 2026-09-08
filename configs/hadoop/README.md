# Apache Hadoop

O Apache Hadoop fornece a infraestrutura de armazenamento distribuído utilizada pela pipeline de Big Data, tendo o **HDFS** como principal camada de persistência dos dados.

## Responsabilidade

Nesta arquitetura, o Hadoop é utilizado principalmente para:

* armazenamento distribuído dos eventos;
* persistência dos dados brutos;
* disponibilização do histórico para processamento batch;
* suporte à execução distribuída através do YARN.

O fluxo de armazenamento é:

```text
Eventos
   │
   ▼
Apache Flume
   │
   ▼
HDFS
   │
   ├── /data/raw
   │
   ├── /data/processed/clean
   │
   └── /data/processed/curated
          │
          ▼
       Apache Spark
```

## HDFS

O HDFS representa a camada de **Data Lake** da pipeline.

Os eventos brutos são armazenados sem aplicação de regras de negócio, preservando os dados originais para posterior processamento.

A configuração principal do sistema de arquivos está em:

```text
hdfs-site.xml
```

Enquanto a definição do sistema de arquivos padrão está em:

```text
core-site.xml
```

O filesystem utilizado pela pipeline é:

```text
hdfs://namenode:9000
```

## Organização dos dados

Os dados são organizados em diferentes camadas:

```text
/data/raw
/data/processed/clean
/data/processed/curated
```

### Raw

Armazena os eventos originais coletados durante a ingestão.

### Clean

Armazena os dados após validação, normalização e limpeza.

### Curated

Armazena os dados transformados e preparados para consumo analítico.

## Blocos HDFS

O HDFS divide os arquivos em blocos distribuídos entre os DataNodes.

A configuração utiliza blocos de:

```text
128 MB
```

Esse comportamento permite que grandes volumes de dados sejam distribuídos e processados de forma paralela.

## Replicação

O ambiente local utiliza fator de replicação:

```text
1
```

pois a infraestrutura de desenvolvimento utiliza um único DataNode.

Em uma infraestrutura com múltiplos DataNodes, o fator de replicação pode ser aumentado para proporcionar maior tolerância a falhas.

## YARN

O YARN é utilizado como camada de gerenciamento de recursos para execução distribuída de workloads Hadoop.

Os principais componentes são:

```text
ResourceManager
        │
        ▼
NodeManager
        │
        ▼
Containers / Tasks
```

A configuração do YARN está em:

```text
yarn-site.xml
```

## MapReduce

O projeto mantém a configuração do MapReduce através de:

```text
mapred-site.xml
```

O framework está configurado para utilizar o YARN:

```text
mapreduce.framework.name = yarn
```

Embora o processamento batch principal da plataforma seja realizado pelo Apache Spark, essa configuração mantém o ambiente Hadoop preparado para execução de workloads MapReduce distribuídos.

## Integração com Spark

O Spark utiliza o HDFS como origem dos dados históricos.

O fluxo principal é:

```text
HDFS
  │
  ▼
Spark
  │
  ├── RDDs
  ├── Spark SQL
  ├── Transformações
  ├── Joins
  └── Agregações
  │
  ▼
Hive
```

Dessa forma, o Hadoop fornece a camada de armazenamento distribuído enquanto o Spark executa o processamento batch.

## Integração com Flume

O Apache Flume utiliza o HDFS como destino para os eventos ingeridos:

```text
Python Generator
       │
       ▼
     Flume
       │
       ▼
     HDFS
```

O Flume é responsável pela ingestão e o HDFS pela persistência dos dados.

## Integração com a pipeline

O Hadoop não é utilizado como camada de processamento de negócio da aplicação.

Sua responsabilidade principal é fornecer a infraestrutura distribuída necessária para:

1. receber os dados ingeridos;
2. armazenar os eventos em grande escala;
3. disponibilizar o histórico para o Spark;
4. fornecer recursos computacionais através do YARN quando necessário.

Isso mantém a separação entre **ingestão, armazenamento, processamento streaming e processamento batch**.
