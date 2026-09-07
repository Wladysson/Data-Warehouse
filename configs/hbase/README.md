# Apache HBase

O Apache HBase é utilizado como camada de armazenamento **orientada a chave e de baixa latência** para os resultados produzidos pelo processamento de streaming da pipeline.

## Responsabilidade

O HBase recebe principalmente os alertas identificados pelo Apache Flink.

```text
Eventos
   │
   ▼
Flume
   │
   ▼
Flink
   │
   ├── processamento em tempo real
   ├── Event Time
   ├── Watermarks
   ├── Sliding Windows
   └── Detecção de alertas
            │
            ▼
          HBase
```

## Por que HBase?

O HDFS é utilizado como armazenamento distribuído para grandes volumes de dados e processamento histórico.

O HBase possui uma finalidade diferente: fornecer acesso rápido a registros individuais ou pequenos conjuntos de registros através de uma chave.

Na arquitetura:

```text
HDFS
→ armazenamento massivo
→ dados brutos
→ histórico
→ processamento batch

HBase
→ acesso orientado a chave
→ alertas
→ resultados de streaming
→ baixa latência
```

Essa separação evita utilizar o HDFS para operações de leitura frequentes e orientadas a registros.

## Integração com HDFS

O HBase utiliza o HDFS como camada persistente:

```text
HBase
  │
  ▼
HDFS
  │
  └── /hbase
```

O diretório raiz configurado é:

```text
hdfs://namenode:9000/hbase
```

## ZooKeeper

O ZooKeeper é utilizado pelo HBase para coordenação do cluster.

A configuração utiliza:

```text
zookeeper:2181
```

O HBase não gerencia a instância do ZooKeeper, pois o serviço é tratado separadamente pela infraestrutura.

## Tabelas

As tabelas utilizadas pela pipeline serão definidas posteriormente em:

```text
sql/hbase/
```

A criação e o gerenciamento das tabelas não fazem parte do `hbase-site.xml`.

Essa separação mantém:

```text
Configuração do HBase
        │
        ▼
configs/hbase/

Estrutura das tabelas
        │
        ▼
sql/hbase/
```

## Row Keys

O acesso aos registros do HBase será baseado em Row Keys.

A definição da estratégia de Row Key será implementada posteriormente em:

```text
src/storage/hbase/row_keys.py
```

Essa camada será responsável por definir uma chave adequada para consultas eficientes.

## Integração com Flink

O Flink será responsável pela identificação dos eventos que devem gerar alertas.

O resultado será encaminhado ao HBase:

```text
Flink
  │
  ▼
Alert Detector
  │
  ▼
HBase Sink
  │
  ▼
HBase
```

A implementação do sink ficará posteriormente em:

```text
src/streaming/sinks/hbase_sink.py
```

## Integração com a pipeline

O HBase representa a camada de armazenamento para informações que precisam estar disponíveis rapidamente após o processamento de streaming.

O fluxo completo de armazenamento é:

```text
                  ┌───────────────┐
                  │    Flume      │
                  └───────┬───────┘
                          │
                          ▼
                  ┌───────────────┐
                  │     HDFS      │
                  │     Raw       │
                  └───────┬───────┘
                          │
                          ▼
                       Spark
                          │
                          ▼
                       Hive


                  Flink Streaming
                          │
                          ▼
                  ┌───────────────┐
                  │     HBase     │
                  │    Alertas    │
                  └───────────────┘
```

Dessa forma, HDFS, HBase e Hive possuem responsabilidades distintas dentro da arquitetura, evitando utilizar uma única tecnologia para necessidades de armazenamento diferentes.
