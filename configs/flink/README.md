# Apache Flink

O Apache Flink é responsável pelo **processamento de streaming dos eventos do e-commerce**, permitindo analisar os dados continuamente conforme eles chegam à pipeline.

## Responsabilidade

O Flink recebe os eventos ingeridos pela camada de dados e executa processamento em tempo real utilizando:

* Event Time;
* Watermarks;
* Sliding Windows;
* agregações;
* identificação de eventos fora de ordem;
* detecção de alertas.

O fluxo conceitual é:

```text
Eventos
   │
   ▼
Source
   │
   ▼
Timestamp Assignment
   │
   ▼
Watermarks
   │
   ▼
Operators
   │
   ▼
Key By
   │
   ▼
Sliding Windows
   │
   ▼
Aggregations
   │
   ▼
Alert Detection
   │
   ├──────────────► HBase
   │
   └──────────────► HDFS
```

## Event Time

A pipeline utiliza o timestamp associado ao próprio evento como referência temporal para o processamento.

Isso permite que o Flink processe corretamente eventos que não chegam exatamente na ordem em que foram gerados.

## Watermarks

Watermarks são utilizadas para indicar o progresso do tempo dos eventos dentro do fluxo.

A configuração da estratégia de watermark será implementada em:

```text
src/streaming/watermarks/
```

Ela permitirá considerar uma tolerância para eventos que chegam atrasados ou fora de ordem.

## Sliding Windows

A análise temporal utiliza janelas deslizantes para produzir agregações contínuas sobre os eventos.

A implementação das janelas ficará em:

```text
src/streaming/windows/
```

As janelas serão utilizadas para identificar comportamentos como aumento de cliques, carrinhos e eventos relacionados às operações de venda e logística.

## Estado e Checkpoints

O Flink está configurado para utilizar estado durante o processamento e checkpoints periódicos para permitir recuperação em caso de falhas.

A configuração utiliza:

```text
EXACTLY_ONCE
```

para os checkpoints, reduzindo o risco de inconsistências durante a recuperação do processamento.

## Paralelismo

O ambiente possui paralelismo configurável para permitir que o processamento seja distribuído entre as tarefas do Flink.

O valor padrão definido na configuração é:

```text
parallelism.default: 2
```

Esse valor pode ser ajustado conforme o ambiente de execução.

## Integração com o armazenamento

Os resultados do processamento de streaming serão direcionados para diferentes objetivos:

### HBase

Utilizado para disponibilizar alertas e resultados que necessitam de acesso rápido.

### HDFS

Utilizado para persistência dos dados e resultados em armazenamento distribuído.

A separação dessas responsabilidades permite que o HDFS funcione como camada de armazenamento massivo, enquanto o HBase atende às necessidades de acesso de baixa latência.

## Configuração

O arquivo:

```text
flink-conf.yaml
```

define parâmetros de infraestrutura, memória, paralelismo, checkpoints, recuperação e comunicação do cluster.

A lógica de processamento de eventos permanece isolada no código em:

```text
src/streaming/
```

mantendo separadas as responsabilidades de configuração e processamento.
