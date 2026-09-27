# Batch

## Visão Geral

O módulo `batch` implementa a camada de **processamento histórico da pipeline de Big Data**, utilizando o **Apache Spark** para executar operações de ETL, transformação, integração e análise sobre os dados persistidos.

Diferentemente da camada `streaming`, que trabalha continuamente sobre eventos em tempo real, o processamento batch opera sobre conjuntos de dados que já foram armazenados nas camadas de persistência.

A implementação utiliza recursos do ecossistema Spark, incluindo:

- RDDs;
- DataFrames;
- Spark SQL;
- operações de transformação;
- operações de agregação;
- joins;
- leitura de dados históricos;
- processamento distribuído;
- geração de resultados analíticos.

O fluxo geral é:

```text
                 ┌──────────────────────┐
                 │       Storage        │
                 │                      │
                 │ HDFS / Hive / HBase  │
                 └──────────┬───────────┘
                            │
                            ▼
                 ┌──────────────────────┐
                 │    Apache Spark      │
                 │                      │
                 │      Read            │
                 │       ↓              │
                 │      RDD             │
                 │       ↓              │
                 │   DataFrame          │
                 │       ↓              │
                 │    Spark SQL         │
                 │       ↓              │
                 │      Joins           │
                 │       ↓              │
                 │    Aggregation       │
                 │       ↓              │
                 │      Output          │
                 └──────────┬───────────┘
                            │
                            ▼
                       Resultados
```

---

# Objetivo

O objetivo da camada batch é processar dados acumulados para produzir informações que dependem de uma visão histórica do conjunto de eventos.

O processamento permite realizar operações como:

- limpeza dos dados;
- transformação de registros;
- seleção de atributos;
- filtragem;
- agregação;
- combinação de diferentes conjuntos;
- consultas SQL;
- análise histórica;
- geração de resultados derivados.

Enquanto o streaming responde ao fluxo contínuo:

```text
Evento → Processamento → Resultado
```

o batch trabalha sobre dados acumulados:

```text
Dados armazenados
       │
       ▼
     Spark
       │
       ▼
Processamento
       │
       ▼
Resultados
```

---

# Papel na Arquitetura

O `batch` ocupa a camada de processamento histórico da pipeline.

```text
┌─────────────────────┐
│   Data Generator    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      Ingestion      │
│     Apache Flume    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│       Storage       │
│                     │
│ HDFS / HBase / Hive │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│        BATCH        │
│      Apache Spark   │
└──────────┬──────────┘
           │
           ▼
      Resultados
```

O processamento batch utiliza os dados já persistidos para realizar análises que não precisam acontecer imediatamente durante a chegada dos eventos.

---

# Apache Spark

O Apache Spark é utilizado como mecanismo de processamento distribuído da camada batch.

A arquitetura do Spark permite dividir operações sobre grandes conjuntos de dados e executar as tarefas de processamento de forma distribuída.

Dentro do projeto, o Spark é utilizado para demonstrar diferentes abstrações de processamento:

```text
Apache Spark
     │
     ├── RDD
     │
     ├── DataFrame
     │
     ├── Spark SQL
     │
     ├── Transformations
     │
     ├── Actions
     │
     └── Joins
```

---

# Fluxo de Processamento

O processamento batch pode ser representado pelas seguintes etapas:

```text
Dados históricos
      │
      ▼
Leitura
      │
      ▼
Ingestão no Spark
      │
      ▼
RDD / DataFrame
      │
      ▼
Limpeza
      │
      ▼
Transformação
      │
      ▼
Integração / Join
      │
      ▼
Agregação
      │
      ▼
Spark SQL
      │
      ▼
Resultado
```

Cada etapa representa uma transformação sobre os dados históricos.

---

# Leitura dos Dados

O primeiro estágio do processamento é a leitura dos dados persistidos.

Os dados podem estar organizados nas camadas utilizadas pela arquitetura:

```text
HDFS
 │
 ├── Dados brutos
 ├── Dados processados
 └── Arquivos históricos

Hive
 │
 └── Tabelas analíticas

HBase
 │
 └── Resultados / eventos processados
```

O Spark atua como mecanismo de processamento sobre os dados disponibilizados pelo ambiente.

---

# ETL

A camada batch implementa o conceito de **ETL — Extract, Transform, Load**.

```text
Extract
   │
   ▼
Transform
   │
   ▼
Load
```

### Extract

Os dados históricos são extraídos das camadas de armazenamento.

```text
HDFS / Hive / Storage
          │
          ▼
        Spark
```

### Transform

Os registros são processados e transformados conforme as necessidades analíticas.

Exemplos:

- filtragem;
- normalização;
- seleção;
- conversão de tipos;
- enriquecimento;
- agregação;
- combinação de conjuntos.

### Load

Os resultados processados podem ser direcionados para a camada de saída definida pela aplicação.

---

# RDD

O **RDD — Resilient Distributed Dataset** é uma das abstrações fundamentais do Apache Spark.

Um RDD representa uma coleção distribuída de dados que pode ser processada de forma paralela.

Conceitualmente:

```text
Dataset
   │
   ▼
RDD
   │
   ├── Partition 1
   ├── Partition 2
   ├── Partition 3
   └── Partition N
```

Essa abstração permite demonstrar operações funcionais sobre conjuntos distribuídos.

---

# Transformações com RDD

As transformações em RDD são operações que produzem novos RDDs a partir de dados existentes.

Exemplos comuns:

```text
map
filter
flatMap
reduceByKey
groupByKey
```

O conceito pode ser representado como:

```text
RDD original
     │
     ▼
 Transformation
     │
     ▼
Novo RDD
```

As transformações são avaliadas de forma lazy pelo Spark.

Isso significa que a operação não precisa executar imediatamente quando é definida.

---

# Lazy Evaluation

O Spark utiliza **lazy evaluation** para construir um plano de execução antes de executar determinadas operações.

Por exemplo:

```text
RDD
 │
 ├── filter
 │
 ├── map
 │
 └── groupBy
```

Essas transformações podem ser organizadas no plano de execução.

A execução efetiva ocorre quando uma **action** solicita o resultado.

Exemplos de actions incluem:

```text
count()
collect()
save()
```

Esse modelo permite ao Spark otimizar a execução das operações.

---

# DataFrames

Além dos RDDs, o processamento pode utilizar **DataFrames** para trabalhar com dados estruturados.

Um DataFrame organiza os dados em linhas e colunas, permitindo que o Spark aplique otimizações específicas sobre consultas estruturadas.

Conceitualmente:

```text
DataFrame
│
├── user_id
├── product_id
├── event_type
├── timestamp
└── ...
```

Essa estrutura facilita operações analíticas e integração com Spark SQL.

---

# Spark SQL

O **Spark SQL** permite executar consultas SQL sobre dados estruturados carregados no Spark.

O fluxo é:

```text
Dados
  │
  ▼
DataFrame
  │
  ▼
Temp View / Table
  │
  ▼
Spark SQL
  │
  ▼
Resultado
```

Isso permite combinar operações programáticas do Spark com consultas declarativas em SQL.

---

# Consultas Analíticas

Consultas SQL podem ser utilizadas para realizar operações como:

```sql
SELECT
    event_type,
    COUNT(*) AS total
FROM events
GROUP BY event_type;
```

O exemplo representa conceitualmente uma agregação por tipo de evento.

As consultas efetivamente executadas devem seguir as estruturas e nomes definidos na implementação do projeto.

---

# Joins

Uma das operações importantes da camada batch é a combinação de diferentes conjuntos de dados utilizando **joins**.

Um cenário de e-commerce pode possuir diferentes entidades:

```text
Eventos
   │
   ├── Usuários
   ├── Produtos
   ├── Pedidos
   └── Entregas
```

O processamento batch pode combinar essas informações para produzir conjuntos mais completos.

Conceitualmente:

```text
Eventos ──────┐
              │
              ├──► JOIN ──► Dataset integrado
              │
Produtos ─────┘
```

---

# Tipos de Join

Dependendo da necessidade analítica, podem ser utilizados diferentes tipos de join.

### Inner Join

Retorna registros que possuem correspondência entre os conjuntos.

```text
A ∩ B
```

### Left Join

Preserva os registros do conjunto esquerdo, mesmo quando não existe correspondência no conjunto direito.

```text
A LEFT JOIN B
```

### Right Join

Preserva os registros do conjunto direito.

```text
A RIGHT JOIN B
```

### Full Join

Combina registros dos dois conjuntos, preservando também elementos sem correspondência.

```text
A FULL JOIN B
```

O tipo de join deve ser escolhido de acordo com a semântica dos dados e o resultado esperado pela análise.

---

# Exemplo de Integração

Um fluxo analítico pode combinar eventos e informações de pedidos:

```text
┌─────────────────┐
│     Events      │
│                 │
│ user_id         │
│ product_id      │
│ event_type      │
└────────┬────────┘
         │
         │ JOIN
         │
┌────────▼────────┐
│     Orders      │
│                 │
│ order_id        │
│ user_id         │
│ product_id      │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Integrated Data │
└─────────────────┘
```

Essa integração permite realizar análises sobre diferentes dimensões do negócio.

---

# Agregações

Após transformações e joins, os dados podem ser agregados.

Exemplos de operações:

```text
COUNT
SUM
AVG
MIN
MAX
GROUP BY
```

Um fluxo típico seria:

```text
Dados
  │
  ▼
Filter
  │
  ▼
Join
  │
  ▼
Group
  │
  ▼
Aggregate
  │
  ▼
Resultado
```

As agregações permitem transformar grandes volumes de registros em informações resumidas.

---

# Processamento Histórico

O processamento batch é especialmente adequado para análises que consideram períodos já encerrados ou grandes conjuntos de dados acumulados.

Exemplos:

```text
Dados do dia
Dados da semana
Dados do mês
Histórico de pedidos
Histórico de entregas
Histórico de interações
```

O Spark pode processar esses conjuntos independentemente do momento em que os eventos originalmente ocorreram.

---

# Batch x Streaming

As duas camadas possuem responsabilidades diferentes dentro da arquitetura.

| Batch | Streaming |
|---|---|
| Apache Spark | Apache Flink |
| Dados históricos | Eventos contínuos |
| Processamento em lotes | Processamento contínuo |
| RDDs | Streams |
| Spark SQL | Event Time |
| Joins históricos | Watermarks |
| ETL | Sliding Windows |
| Análise histórica | Alertas em tempo real |

O uso dos dois paradigmas permite demonstrar diferentes estratégias de processamento de dados na mesma arquitetura.

---

# Relação com HDFS

O HDFS funciona como uma camada de armazenamento distribuído para dados que podem posteriormente ser processados pelo Spark.

O fluxo pode ser:

```text
Eventos
   │
   ▼
Storage
   │
   ▼
HDFS
   │
   ▼
Spark
   │
   ▼
ETL
```

A separação entre armazenamento e processamento permite que os dados sejam reutilizados por diferentes jobs.

---

# Relação com Hive

O Hive fornece uma camada orientada a consultas sobre dados estruturados.

O Spark pode utilizar dados organizados em tabelas para realizar processamento analítico.

```text
Hive
  │
  ▼
Spark
  │
  ├── DataFrame
  ├── SQL
  ├── Join
  └── Aggregation
```

Essa integração aproxima o processamento distribuído das consultas analíticas tradicionais em SQL.

---

# Relação com HBase

O HBase possui uma finalidade diferente do HDFS e Hive, sendo utilizado como armazenamento orientado a colunas e acesso distribuído a registros.

Na arquitetura:

```text
Streaming
    │
    ▼
   HBase
    │
    ▼
  Batch
    │
    ▼
  Spark
```

Quando aplicável, os dados armazenados no HBase podem participar de processos posteriores de análise ou validação.

---

# Execução

O processamento batch é executado através do ambiente Spark disponibilizado pelo projeto.

Para iniciar o ambiente:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Verificar os serviços:

```bash
docker compose -f docker/docker-compose.yml ps
```

Verificar o container Spark:

```bash
docker ps | grep spark
```

---

# Spark Submit

Jobs Spark podem ser submetidos utilizando `spark-submit`.

Um exemplo de execução no ambiente é:

```bash
docker exec -it ecommerce-spark spark-submit \
  --master local[*] \
  --deploy-mode client \
  /app/src/batch/spark_job.py
```

O caminho do script deve corresponder à estrutura efetivamente montada no container.

O modo:

```text
--master local[*]
```

permite utilizar os recursos disponíveis no ambiente local do container para execução do job.

---

# Aplicação Spark

O job pode ser identificado através do nome configurado na aplicação Spark.

Durante a execução, o Spark disponibiliza informações como:

```text
Application
   │
   ├── Jobs
   ├── Stages
   ├── Tasks
   ├── Storage
   ├── Executors
   └── Environment
```

Essas informações são úteis para acompanhar o processamento.

---

# Spark UI

O Spark disponibiliza uma interface Web para observação da execução da aplicação.

No ambiente utilizado pela pipeline, a interface pode ser exposta através da porta configurada para o serviço Spark.

A interface permite observar:

- aplicações;
- jobs;
- stages;
- tasks;
- duração das operações;
- armazenamento;
- executors;
- ambiente;
- métricas de execução.

Durante uma execução ativa, a interface é uma das principais ferramentas para demonstrar o processamento batch.

---

# Jobs, Stages e Tasks

O Spark divide o processamento em diferentes níveis de execução.

```text
Application
     │
     ▼
   Jobs
     │
     ▼
   Stages
     │
     ▼
   Tasks
```

### Job

Uma ação executada pelo programa Spark pode gerar um job.

### Stage

O job pode ser dividido em estágios de execução.

### Task

As tasks representam unidades menores de trabalho executadas sobre partições dos dados.

Essa estrutura permite ao Spark distribuir o processamento.

---

# Particionamento

Os dados processados pelo Spark são divididos em partições.

```text
Dataset
   │
   ├── Partition 1
   ├── Partition 2
   ├── Partition 3
   └── Partition N
```

As partições podem ser processadas paralelamente.

O particionamento influencia diretamente a execução distribuída e pode afetar o desempenho das operações.

---

# Shuffle

Algumas operações exigem redistribuição dos dados entre as partições.

Esse processo é conhecido como **shuffle**.

Exemplos de operações que podem provocar shuffle:

```text
groupBy
reduceByKey
join
orderBy
```

Conceitualmente:

```text
Partition A ──┐
Partition B ──┼──► Shuffle ──► Novas partições
Partition C ──┘
```

O shuffle pode representar uma etapa de maior custo computacional e de comunicação dentro do processamento Spark.

---

# Otimização

A camada batch deve priorizar operações que permitam ao Spark executar o processamento de maneira eficiente.

Alguns princípios relevantes são:

- evitar `collect()` desnecessário;
- filtrar dados o mais cedo possível;
- selecionar somente as colunas necessárias;
- evitar materialização desnecessária;
- utilizar DataFrames quando o processamento for estruturado;
- avaliar o custo de joins;
- observar operações que provocam shuffle.

A otimização deve ser orientada pelo comportamento real do job e pelas métricas disponibilizadas pelo Spark.

---

# Logs

Os logs do Spark permitem acompanhar a execução do job.

Para verificar os logs do container:

```bash
docker logs --tail 100 ecommerce-spark
```

Para acompanhar em tempo real:

```bash
docker logs -f ecommerce-spark
```

Os logs permitem identificar:

- inicialização do Spark;
- criação da aplicação;
- leitura dos dados;
- execução dos stages;
- erros de conexão;
- falhas durante joins;
- problemas de configuração;
- finalização da aplicação.

---

# Monitoramento

A observabilidade da camada batch pode ser realizada através de três fontes principais:

```text
┌─────────────────┐
│    Docker       │
│     Logs        │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Spark UI     │
│                 │
│ Jobs            │
│ Stages          │
│ Tasks           │
│ Executors       │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│ Storage / HDFS   │
│ Resultados       │
└─────────────────┘
```

Essa combinação permite acompanhar tanto a infraestrutura quanto o processamento.

---

# Tratamento de Erros

Durante a execução do batch podem ocorrer falhas relacionadas a:

- arquivos inexistentes;
- formato de dados inválido;
- schemas incompatíveis;
- falhas de conexão;
- tabelas indisponíveis;
- problemas no Hive;
- problemas no HDFS;
- joins com estruturas incompatíveis;
- erros de transformação;
- configuração do Spark.

O diagnóstico deve partir do erro apresentado pelo Spark e identificar em qual etapa do pipeline de processamento ocorreu a falha.

---

# Diagnóstico

Uma sequência básica de diagnóstico é:

### 1. Verificar os containers

```bash
docker compose -f docker/docker-compose.yml ps
```

### 2. Verificar Spark

```bash
docker logs --tail 100 ecommerce-spark
```

### 3. Executar o job

```bash
docker exec -it ecommerce-spark spark-submit \
  --master local[*] \
  --deploy-mode client \
  /app/src/batch/spark_job.py
```

### 4. Verificar Spark UI

Acompanhar:

```text
Jobs
Stages
Tasks
Executors
```

### 5. Verificar origem dos dados

Confirmar se os dados necessários estão disponíveis no HDFS, Hive ou demais fontes utilizadas pelo job.

---

# Reprodutibilidade

O processamento batch é executado dentro do ambiente Docker, permitindo reproduzir as mesmas condições de execução da pipeline.

A infraestrutura centraliza:

- versão do Spark;
- ambiente Java;
- configurações;
- dependências;
- caminhos dos dados;
- comunicação com HDFS/Hive;
- recursos de execução.

Isso reduz diferenças entre execuções realizadas em ambientes diferentes.

---

# Dependências

A camada batch depende principalmente das fontes de dados persistidas e do ambiente Spark.

```text
HDFS / Hive / Storage
          │
          ▼
      Spark Job
          │
          ▼
     Transformações
          │
          ▼
       Resultado
```

Para execução correta, devem estar disponíveis:

- container Spark;
- dados de entrada;
- HDFS quando utilizado como fonte;
- Hive quando utilizado pelo job;
- configurações de conexão;
- dependências da aplicação;
- recursos computacionais necessários.

---

# Relação com as Outras Camadas

A arquitetura completa pode ser representada como:

```text
┌─────────────────────┐
│   DATA GENERATOR    │
│                     │
│ Eventos JSON        │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      INGESTION      │
│     Apache Flume    │
└──────────┬──────────┘
           │
           ├───────────────────────┐
           │                       │
           ▼                       ▼
┌─────────────────────┐   ┌─────────────────────┐
│      STREAMING      │   │       STORAGE       │
│       Flink         │   │ HDFS / HBase / Hive │
└──────────┬──────────┘   └──────────┬──────────┘
           │                         │
           │                         ▼
           │                ┌─────────────────────┐
           │                │        BATCH        │
           │                │       Spark         │
           │                │                     │
           │                │ RDD / DataFrame     │
           │                │ Spark SQL            │
           │                │ ETL                  │
           │                │ Joins                │
           │                └──────────┬──────────┘
           │                           │
           └──────────────┬────────────┘
                          ▼
                    Resultados
```

O batch utiliza os dados acumulados pela arquitetura para realizar processamento histórico e análises que não dependem exclusivamente da chegada imediata dos eventos.

---

# Streaming e Batch no Mesmo Projeto

A utilização conjunta de Flink e Spark permite demonstrar dois paradigmas de processamento.

### Streaming

```text
Evento
  │
  ▼
Flink
  │
  ▼
Resultado em tempo real
```

### Batch

```text
Dados acumulados
      │
      ▼
Spark
      │
      ▼
Resultado histórico
```

A combinação permite que a arquitetura processe tanto eventos continuamente quanto grandes conjuntos de dados históricos.

---

# Resumo

O módulo `batch` implementa o processamento histórico da pipeline utilizando Apache Spark.

A camada realiza operações de ETL sobre dados persistidos, utilizando diferentes abstrações do Spark, como **RDDs, DataFrames e Spark SQL**.

O processamento também contempla operações de integração por meio de **joins**, transformações, filtros e agregações, permitindo combinar diferentes conjuntos de dados e produzir informações analíticas.

```text
              Dados Persistidos
                     │
                     ▼
              ┌──────────────┐
              │    Spark     │
              └──────┬───────┘
                     │
          ┌──────────┼──────────┐
          ▼          ▼          ▼
        RDD      DataFrame   Spark SQL
          │          │          │
          └──────────┼──────────┘
                     ▼
                 Transform
                     │
                     ▼
                   Join
                     │
                     ▼
                 Aggregate
                     │
                     ▼
                  Output
```

Essa camada complementa o processamento em tempo real realizado pelo Flink, permitindo que a arquitetura utilize tanto **stream processing** quanto **batch processing** sobre os dados do e-commerce.