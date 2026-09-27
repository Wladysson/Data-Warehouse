# Storage

## Visão Geral

O módulo `storage` representa a camada de **persistência e armazenamento** da arquitetura de Big Data do projeto `Data-Warehouse`.

Sua responsabilidade é disponibilizar os mecanismos necessários para armazenar, consultar e disponibilizar os dados processados pelos demais componentes da pipeline, utilizando três tecnologias com características distintas:

- **HDFS (Hadoop Distributed File System)** — armazenamento distribuído de arquivos em grande volume;
- **HBase** — banco NoSQL distribuído orientado a colunas, utilizado para dados que precisam de acesso rápido e atualização/consulta por chave;
- **Hive** — camada SQL e analítica sobre dados armazenados no ecossistema Hadoop.

A utilização dessas três tecnologias não representa redundância. Cada uma atende a um requisito diferente da arquitetura.

```text
                    ┌──────────────────────────┐
                    │       Processamento      │
                    │                          │
                    │   Apache Flink / Spark   │
                    └────────────┬─────────────┘
                                 │
                ┌────────────────┼────────────────┐
                │                │                │
                ▼                ▼                ▼
        ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
        │     HDFS     │ │    HBase     │ │     Hive     │
        │              │ │              │ │              │
        │ Arquivos     │ │ NoSQL        │ │ SQL/Analítico│
        │ distribuídos │ │ baixa        │ │              │
        │              │ │ latência     │ │ consultas    │
        └──────┬───────┘ └──────┬───────┘ └──────┬───────┘
               │                │                │
               └────────────────┼────────────────┘
                                ▼
                       Camada de dados
```

---

## Objetivo

O objetivo do módulo é fornecer uma camada de armazenamento capaz de atender simultaneamente aos requisitos de:

- persistência de grandes volumes de dados;
- armazenamento distribuído;
- processamento histórico;
- consultas analíticas;
- acesso rápido a dados selecionados;
- integração com processamento batch;
- integração com processamento em tempo real;
- recuperação e reutilização dos dados;
- separação entre armazenamento histórico e acesso operacional.

A arquitetura utiliza o princípio de que **o mecanismo de armazenamento deve ser escolhido de acordo com o padrão de acesso ao dado**, e não apenas pelo volume armazenado.

---

# Arquitetura de Armazenamento

A camada é organizada conceitualmente em três componentes principais:

```text
                        STORAGE
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
      ┌────────┐       ┌────────┐       ┌────────┐
      │  HDFS  │       │ HBase  │       │  Hive  │
      └────────┘       └────────┘       └────────┘
          │                │                │
          │                │                │
          ▼                ▼                ▼
       Arquivos          Registros          SQL
       distribuídos      orientados        analítico
                          a chave
```

Cada tecnologia possui uma função específica dentro da pipeline.

| Tecnologia | Função principal | Padrão de acesso |
|---|---|---|
| HDFS | Armazenamento distribuído | Arquivos e grandes volumes |
| HBase | Persistência NoSQL | Acesso por chave/registro |
| Hive | Consulta analítica | SQL sobre dados distribuídos |

---

# HDFS

## Responsabilidade

O **Hadoop Distributed File System (HDFS)** é utilizado como camada de armazenamento distribuído para dados que precisam permanecer persistidos no ecossistema Hadoop.

Seu principal objetivo é armazenar grandes quantidades de dados distribuindo os arquivos entre os nós de armazenamento.

No ambiente do projeto, o HDFS está representado principalmente pela arquitetura:

```text
                HDFS
                 │
        ┌────────┴────────┐
        │                 │
   NameNode           DataNode
        │                 │
        │          Blocos dos arquivos
        │                 │
        └─────────────────┘
```

### NameNode

O NameNode é responsável pelos metadados do sistema de arquivos.

Entre suas responsabilidades estão:

- estrutura de diretórios;
- localização dos blocos;
- informações sobre arquivos;
- metadados dos DataNodes;
- gerenciamento do namespace do HDFS.

### DataNode

Os DataNodes são responsáveis pelo armazenamento físico dos blocos dos arquivos.

Em uma arquitetura distribuída, um arquivo pode ser dividido em blocos e distribuído entre os nós disponíveis.

```text
Arquivo
   │
   ├── Bloco A ──► DataNode
   ├── Bloco B ──► DataNode
   ├── Bloco C ──► DataNode
   └── Bloco D ──► DataNode
```

---

# Por que utilizar HDFS?

O HDFS é adequado para este projeto porque o pipeline produz dados continuamente e também precisa manter dados para processamento histórico.

Entre suas características relevantes estão:

- armazenamento distribuído;
- capacidade de trabalhar com grandes volumes;
- integração nativa com Spark;
- integração com Hive;
- processamento orientado a arquivos;
- tolerância a falhas através do modelo distribuído;
- separação entre armazenamento e processamento.

O HDFS funciona, portanto, como uma base persistente para os dados utilizados posteriormente pelo processamento batch e pelas consultas analíticas.

---

# HDFS e processamento Batch

A integração entre HDFS e Spark é uma das principais funções da arquitetura.

O fluxo pode ser representado como:

```text
              Dados persistidos
                     │
                     ▼
                   HDFS
                     │
                     │ leitura
                     ▼
                  Apache
                   Spark
                     │
                     ▼
              ETL / RDD / SQL
                     │
                     ▼
              Dados processados
```

O Spark pode utilizar os dados armazenados no HDFS como entrada para operações de:

- leitura;
- transformação;
- filtragem;
- agregação;
- join;
- limpeza;
- enriquecimento;
- geração de resultados históricos.

Isso permite separar a etapa de **persistência** da etapa de **processamento**.

---

# HBase

## Responsabilidade

O **Apache HBase** é utilizado como banco NoSQL distribuído orientado a colunas.

Enquanto o HDFS é otimizado para armazenamento de arquivos e processamento em grandes volumes, o HBase é destinado a cenários nos quais é necessário trabalhar com registros individualizados e acesso baseado em chave.

Na pipeline, isso é particularmente relevante para o processamento em tempo real realizado pelo Flink.

```text
Apache Flink
     │
     │ eventos processados
     ▼
   HBase
     │
     ├── Row Key
     ├── Column Family
     ├── Qualifiers
     └── Values
```

---

# Modelo de dados do HBase

O HBase não utiliza o modelo relacional tradicional de tabelas, linhas e colunas fixas.

Seu modelo é baseado em:

- tabela;
- Row Key;
- Column Family;
- Column Qualifier;
- timestamp;
- valor.

Conceitualmente:

```text
Tabela
│
├── Row Key
│
├── Column Family
│     ├── Qualifier
│     ├── Qualifier
│     └── Qualifier
│
└── Version / Timestamp
```

A **Row Key** é particularmente importante porque participa diretamente do padrão de acesso aos registros.

Por isso, seu desenho deve considerar:

- unicidade;
- distribuição dos registros;
- padrão de consulta;
- risco de hotspot;
- ordenação necessária;
- volume de escrita.

---

# HBase no processamento em tempo real

O processamento realizado pelo Flink pode produzir informações que precisam ser persistidas imediatamente.

O fluxo conceitual é:

```text
Eventos
   │
   ▼
Apache Flume
   │
   ▼
Apache Flink
   │
   ├── Parsing
   ├── Event Time
   ├── Watermarks
   ├── Sliding Windows
   ├── Agregações
   └── Alertas
          │
          ▼
        HBase
```

Essa integração permite que resultados derivados do processamento de eventos sejam persistidos sem depender exclusivamente do processamento histórico.

---

# HBase Thrift

A integração da aplicação com o HBase pode utilizar a interface **Thrift**, disponibilizando uma camada de comunicação entre o processamento e o banco.

No ambiente do projeto, o serviço Thrift pode ser disponibilizado na porta:

```text
9090
```

A comunicação conceitual é:

```text
Flink
  │
  │ Thrift
  ▼
HBase Thrift
  │
  ▼
HBase
```

A existência dessa camada permite desacoplar o código responsável pelo processamento da implementação interna do armazenamento.

---

# HBase e dados em tempo real

A escolha do HBase está relacionada principalmente ao padrão de acesso.

O objetivo não é substituir o HDFS.

A diferença pode ser representada da seguinte maneira:

```text
HDFS
│
├── grandes volumes
├── arquivos
├── histórico
├── processamento batch
└── throughput

HBase
│
├── registros individualizados
├── acesso por chave
├── baixa latência
├── atualizações
└── dados utilizados operacionalmente
```

Assim, HDFS e HBase podem coexistir na mesma arquitetura sem representar duplicação de responsabilidade.

---

# Hive

## Responsabilidade

O **Apache Hive** fornece uma camada de consulta SQL para dados armazenados no ecossistema Hadoop.

Sua função principal na arquitetura é permitir que os dados possam ser explorados por meio de uma interface analítica baseada em SQL.

```text
              Apache Hive
                   │
                   ▼
             HiveServer2
                   │
                   ▼
               Metastore
                   │
                   ▼
                  HDFS
```

O Hive não deve ser interpretado simplesmente como mais um banco de dados independente.

Ele funciona como uma camada de abstração e consulta sobre os dados disponíveis no ambiente distribuído.

---

# Hive Metastore

O Hive utiliza o **Metastore** para manter informações sobre os objetos utilizados pelo Hive.

Entre essas informações estão:

- databases;
- tabelas;
- colunas;
- tipos;
- localização dos dados;
- metadados necessários para interpretação das estruturas.

No ambiente Docker do projeto, o Metastore possui comunicação própria e pode utilizar uma instância PostgreSQL como banco de metadados.

Conceitualmente:

```text
                 HiveServer2
                     │
                     ▼
                Hive Metastore
                     │
                     ▼
                 PostgreSQL
              (metadados Hive)
```

É importante diferenciar:

> O PostgreSQL utilizado pelo Metastore não representa o armazenamento principal dos eventos do pipeline.

Ele mantém os **metadados do Hive** necessários para gerenciamento e consulta das estruturas.

---

# HiveServer2

O HiveServer2 fornece a interface de acesso aos serviços SQL do Hive.

No ambiente do projeto, a comunicação é disponibilizada através das portas configuradas no Docker Compose, incluindo:

```text
10000
10002
```

A arquitetura conceitual é:

```text
Cliente / Spark / Ferramenta SQL
             │
             ▼
        HiveServer2
             │
             ▼
       Hive Metastore
             │
             ▼
            HDFS
```

---

# Hive e Spark

O Hive também se integra ao processamento batch.

O Spark pode utilizar estruturas SQL e dados disponibilizados no ecossistema Hive para realizar operações analíticas.

Essa integração permite combinar:

```text
HDFS
  │
  ▼
Hive
  │
  ▼
Spark SQL
  │
  ├── SELECT
  ├── JOIN
  ├── GROUP BY
  ├── FILTER
  └── AGGREGATION
```

Dessa maneira, o projeto consegue combinar processamento distribuído com uma interface SQL.

---

# HDFS × HBase × Hive

Uma das decisões arquiteturais mais importantes do projeto é entender que as três tecnologias resolvem problemas diferentes.

## HDFS

O HDFS é orientado ao armazenamento de arquivos.

É apropriado quando o objetivo é:

- armazenar grandes volumes;
- manter dados históricos;
- disponibilizar arquivos para processamento;
- trabalhar com Spark;
- manter dados distribuídos.

## HBase

O HBase é orientado ao acesso a registros distribuídos.

É apropriado quando o objetivo é:

- acessar registros por chave;
- realizar operações de baixa latência;
- persistir resultados do processamento em tempo real;
- trabalhar com modelo NoSQL;
- disponibilizar dados sem depender de uma consulta analítica completa.

## Hive

O Hive é orientado à consulta analítica.

É apropriado quando o objetivo é:

- consultar dados utilizando SQL;
- explorar históricos;
- realizar agregações;
- executar joins;
- disponibilizar uma camada analítica sobre o ecossistema Hadoop.

---

# Comparação arquitetural

| Característica | HDFS | HBase | Hive |
|---|---|---|---|
| Tipo | Sistema de arquivos distribuído | Banco NoSQL | Data warehouse / camada SQL |
| Unidade principal | Arquivo | Registro / Row Key | Tabela |
| Modelo | Arquivos e diretórios | Wide-column | Relacional/SQL |
| Principal acesso | Arquivos | Chave | Consulta SQL |
| Uso no projeto | Histórico e persistência | Dados em tempo real | Análise |
| Integração principal | Spark | Flink | Spark / SQL |
| Latência de acesso | Voltada a throughput | Baixa latência por chave | Consulta analítica |
| Processamento | Batch | Operacional/tempo real | Analítico |

---

# Fluxo de armazenamento da pipeline

A camada de storage recebe dados provenientes de diferentes etapas da arquitetura.

O fluxo completo pode ser representado como:

```text
                    DATA GENERATOR
                          │
                          ▼
                    Apache Flume
                          │
                          ▼
                    ┌───────────┐
                    │  Eventos  │
                    └─────┬─────┘
                          │
             ┌────────────┴────────────┐
             │                         │
             ▼                         ▼
        Apache Flink             Persistência
             │                         │
             │                         ▼
             │                       HDFS
             │                         │
             ▼                         │
           HBase                       │
                                       ▼
                                     Hive
                                       │
                                       ▼
                               Consultas analíticas
```

Na prática, os caminhos podem variar de acordo com o tipo de evento e com a etapa da pipeline responsável pela persistência.

---

# Separação entre tempo real e histórico

A arquitetura de armazenamento também permite separar dois padrões de utilização.

## Tempo real

```text
Evento
  │
  ▼
Flume
  │
  ▼
Flink
  │
  ▼
HBase
```

O objetivo é disponibilizar rapidamente os resultados derivados dos eventos.

## Histórico

```text
Evento / resultado
       │
       ▼
      HDFS
       │
       ▼
     Spark
       │
       ▼
     Hive
       │
       ▼
   Análise SQL
```

O objetivo é preservar dados para processamento histórico e consultas analíticas.

---

# Integração com o módulo Streaming

O módulo `streaming` utiliza o armazenamento para persistir resultados gerados durante o processamento contínuo.

A relação é:

```text
streaming
    │
    │ resultados processados
    ▼
 storage
    │
    ▼
  HBase
```

Essa integração é importante para que os resultados das janelas temporais e demais operações do Flink possam ser disponibilizados posteriormente.

---

# Integração com o módulo Batch

O módulo `batch` utiliza principalmente os dados persistidos para realizar processamento histórico.

O fluxo é:

```text
storage
   │
   ▼
 HDFS
   │
   ▼
 Spark
   │
   ├── RDD
   ├── DataFrame
   ├── Spark SQL
   ├── Join
   └── Aggregation
```

O resultado do processamento pode novamente ser persistido no ecossistema de dados para consultas e análises posteriores.

---

# Organização lógica dos dados

A organização dos dados deve manter separação entre:

```text
Dados de entrada
      │
      ▼
Dados processados
      │
      ▼
Dados analíticos
      │
      ▼
Dados utilizados em tempo real
```

Essa separação evita que diferentes padrões de acesso sejam tratados como se fossem equivalentes.

A estrutura física definitiva deve seguir os diretórios, configurações, tabelas e schemas definidos no código e nos arquivos de configuração do projeto.

---

# Persistência de eventos

Os eventos produzidos pelo gerador possuem informações que podem ser utilizadas por diferentes componentes.

Entre os principais conceitos estão:

- identificação do evento;
- tipo do evento;
- identificação do usuário;
- identificação do produto;
- identificação do pedido;
- timestamp;
- status;
- informações relacionadas à logística.

O mesmo domínio de eventos pode gerar diferentes representações conforme o mecanismo de armazenamento.

Por exemplo:

```text
Evento JSON
     │
     ├──────────────► HDFS
     │                 Arquivo
     │
     ├──────────────► HBase
     │                 Row Key / Columns
     │
     └──────────────► Hive
                       Tabela / SQL
```

Isso não significa necessariamente que todas as cópias precisem possuir a mesma estrutura física.

Cada representação deve atender ao padrão de uso correspondente.

---

# Docker e infraestrutura

Os componentes de armazenamento são executados como serviços do ambiente Docker Compose.

A inicialização completa do ambiente pode ser realizada com:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Para verificar o estado dos serviços:

```bash
docker compose -f docker/docker-compose.yml ps
```

Os componentes relacionados ao armazenamento podem incluir:

```text
ecommerce-namenode
ecommerce-datanode
ecommerce-hbase
ecommerce-hive
ecommerce-hive-metastore
```

Os nomes efetivamente utilizados devem seguir o `docker-compose.yml` atual do projeto.

---

# Verificação do HDFS

O estado dos serviços pode ser verificado com:

```bash
docker compose -f docker/docker-compose.yml ps
```

Também é possível acessar o NameNode:

```bash
docker exec -it ecommerce-namenode bash
```

Dentro do container:

```bash
hdfs dfs -ls /
```

Para verificar diretórios específicos:

```bash
hdfs dfs -ls -R /
```

Para verificar o espaço utilizado:

```bash
hdfs dfs -df -h
```

Para verificar o estado do filesystem:

```bash
hdfs dfsadmin -report
```

Esses comandos permitem validar se o HDFS está operacional e se os DataNodes estão registrados corretamente.

---

# Verificação do HBase

O container do HBase pode ser acessado com:

```bash
docker exec -it ecommerce-hbase bash
```

A partir do ambiente do HBase, é possível utilizar o shell:

```bash
hbase shell
```

Para verificar as tabelas disponíveis:

```text
list
```

Para verificar a estrutura de uma tabela:

```text
describe 'ecommerce:realtime_alerts'
```

Para consultar registros:

```text
scan 'ecommerce:realtime_alerts'
```

A existência e a estrutura das tabelas devem ser verificadas de acordo com os schemas efetivamente utilizados pelo projeto.

---

# Verificação do HBase Thrift

Quando o serviço Thrift estiver sendo utilizado, a porta configurada pode ser verificada no container:

```bash
docker exec ecommerce-hbase bash -c \
'netstat -lntp 2>/dev/null | grep 9090 || true'
```

O resultado esperado é a existência de um processo escutando na porta:

```text
9090
```

O serviço pode ser iniciado no ambiente de desenvolvimento com:

```bash
docker exec -d ecommerce-hbase hbase thrift start -p 9090
```

Depois:

```bash
sleep 3
```

E novamente:

```bash
docker exec ecommerce-hbase bash -c \
'netstat -lntp 2>/dev/null | grep 9090 || true'
```

A porta somente deve ser considerada disponível quando houver efetivamente um processo escutando nela.

---

# Verificação do Hive

O estado dos componentes Hive pode ser verificado através do Docker:

```bash
docker compose -f docker/docker-compose.yml ps
```

Para consultar os logs do HiveServer2:

```bash
docker logs -f ecommerce-hive
```

Para consultar o Metastore:

```bash
docker logs -f ecommerce-hive-metastore
```

A comunicação do Metastore normalmente utiliza a porta:

```text
9083
```

Enquanto o HiveServer2 utiliza as portas configuradas para acesso SQL, incluindo:

```text
10000
10002
```

A disponibilidade deve ser validada no ambiente antes da execução de consultas.

---

# Hive Metastore e PostgreSQL

O PostgreSQL utilizado pelo Hive Metastore possui uma responsabilidade específica:

```text
                  PostgreSQL
                       ▲
                       │
                  Metadados
                       │
                Hive Metastore
                       │
                       ▼
                  HiveServer2
```

O banco PostgreSQL mantém informações administrativas do Metastore, e não deve ser confundido com o armazenamento distribuído dos eventos.

Os dados do pipeline permanecem associados ao ecossistema Hadoop e às estruturas utilizadas pelo Hive.

---

# Observabilidade

A camada de armazenamento deve ser monitorada porque falhas nessa etapa podem interromper o fluxo completo da pipeline.

Os principais pontos de observação incluem:

### HDFS

- NameNode disponível;
- DataNode registrado;
- capacidade disponível;
- arquivos presentes;
- permissões;
- erros de leitura/escrita.

### HBase

- RegionServer disponível;
- tabela existente;
- namespace existente;
- Row Keys sendo gravadas;
- conexão Thrift;
- erros de escrita;
- latência de acesso.

### Hive

- HiveServer2 disponível;
- Metastore disponível;
- conexão com PostgreSQL;
- tabelas registradas;
- localização dos dados;
- execução das consultas.

---

# Diagnóstico de problemas

## HDFS indisponível

Verificar:

```bash
docker compose -f docker/docker-compose.yml ps
```

Depois:

```bash
docker logs ecommerce-namenode
```

E:

```bash
docker logs ecommerce-datanode
```

---

## HBase indisponível

Verificar:

```bash
docker logs ecommerce-hbase
```

Depois:

```bash
docker exec -it ecommerce-hbase bash
```

E:

```bash
hbase shell
```

Se a integração depender do Thrift:

```bash
docker exec ecommerce-hbase bash -c \
'netstat -lntp 2>/dev/null | grep 9090 || true'
```

---

## HiveServer2 indisponível

Verificar:

```bash
docker logs ecommerce-hive
```

Depois:

```bash
docker logs ecommerce-hive-metastore
```

Também deve ser validada a comunicação entre:

```text
HiveServer2
     │
     ▼
Metastore
     │
     ▼
PostgreSQL
```

Uma falha no Metastore pode impedir o funcionamento correto das operações dependentes de metadados.

---

# Consistência arquitetural

Os três componentes não devem ser tratados como substitutos diretos.

A arquitetura segue uma separação baseada no padrão de acesso:

```text
                  DADOS
                    │
          ┌─────────┼─────────┐
          │         │         │
          ▼         ▼         ▼
        HDFS      HBase     Hive
          │         │         │
          │         │         │
       Volume     Baixa      SQL
       histórico  latência    analítico
```

Essa decisão permite que cada mecanismo seja utilizado onde apresenta maior aderência ao requisito funcional da pipeline.

---

# Fluxo completo de armazenamento

Considerando todas as camadas:

```text
┌──────────────────────┐
│    Data Generator    │
│                      │
│ Eventos JSON         │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│       Apache Flume   │
│                      │
│ Ingestão              │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Processamento   │
│                      │
│ Flink / Spark        │
└───────┬─────────┬────┘
        │         │
        │         │
        ▼         ▼
   ┌────────┐ ┌────────┐
   │  HDFS  │ │  HBase │
   └────┬───┘ └────────┘
        │
        ▼
   ┌────────┐
   │  Hive  │
   └────┬───┘
        │
        ▼
   Consultas SQL
   e análises
```

---

# Papel do Storage na arquitetura geral

O módulo `storage` funciona como a camada responsável por transformar os resultados transitórios da pipeline em dados persistentes e reutilizáveis.

Sua posição na arquitetura pode ser resumida como:

```text
             INGESTÃO
                 │
                 ▼
           PROCESSAMENTO
                 │
                 ▼
             STORAGE
          ┌──────┼──────┐
          ▼      ▼      ▼
        HDFS   HBase   Hive
          │      │      │
          ▼      ▼      ▼
       Histórico Real-  Análise
                  time
```

Assim, o armazenamento conecta o processamento de eventos em tempo real ao processamento histórico e à análise dos dados.

---

# Dependências

A camada `storage` depende da infraestrutura distribuída utilizada pelo restante da pipeline.

Conceitualmente:

```text
Docker
  │
  ├── Hadoop
  │    ├── NameNode
  │    └── DataNode
  │
  ├── HBase
  │    └── Thrift
  │
  └── Hive
       ├── HiveServer2
       ├── Metastore
       └── PostgreSQL
```

Além disso, os consumidores dos dados dependem das respectivas interfaces:

```text
Flink ───────► HBase
Spark ───────► HDFS
Spark/Hive ──► Hive
```

---

# Validação da camada

Uma validação mínima do storage deve confirmar:

### HDFS

```bash
hdfs dfs -ls /
```

### HBase

```text
list
```

E, quando aplicável:

```text
scan 'ecommerce:realtime_alerts'
```

### Hive

A validação deve confirmar:

- conexão com HiveServer2;
- acesso ao Metastore;
- existência das tabelas;
- disponibilidade dos dados;
- execução de consultas SQL.

---

# Critérios de funcionamento

A camada de armazenamento é considerada operacional quando:

- HDFS está disponível;
- NameNode reconhece os DataNodes;
- arquivos podem ser gravados e lidos;
- HBase está disponível;
- tabelas necessárias existem;
- registros podem ser persistidos;
- Thrift está disponível quando utilizado;
- HiveServer2 está disponível;
- Metastore está conectado;
- estruturas Hive podem ser consultadas;
- Spark consegue acessar os dados necessários;
- Flink consegue persistir os resultados destinados ao HBase.

---

# Considerações de arquitetura

A principal decisão arquitetural deste módulo é evitar utilizar uma única tecnologia para todos os padrões de armazenamento.

O HDFS atende ao armazenamento distribuído de arquivos e ao histórico.

O HBase atende ao acesso NoSQL orientado a registros e chaves, especialmente para resultados associados ao processamento em tempo real.

O Hive fornece uma camada SQL para exploração e análise dos dados disponíveis no ecossistema Hadoop.

Portanto:

```text
HDFS  → Persistência distribuída
HBase → Acesso NoSQL / tempo real
Hive  → Consulta SQL / análise
```

Essa divisão permite que a pipeline utilize diferentes mecanismos sem misturar suas responsabilidades.

---

# Resumo

O módulo `storage` é responsável pela persistência dos dados produzidos e processados pela arquitetura de Big Data.

Sua implementação combina:

```text
┌─────────────────────────────────────┐
│              STORAGE                │
├─────────────────────────────────────┤
│                                     │
│  HDFS                               │
│  └── armazenamento distribuído      │
│                                     │
│  HBase                              │
│  └── acesso NoSQL por chave         │
│                                     │
│  Hive                               │
│  └── consultas SQL analíticas       │
│                                     │
└─────────────────────────────────────┘
```

A integração desses componentes permite que o projeto trabalhe simultaneamente com:

- eventos em tempo real;
- persistência histórica;
- grandes volumes de dados;
- consultas analíticas;
- processamento distribuído;
- acesso de baixa latência;
- integração com Flink;
- integração com Spark.

Dessa forma, o `storage` funciona como a camada que mantém os dados disponíveis depois das etapas de ingestão e processamento, permitindo que os resultados da pipeline sejam persistidos, consultados e reutilizados por diferentes componentes da arquitetura.