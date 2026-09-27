# Streaming

## Visão Geral

O módulo `streaming` implementa a camada de **processamento de eventos em tempo real** da pipeline de Big Data utilizando **Apache Flink**.

Seu objetivo é processar continuamente os eventos provenientes da camada de ingestão, utilizando o timestamp associado a cada evento para realizar processamento baseado em **event time**.

A implementação contempla os principais mecanismos necessários para processamento de fluxos contínuos:

- processamento de eventos em tempo real;
- parsing e normalização dos eventos;
- atribuição de timestamps;
- geração de watermarks;
- tratamento de eventos fora de ordem;
- janelas temporais deslizantes;
- agregações;
- identificação de condições de alerta;
- persistência dos resultados;
- checkpoints para recuperação do estado.

O fluxo geral é:

```text
                    ┌─────────────────────┐
                    │      Ingestion      │
                    │     Apache Flume    │
                    └──────────┬──────────┘
                               │
                               ▼
                    ┌─────────────────────┐
                    │       Flink         │
                    │                     │
                    │  Source             │
                    │    ↓                │
                    │  Parsing            │
                    │    ↓                │
                    │  Event Time         │
                    │    ↓                │
                    │  Watermark          │
                    │    ↓                │
                    │  Sliding Window     │
                    │    ↓                │
                    │  Aggregation        │
                    │    ↓                │
                    │  Alerting            │
                    └──────────┬──────────┘
                               │
                               ▼
                           Storage
```

---

# Objetivo do Streaming

O processamento streaming foi desenvolvido para permitir que os eventos sejam analisados **à medida que são produzidos**, sem depender da conclusão de um lote de dados.

Em vez de aguardar a acumulação de registros:

```text
Eventos
  │
  ▼
Batch
  │
  ▼
Processamento
```

o modelo streaming trabalha continuamente:

```text
Evento ──► Processamento ──► Resultado
Evento ──► Processamento ──► Resultado
Evento ──► Processamento ──► Resultado
Evento ──► Processamento ──► Resultado
```

Isso permite que condições relevantes sejam identificadas durante a execução da pipeline.

---

# Papel na Arquitetura

O `streaming` ocupa a camada de processamento em tempo real da arquitetura.

```text
┌─────────────────────┐
│   DATA GENERATOR    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      INGESTION      │
│     Apache Flume    │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      STREAMING      │
│                     │
│     Apache Flink    │
└──────────┬──────────┘
           │
           ├──────────────► HBase
           │
           └──────────────► Alertas
```

Enquanto a camada `ingestion` é responsável pelo transporte dos eventos, o `streaming` é responsável pela **interpretação temporal e processamento dos eventos**.

---

# Apache Flink

O processamento é executado utilizando Apache Flink, um framework distribuído voltado ao processamento de dados contínuos e aplicações orientadas a eventos.

O Flink permite que a pipeline trabalhe com:

- streams contínuos;
- event time;
- processamento por janelas;
- estado;
- watermarks;
- checkpoints;
- processamento distribuído.

Na pipeline, esses recursos são utilizados para demonstrar o processamento de eventos de e-commerce em tempo real.

---

# Fluxo de Processamento

O processamento pode ser representado pela seguinte sequência:

```text
Eventos
   │
   ▼
Source
   │
   ▼
Parsing JSON
   │
   ▼
Validação
   │
   ▼
Extração do timestamp
   │
   ▼
Event Time
   │
   ▼
Watermark
   │
   ▼
Sliding Window
   │
   ▼
Agregação
   │
   ▼
Detecção de condição
   │
   ├──────────────► Evento normal
   │
   └──────────────► Alerta
                         │
                         ▼
                      Storage
```

Cada etapa possui uma função específica dentro do processamento.

---

# Event Time

Um dos pontos fundamentais da implementação é a utilização de **event time**.

Event time representa o momento em que o evento realmente ocorreu, de acordo com o timestamp presente no próprio evento.

Isso é diferente de utilizar simplesmente o instante em que o Flink recebeu o registro.

### Exemplo

Considere os seguintes eventos:

```text
Evento A → timestamp 10:00:01
Evento B → timestamp 10:00:03
Evento C → timestamp 10:00:02
```

Os eventos chegaram na ordem:

```text
A → B → C
```

mas ocorreram na ordem:

```text
A → C → B
```

O evento `C` está, portanto, **fora de ordem de chegada**.

A utilização de event time permite que o processamento considere o timestamp associado ao evento, e não apenas a ordem física de chegada.

---

# Eventos Fora de Ordem

Em sistemas distribuídos, eventos podem chegar ao processamento em uma ordem diferente daquela em que foram originalmente produzidos.

Isso pode ocorrer devido a:

- latência de rede;
- atraso no produtor;
- filas intermediárias;
- diferenças de velocidade entre componentes;
- processamento distribuído;
- atrasos temporários no transporte.

A arquitetura utiliza watermarks para lidar com esse cenário.

```text
Tempo do evento:

10:01 ────── 10:02 ────── 10:03 ────── 10:04
               ▲
               │
          evento atrasado
```

O objetivo não é simplesmente descartar qualquer evento que não chegue imediatamente na ordem esperada.

O processamento baseado em event time permite considerar uma margem para eventos atrasados.

---

# Watermarks

A **watermark** representa uma estimativa do progresso do tempo de evento dentro do fluxo.

Ela permite ao Flink determinar quando possui informações suficientes para considerar que uma determinada região temporal pode ser processada.

Conceitualmente:

```text
Eventos recebidos
       │
       ▼
Extração do timestamp
       │
       ▼
Maior timestamp observado
       │
       ▼
Watermark
       │
       ▼
Progresso do event time
```

A watermark é especialmente importante quando o fluxo possui eventos fora de ordem.

---

## Exemplo de Watermark

Suponha que o maior timestamp observado seja:

```text
12:00:20
```

e a estratégia permita um atraso de:

```text
5 segundos
```

A watermark poderá avançar aproximadamente para:

```text
12:00:15
```

Isso significa que o mecanismo de processamento considera que eventos significativamente anteriores a esse ponto já deveriam ter chegado.

O valor exato depende da estratégia configurada na aplicação.

---

# Relação entre Watermark e Janela

A watermark também controla o progresso das janelas baseadas em event time.

```text
Eventos
   │
   ▼
Timestamps
   │
   ▼
Watermark
   │
   ▼
Janela temporal
   │
   ▼
Resultado
```

Sem uma referência temporal adequada, uma janela baseada exclusivamente na chegada dos eventos poderia produzir resultados inconsistentes quando os dados chegassem fora de ordem.

---

# Sliding Windows

A pipeline utiliza **janelas deslizantes** para analisar continuamente períodos sobrepostos do fluxo de eventos.

Uma sliding window possui:

- tamanho da janela;
- intervalo de avanço.

Por exemplo, conceitualmente:

```text
Window size = 60 segundos
Slide       = 10 segundos
```

O processamento poderia produzir:

```text
00s ───────────────── 60s
       │
10s ───────────────── 70s
       │
20s ───────────────── 80s
       │
30s ───────────────── 90s
```

As janelas se sobrepõem, permitindo acompanhar continuamente o comportamento do fluxo.

---

# Vantagem das Janelas Deslizantes

Uma janela fixa poderia produzir:

```text
[00 ───── 60]
             [60 ───── 120]
```

Enquanto uma sliding window produz:

```text
[00 ───── 60]
    [10 ───── 70]
        [20 ───── 80]
            [30 ───── 90]
```

Essa sobreposição permite obter resultados com maior frequência, mantendo uma visão temporal mais contínua.

---

# Agregação

Depois da aplicação da janela temporal, os eventos podem ser agrupados e agregados de acordo com os campos utilizados pelo processamento.

O fluxo é:

```text
Stream
  │
  ▼
Window
  │
  ▼
Group / Key
  │
  ▼
Aggregation
  │
  ▼
Result
```

As agregações podem ser utilizadas para identificar padrões relacionados ao comportamento das vendas e eventos da plataforma.

---

# Detecção de Alertas

A camada de streaming também possui responsabilidade pela geração de **alertas derivados do processamento dos eventos**.

Um alerta pode ser gerado quando uma determinada condição é identificada dentro de uma janela.

Conceitualmente:

```text
Eventos
   │
   ▼
Sliding Window
   │
   ▼
Agregação
   │
   ▼
Regra
   │
   ├──────────────► Condição normal
   │
   └──────────────► Condição de alerta
                           │
                           ▼
                        Alerta
```

O objetivo é permitir que comportamentos relevantes sejam identificados sem esperar pelo processamento histórico.

---

# Exemplo Conceitual de Alerta

Considere uma janela temporal que contabiliza determinados eventos.

```text
Janela
│
├── Evento
├── Evento
├── Evento
├── Evento
└── Evento
```

Após a agregação:

```text
Quantidade = N
```

Caso `N` atenda à condição configurada pela aplicação:

```text
N >= threshold
```

o processamento poderá produzir um alerta.

O threshold utilizado deve ser considerado o valor definido na implementação e na configuração do projeto.

---

# Estado do Processamento

Operações de streaming podem manter estado durante a execução.

Esse estado permite que o Flink acompanhe informações necessárias para:

- agregações;
- janelas;
- processamento incremental;
- identificação de condições;
- recuperação após falhas.

O estado é parte fundamental de aplicações de processamento contínuo.

---

# Checkpoints

A aplicação utiliza o mecanismo de **checkpointing do Apache Flink** para registrar periodicamente o estado necessário à recuperação da aplicação.

O conceito pode ser representado como:

```text
Stream
  │
  ▼
Flink Job
  │
  ├── State
  │
  └── Checkpoint
          │
          ▼
       Storage
```

Caso ocorra uma falha, os checkpoints permitem que a aplicação seja recuperada a partir de um estado consistente previamente armazenado.

---

# Recuperação

O processo de recuperação pode ser representado como:

```text
Execução
   │
   ▼
Checkpoint
   │
   ▼
Falha
   │
   ▼
Recuperação
   │
   ▼
Estado salvo
   │
   ▼
Continuação
```

Isso é importante em uma pipeline contínua porque o processamento não deve depender exclusivamente do estado mantido em memória durante a execução atual.

---

# Checkpoints na Execução

Durante a execução do ambiente, os checkpoints podem ser observados pela interface do Flink.

Uma execução saudável apresenta a evolução dos checkpoints conforme o job permanece ativo.

Os checkpoints permitem acompanhar:

- número do checkpoint;
- estado da execução;
- tamanho do checkpoint;
- duração;
- quantidade de checkpoints concluídos;
- progresso da persistência do estado.

---

# Interface Web do Flink

A interface Web do Flink permite acompanhar a execução dos jobs.

No ambiente Docker utilizado pela pipeline, a interface é disponibilizada através da porta configurada para o JobManager.

Acessando a interface, é possível observar:

```text
Jobs
  │
  ├── Job Status
  ├── Operators
  ├── Metrics
  ├── Checkpoints
  └── Task Managers
```

A interface é especialmente útil durante a demonstração da pipeline porque permite visualizar o processamento em execução.

---

# Operadores

O job de streaming é composto por diferentes operações encadeadas.

Conceitualmente:

```text
Source
  │
  ▼
Parse
  │
  ▼
Timestamp / Watermark
  │
  ▼
Window
  │
  ▼
Aggregation
  │
  ▼
Alert
  │
  ▼
Sink
```

A interface do Flink permite visualizar esses operadores e acompanhar métricas associadas à execução.

---

# Métricas

As métricas da aplicação podem ser utilizadas para acompanhar o comportamento do job.

Entre os indicadores relevantes estão:

- registros processados;
- registros recebidos;
- throughput;
- latência;
- estado dos operadores;
- progresso das janelas;
- progresso das watermarks;
- checkpoints;
- falhas e reinicializações.

Essas informações complementam os logs do container.

---

# Persistência dos Resultados

Após o processamento, os resultados podem ser encaminhados para as camadas de armazenamento utilizadas pela pipeline.

Uma das integrações previstas é com o HBase.

O fluxo pode ser representado como:

```text
Flink
  │
  ▼
Processed Event
  │
  ▼
HBase Sink
  │
  ▼
HBase
```

Essa integração permite armazenar resultados do processamento streaming para posterior consulta ou validação.

---

# Integração com HBase

O HBase é utilizado como uma camada de armazenamento adequada para resultados que precisam estar disponíveis após o processamento dos eventos.

O streaming pode produzir registros relacionados a:

- eventos processados;
- agregações;
- alertas;
- informações derivadas das janelas.

A implementação do sink deve manter o processamento desacoplado da lógica específica de armazenamento.

```text
Flink Processing
       │
       ▼
   Sink Layer
       │
       ▼
      HBase
```

---

# Tratamento de Erros

O processamento streaming deve lidar com problemas que podem ocorrer durante o fluxo.

Entre os problemas possíveis estão:

- JSON inválido;
- campos ausentes;
- timestamp inválido;
- eventos fora da estrutura esperada;
- falha de comunicação com o destino;
- falha temporária de infraestrutura;
- reinicialização do job.

O tratamento deve impedir que um problema localizado provoque necessariamente a interrupção completa do fluxo.

---

# Logs

Os logs do container permitem acompanhar a inicialização e execução da aplicação.

Para visualizar os logs:

```bash
docker logs -f ecommerce-flink
```

Caso o job seja submetido a partir do container responsável pelo processamento:

```bash
docker exec -it ecommerce-flink bash
```

A partir do ambiente interno, podem ser utilizados os comandos e scripts definidos pelo projeto para iniciar ou verificar o job.

---

# Execução com Docker

A camada de streaming é executada dentro do ambiente Docker da pipeline.

Para iniciar os serviços:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Para verificar o estado:

```bash
docker compose -f docker/docker-compose.yml ps
```

Para verificar o serviço Flink:

```bash
docker ps | grep flink
```

Os nomes exatos dos containers devem seguir a configuração do `docker-compose.yml`.

---

# Verificação do Job

Depois que o ambiente estiver iniciado, a execução deve ser validada em três níveis.

### 1. Container

```bash
docker ps
```

### 2. Interface do Flink

Verificar:

- JobManager;
- TaskManagers;
- jobs ativos;
- operadores;
- checkpoints.

### 3. Logs

```bash
docker logs --tail 100 ecommerce-flink
```

A combinação dessas três verificações permite confirmar tanto a disponibilidade da infraestrutura quanto a execução do processamento.

---

---

# Streaming x Batch

A camada de streaming possui uma finalidade diferente da camada `batch`.

| Streaming | Batch |
|---|---|
| Processamento contínuo | Processamento histórico |
| Apache Flink | Apache Spark |
| Event time | Processamento sobre conjuntos de dados |
| Watermarks | ETL |
| Sliding windows | RDDs |
| Eventos em tempo real | Dados persistidos |
| Alertas durante a execução | Análises históricas |

O streaming permite responder ao fluxo enquanto ele acontece, enquanto o processamento batch trabalha sobre dados já acumulados.

---

# Por que Apache Flink?

O Apache Flink foi utilizado na camada de streaming porque fornece abstrações específicas para processamento contínuo baseado em eventos.

Entre os recursos relevantes para este projeto estão:

- processamento por event time;
- watermarks;
- janelas temporais;
- estado;
- checkpoints;
- processamento contínuo;
- operadores de transformação;
- integração com diferentes sistemas externos.

Esses recursos permitem demonstrar os conceitos de processamento de streams exigidos pela arquitetura da pipeline.

---

# Tratamento de Eventos Atrasados

O tratamento temporal da aplicação considera que a ordem de chegada dos eventos pode não representar a ordem em que eles ocorreram.

```text
Ordem de ocorrência:

A ──► B ──► C

Ordem de chegada:

A ──► C ──► B
```

As watermarks fornecem ao Flink uma forma de acompanhar o progresso do event time e determinar quando uma janela pode avançar no processamento.

Esse mecanismo é essencial para aplicações distribuídas nas quais atrasos podem ocorrer durante o transporte dos dados.

---

# Cenário de E-commerce

O processamento streaming foi projetado para representar um ambiente de comércio eletrônico no qual eventos são produzidos continuamente.

Os eventos podem representar:

```text
Click
  │
  ▼
Cart
  │
  ▼
Order
  │
  ▼
Delivery
```

Esses eventos podem ser processados individualmente ou agrupados em janelas temporais para produzir informações derivadas.

A arquitetura permite analisar tanto o comportamento de usuários quanto informações relacionadas ao fluxo de vendas e logística.

---

As principais evidências são:

```text
┌─────────────────────────────┐
│       Flink Dashboard       │
├─────────────────────────────┤
│ Job em execução             │
│                             │
│ Operators                   │
│ Watermarks                  │
│ Windows                     │
│ Checkpoints                 │
│ Metrics                     │
│ Task Managers               │
└─────────────────────────────┘
```

Essas informações demonstram que o processamento não está apenas configurado, mas efetivamente executando os eventos produzidos pela pipeline.

---

# Diagnóstico de Problemas

Quando o streaming não estiver processando eventos, a investigação deve seguir a ordem:

```text
Generator
    │
    ▼
Flume
    │
    ▼
Flink Source
    │
    ▼
Parsing
    │
    ▼
Window
    │
    ▼
Sink
```

### Verificar containers

```bash
docker compose -f docker/docker-compose.yml ps
```

### Verificar logs

```bash
docker logs --tail 100 ecommerce-flink
```

### Verificar Flink

Acessar a interface Web e verificar o estado do job.

### Verificar entrada

Confirmar se o Flume está recebendo e encaminhando eventos.

### Verificar destino

Confirmar se o sink consegue escrever no sistema de armazenamento configurado.

---

# Dependências

A camada de streaming depende da disponibilidade do fluxo de entrada e dos componentes necessários para persistência dos resultados.

```text
Data Generator
      │
      ▼
Apache Flume
      │
      ▼
Apache Flink
      │
      ▼
HBase / Storage
```

Também são necessários:

- configuração válida do job;
- ambiente Flink ativo;
- TaskManager disponível;
- conectividade com os componentes downstream;
- eventos em formato compatível;
- timestamps válidos para processamento temporal.

---

# Resumo

O módulo `streaming` implementa o processamento contínuo da pipeline utilizando Apache Flink.

A camada recebe eventos produzidos continuamente, interpreta seus timestamps, acompanha o progresso do event time através de watermarks e processa os dados utilizando janelas deslizantes.

A utilização de event time e watermarks permite lidar com eventos que chegam fora de ordem, enquanto as sliding windows possibilitam realizar análises contínuas sobre períodos temporais sobrepostos.

O processamento também contempla agregações, geração de alertas e persistência dos resultados, enquanto os checkpoints fornecem mecanismos para recuperação do estado da aplicação.

```text
Eventos
   │
   ▼
Flume
   │
   ▼
Flink
   │
   ├── Event Time
   ├── Watermarks
   ├── Sliding Windows
   ├── Aggregations
   ├── Alerts
   └── Checkpoints
          │
          ▼
        HBase
```

Essa camada representa o núcleo de **processamento em tempo real** da arquitetura de Big Data, conectando a ingestão contínua dos eventos às operações de análise e persistência dos resultados.