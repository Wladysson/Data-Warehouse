# Ingestion

## Visão Geral

O módulo `ingestion` é responsável pela **coleta, validação, transporte e encaminhamento dos eventos produzidos pelo `data-generator`** para as próximas etapas da pipeline de Big Data.

A camada utiliza o **Apache Flume** como mecanismo de ingestão, fornecendo uma estrutura baseada em:

- Source;
- Channel;
- Sink.

Essa arquitetura permite desacoplar a geração dos eventos do processamento posterior, criando uma etapa intermediária responsável por receber os dados, armazená-los temporariamente durante o transporte e encaminhá-los ao destino configurado.

O fluxo principal é:

```text
Data Generator
      │
      │ Eventos JSON
      ▼
┌─────────────────────┐
│    Apache Flume     │
│                     │
│      Source         │
│         │           │
│         ▼           │
│      Channel        │
│         │           │
│         ▼           │
│       Sink          │
└─────────┬───────────┘
          │
          ▼
Processamento / Storage
```

---

## Responsabilidade na Arquitetura

A camada de ingestão funciona como a **fronteira entre a origem dos eventos e os componentes de processamento da plataforma**.

Enquanto o `data-generator` é responsável por produzir os eventos, o `ingestion` é responsável por transportá-los de maneira estruturada para os componentes seguintes.

Suas principais responsabilidades são:

1. receber os eventos;
2. interpretar o fluxo de entrada;
3. transportar os eventos através do Channel;
4. encaminhar os eventos para o destino configurado;
5. preservar o desacoplamento entre produtor e consumidor;
6. permitir observabilidade do fluxo de ingestão;
7. fornecer uma camada controlada de entrada para o restante da pipeline.

---

## Arquitetura do Apache Flume

O Apache Flume utiliza uma arquitetura composta por três elementos principais:

```text
┌──────────────┐
│    Source    │
│              │
│ Recebe dados │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│   Channel    │
│              │
│ Bufferização │
└──────┬───────┘
       │
       ▼
┌──────────────┐
│     Sink     │
│              │
│ Encaminha    │
└──────┬───────┘
       │
       ▼
 Destino
```

Essa separação é importante porque cada componente possui uma responsabilidade específica dentro do fluxo de ingestão.

---

# Source

O **Source** representa o ponto de entrada dos eventos no Apache Flume.

É responsável por receber os dados produzidos pelo `data-generator` e transformá-los em eventos que possam ser processados pelo agente Flume.

No contexto desta pipeline, os eventos são produzidos continuamente e encaminhados para o agente configurado.

O fluxo conceitual é:

```text
Data Generator
      │
      │ JSON
      ▼
Flume Source
```

O Source não realiza o processamento analítico dos dados. Sua responsabilidade está relacionada à entrada e à adaptação dos dados para o modelo de eventos do Flume.

---

# Channel

O **Channel** funciona como uma camada intermediária entre Source e Sink.

Seu objetivo é armazenar temporariamente os eventos antes que eles sejam consumidos pelo Sink.

```text
Source
  │
  ▼
Channel
  │
  ▼
Sink
```

Essa separação proporciona desacoplamento entre a velocidade de produção e a velocidade de consumo dos eventos.

Por exemplo, caso o produtor gere eventos mais rapidamente do que o destino consegue processá-los momentaneamente, o Channel pode atuar como mecanismo intermediário de transporte.

---

## Papel do Channel no Fluxo

O Channel é particularmente importante em uma arquitetura de ingestão porque evita que Source e Sink precisem operar diretamente acoplados.

A arquitetura passa a funcionar como:

```text
┌─────────────┐
│   Producer  │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│    Source   │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│   Channel   │
│             │
│  Buffer     │
└──────┬──────┘
       │
       ▼
┌─────────────┐
│     Sink    │
└──────┬──────┘
       │
       ▼
   Destino
```

---

# Sink

O **Sink** é responsável por consumir os eventos presentes no Channel e encaminhá-los ao próximo componente da arquitetura.

Ele representa o ponto de saída do agente Flume.

```text
Channel
   │
   ▼
 Sink
   │
   ▼
Destino
```

A configuração do Sink determina para onde os eventos serão enviados após a etapa de ingestão.

Isso permite que o Flume funcione como uma camada de transporte entre a origem e os sistemas responsáveis pelo processamento ou armazenamento.

---

# Fluxo Completo de Ingestão

O fluxo completo pode ser representado da seguinte maneira:

```text
┌──────────────────────┐
│    Data Generator    │
│                      │
│ Click                │
│ Cart                 │
│ Order                │
│ Delivery             │
└──────────┬───────────┘
           │
           │ JSON
           ▼
┌──────────────────────┐
│     Flume Source     │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│    Flume Channel     │
│                      │
│ Bufferização         │
│ Transporte interno   │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│      Flume Sink      │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│ Próxima camada       │
│ da pipeline          │
└──────────────────────┘
```

---

# Eventos JSON

A entrada da camada de ingestão é composta por eventos serializados em JSON.

Os eventos podem representar diferentes operações do domínio do e-commerce, como:

```text
click
cart
order
delivery
```

A utilização de JSON facilita a interoperabilidade entre os componentes da arquitetura e permite que os eventos sejam transportados sem exigir uma estrutura binária específica.

---

# Validação dos Eventos

A camada de ingestão também participa da **validação inicial dos eventos** antes que eles avancem pela pipeline.

Essa validação deve ocorrer antes do processamento analítico, permitindo identificar eventos que não possuem uma estrutura compatível com o fluxo esperado.

Entre os aspectos que podem ser verificados estão:

- presença do tipo de evento;
- estrutura JSON válida;
- presença dos campos necessários;
- existência de timestamp;
- identificação dos elementos relacionados ao evento;
- compatibilidade com o formato esperado pela pipeline.

A validação nessa etapa evita que registros estruturalmente inválidos avancem para as camadas de processamento.

---

## Validação Estrutural

A validação estrutural pode ser representada como:

```text
Evento recebido
      │
      ▼
JSON válido?
      │
   ┌──┴──┐
   │     │
  SIM    NÃO
   │     │
   ▼     ▼
Validação  Evento
dos campos  inválido
   │
   ▼
Evento aceito
```

Essa etapa é diferente da validação de negócio.

A camada de ingestão está principalmente interessada em garantir que o evento possua uma estrutura adequada para ser transportado e processado pelas etapas seguintes.

---

# Tratamento de Eventos Inválidos

Eventos que não estejam de acordo com o formato esperado devem ser identificados durante o processo de ingestão.

O tratamento de registros inválidos pode envolver:

```text
Evento
  │
  ▼
Validação
  │
  ├──────────────► Válido
  │                   │
  │                   ▼
  │               Pipeline
  │
  └──────────────► Inválido
                      │
                      ▼
                 Tratamento
                 / descarte
                 / registro
```

O objetivo é impedir que dados estruturalmente incorretos contaminem as etapas posteriores.

---

# Desacoplamento

Um dos principais benefícios da utilização do Apache Flume é o **desacoplamento entre produção e consumo**.

Sem uma camada de ingestão:

```text
Generator ─────────────► Processing
```

Com Flume:

```text
Generator
    │
    ▼
Flume
    │
    ▼
Processing
```

Esse modelo permite que alterações no mecanismo de processamento não exijam necessariamente alterações na lógica responsável pela geração dos eventos.

---

# Integração com Streaming

Após a ingestão, os eventos podem seguir para a camada de processamento em tempo real.

O fluxo geral é:

```text
Data Generator
      │
      ▼
Apache Flume
      │
      ▼
Apache Flink
      │
      ├── Parsing
      ├── Timestamp
      ├── Watermarks
      ├── Sliding Windows
      └── Alertas
```

A camada de ingestão, portanto, prepara o fluxo para que o processamento de streaming possa trabalhar sobre os eventos recebidos.

---

# Integração com Storage

Os dados transportados pelo Flume também podem ser direcionados para componentes responsáveis pela persistência.

Na arquitetura do projeto, a camada de armazenamento contempla:

```text
HDFS
HBase
Hive
```

O fluxo pode ser representado como:

```text
Generator
    │
    ▼
Flume
    │
    ▼
Storage
    │
    ├── HDFS
    ├── HBase
    └── Hive
```

A persistência permite que os eventos sejam posteriormente utilizados para análises históricas e processamento batch.

---

# Integração com Batch

Os dados persistidos podem posteriormente alimentar os jobs Spark.

```text
Generator
    │
    ▼
Flume
    │
    ▼
Storage
    │
    ▼
Apache Spark
    │
    ├── RDD
    ├── ETL
    ├── Spark SQL
    └── Joins
```

Dessa forma, a ingestão participa tanto do fluxo de processamento em tempo real quanto do fluxo histórico.

---

# Configuração do Flume

A configuração do agente Flume define a relação entre:

```text
Source
Channel
Sink
```

No ambiente da pipeline, o agente é iniciado utilizando o arquivo de configuração disponibilizado no container:

```bash
flume-ng agent -n agent -f /opt/flume/conf/flume-conf.properties
```

O agente é identificado pelo nome:

```text
agent
```

e utiliza o arquivo:

```text
/opt/flume/conf/flume-conf.properties
```

A configuração centraliza o comportamento do componente de ingestão e permite reproduzir o mesmo fluxo dentro do ambiente Docker.

---

# Execução com Docker

O Apache Flume é executado como um serviço do ambiente containerizado da pipeline.

A inicialização do ambiente pode ser realizada através do Docker Compose:

```bash
docker compose -f docker/docker-compose.yml up -d
```

Para verificar o estado dos containers:

```bash
docker compose -f docker/docker-compose.yml ps
```

Para acompanhar os logs do Flume:

```bash
docker logs -f ecommerce-flume
```

Os logs são importantes para verificar a inicialização do agente e identificar problemas durante a ingestão.

---

# Verificação do Serviço

O estado do container pode ser verificado com:

```bash
docker ps
```

Para obter informações específicas do serviço:

```bash
docker inspect ecommerce-flume
```

Os logs podem ser utilizados para verificar:

- inicialização do agente;
- carregamento da configuração;
- inicialização do Source;
- inicialização do Channel;
- inicialização do Sink;
- recebimento de eventos;
- erros de transporte;
- falhas de configuração.

---

# Observabilidade

A observabilidade da camada de ingestão é baseada principalmente nos logs e no acompanhamento do fluxo entre Source, Channel e Sink.

O monitoramento deve permitir identificar em qual etapa ocorreu uma eventual interrupção:

```text
Data Generator
      │
      ▼
   Source
      │
      ▼
   Channel
      │
      ▼
    Sink
      │
      ▼
   Destino
```

Uma falha no Source indica um problema relacionado à entrada.

Uma falha no Channel pode impedir o armazenamento temporário dos eventos.

Uma falha no Sink pode impedir que os eventos avancem para o próximo componente.

Essa separação facilita o diagnóstico da pipeline.

---

# Diagnóstico de Problemas

Quando os eventos não chegam às próximas etapas, a investigação deve seguir o fluxo da arquitetura.

### 1. Verificar o gerador

```bash
docker logs --tail 100 ecommerce-generator
```

### 2. Verificar o Flume

```bash
docker logs --tail 100 ecommerce-flume
```

### 3. Verificar os serviços

```bash
docker compose -f docker/docker-compose.yml ps
```

### 4. Verificar conectividade entre containers

```bash
docker exec ecommerce-flume getent hosts ecommerce-generator
```

O diagnóstico deve começar pela origem e seguir até o destino, permitindo identificar em qual componente os eventos deixam de avançar.

---

# Confiabilidade da Ingestão

O Apache Flume fornece uma arquitetura intermediária que ajuda a controlar o transporte dos eventos.

A utilização de um Channel entre Source e Sink evita que ambos precisem operar diretamente conectados.

```text
Producer
   │
   ▼
Source
   │
   ▼
Channel
   │
   ▼
Sink
```

Essa arquitetura é particularmente adequada para pipelines nas quais a produção e o consumo dos eventos possuem ritmos diferentes.

---

# Escalabilidade

A separação entre Source, Channel e Sink também fornece uma base para evolução da arquitetura.

O fluxo pode posteriormente ser ampliado para diferentes fontes e destinos:

```text
                 ┌──► Source A ──┐
                 │               │
Data Sources ────┼──► Source B ──┼──► Channel ──► Sink
                 │               │
                 └──► Source C ──┘
```

Da mesma forma, diferentes tipos de Sink podem ser utilizados dependendo da necessidade de persistência ou processamento.

Na implementação atual, entretanto, devem ser considerados apenas os componentes efetivamente configurados no projeto.

---

# Segurança e Isolamento

A execução através de containers fornece isolamento entre o serviço de ingestão e os demais componentes da infraestrutura.

O Flume opera dentro da rede Docker definida pelo projeto, permitindo que os serviços se comuniquem através dos nomes dos containers e das portas configuradas.

Isso evita a necessidade de expor internamente todos os componentes diretamente ao host.

---

# Papel na Pipeline Completa

A camada `ingestion` ocupa uma posição intermediária essencial:

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
│                     │
│    Apache Flume     │
│                     │
│ Source              │
│ Channel             │
│ Sink                │
└──────────┬──────────┘
           │
           ├──────────────────┐
           │                  │
           ▼                  ▼
┌─────────────────┐   ┌─────────────────┐
│    STREAMING    │   │     STORAGE     │
│     Flink       │   │ HDFS/HBase/Hive │
└────────┬────────┘   └────────┬────────┘
         │                     │
         │                     ▼
         │              ┌──────────────┐
         │              │     BATCH    │
         │              │     Spark    │
         │              └──────────────┘
         │
         ▼
    Monitoring
```

O `ingestion` não substitui o processamento. Seu objetivo é garantir que os dados produzidos pela origem sejam **recebidos, transportados e encaminhados de maneira estruturada**.

---

# Tecnologias

| Tecnologia | Função |
|---|---|
| Apache Flume | Ingestão e transporte dos eventos |
| JSON | Formato dos eventos |
| Docker | Isolamento e execução do serviço |
| Docker Compose | Orquestração do ambiente |
| Python | Origem dos eventos através do `data-generator` |
| Apache Flink | Processamento posterior em streaming |
| HDFS | Persistência distribuída |
| HBase | Persistência orientada a colunas |
| Hive | Consulta e organização dos dados |
| Apache Spark | Processamento batch |

---

# Dependências

A camada de ingestão depende dos seguintes elementos da arquitetura:

```text
Data Generator
      │
      ▼
   Apache Flume
      │
      ▼
Próximas camadas
```

O funcionamento correto depende principalmente de:

- configuração válida do agente;
- Source corretamente configurado;
- Channel disponível;
- Sink corretamente configurado;
- conectividade entre os serviços;
- formato compatível dos eventos;
- serviços downstream disponíveis quando necessários.

---

# Resumo

O módulo `ingestion` implementa a camada de **entrada e transporte de dados da pipeline**, utilizando Apache Flume como mecanismo de ingestão.

Sua arquitetura baseada em **Source → Channel → Sink** permite separar a produção dos eventos do processamento e armazenamento, fornecendo uma camada intermediária para transporte dos dados.

Dentro da arquitetura completa, o módulo recebe os eventos JSON produzidos pelo `data-generator` e os encaminha para as etapas responsáveis pelo processamento em tempo real, persistência e processamento histórico.

```text
Data Generator
      │
      ▼
Apache Flume
      │
      ▼
Streaming / Storage
      │
      ├── Flink
      ├── HDFS
      ├── HBase
      └── Hive
              │
              ▼
            Spark
```

Essa organização permite demonstrar uma arquitetura de Big Data composta por **geração contínua, ingestão, processamento streaming, armazenamento distribuído e processamento batch**, mantendo cada responsabilidade isolada em seu respectivo componente.