# Data Generator

## Visão Geral

O módulo `data_generator` é responsável pela **geração contínua de eventos sintéticos em formato JSON**, simulando o comportamento de usuários e operações de um e-commerce em tempo real.

O componente atua como a **fonte de dados da pipeline**, produzindo eventos que posteriormente são encaminhados para a camada de ingestão, processamento em streaming e processamento batch.

A geração contínua permite reproduzir características comuns de ambientes de produção, como:

- alto volume de eventos;
- geração contínua de dados;
- diferentes tipos de eventos;
- timestamps de ocorrência;
- variação na frequência dos eventos;
- eventos relacionados ao ciclo de compra;
- possibilidade de eventos chegarem fora de ordem nas etapas posteriores da pipeline.

Os eventos gerados representam principalmente interações de usuários e etapas do processo de venda e logística:

| Evento | Descrição |
|---|---|
| `click` | Interação do usuário com um produto |
| `cart` | Inclusão de um produto no carrinho |
| `order` | Criação de um pedido |
| `delivery` | Atualização relacionada à entrega do pedido |

---

## Responsabilidade na Arquitetura

Dentro da arquitetura da pipeline, o `data_generator` ocupa a posição de **origem dos dados**.

```text
┌─────────────────────┐
│    Data Generator   │
│                     │
│ Eventos JSON        │
│ Click / Cart        │
│ Order / Delivery    │
└──────────┬──────────┘
           │
           │ Eventos
           ▼
┌─────────────────────┐
│   Apache Flume      │
│      Ingestion      │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│      Streaming      │
│       Apache Flink  │
└──────────┬──────────┘
           │
           ├──────────────► HBase
           │
           ▼
┌─────────────────────┐
│       Storage       │
│ HDFS / Hive / HBase │
└──────────┬──────────┘
           │
           ▼
┌─────────────────────┐
│       Batch         │
│       Apache Spark  │
└─────────────────────┘
```

O gerador não realiza processamento analítico dos eventos. Sua função é produzir dados de entrada de forma contínua e disponibilizá-los para a camada de ingestão.

---

## Fluxo de Geração

O fluxo básico de geração segue as seguintes etapas:

```text
Inicialização
     │
     ▼
Configuração do gerador
     │
     ▼
Seleção do tipo de evento
     │
     ▼
Geração dos dados
     │
     ▼
Construção do objeto JSON
     │
     ▼
Registro do timestamp
     │
     ▼
Serialização JSON
     │
     ▼
Saída do evento
     │
     ▼
Apache Flume
     │
     └──────► próximo evento
```

O processo permanece em execução continuamente, permitindo que a pipeline trabalhe com um fluxo constante de dados.

---

## Tipos de Eventos

### Click

Representa uma interação de um usuário com um produto.

Esse tipo de evento permite simular o comportamento de navegação de clientes dentro da plataforma de e-commerce.

Exemplo conceitual:

```json
{
  "event_type": "click",
  "user_id": "user_1024",
  "product_id": "product_583",
  "timestamp": "2026-09-27T13:20:14Z"
}
```

---

### Cart

Representa a inclusão de um produto no carrinho.

O evento pode ser utilizado para analisar a evolução do comportamento do usuário entre a visualização do produto e a intenção de compra.

Exemplo conceitual:

```json
{
  "event_type": "cart",
  "user_id": "user_1024",
  "product_id": "product_583",
  "timestamp": "2026-09-27T13:20:19Z"
}
```

---

### Order

Representa a criação de um pedido.

Esse evento é utilizado pela pipeline para representar a conversão de uma interação em uma operação de venda.

Exemplo conceitual:

```json
{
  "event_type": "order",
  "order_id": "order_78421",
  "user_id": "user_1024",
  "product_id": "product_583",
  "timestamp": "2026-09-27T13:20:31Z"
}
```

---

### Delivery

Representa uma atualização relacionada ao processo de entrega.

Esse evento permite que a pipeline trabalhe também com informações relacionadas à logística dos pedidos.

Exemplo conceitual:

```json
{
  "event_type": "delivery",
  "order_id": "order_78421",
  "status": "in_transit",
  "timestamp": "2026-09-27T13:21:05Z"
}
```

Os campos apresentados acima representam a estrutura conceitual dos eventos. A estrutura efetivamente utilizada pela pipeline deve ser considerada a fonte de verdade para o processamento downstream.

---

## Eventos e Timestamps

O timestamp é um elemento importante do gerador porque os eventos são posteriormente processados por mecanismos de streaming.

A informação temporal permite que o Apache Flink trabalhe com:

- event time;
- watermarks;
- eventos atrasados;
- eventos fora de ordem;
- janelas temporais;
- agregações baseadas em tempo.

Dessa forma, o gerador não representa apenas uma fonte de registros estáticos. Ele fornece uma sequência temporal que pode ser utilizada para demonstrar o comportamento de uma arquitetura de processamento de eventos.

---

## Geração Contínua

O gerador foi projetado para permanecer ativo durante a execução da pipeline.

Conceitualmente:

```text
while generator_is_running:

    event = generate_event()

    serialize(event)

    emit(event)

    wait(interval)
```

Esse comportamento permite controlar a **velocidade de produção dos dados** e criar um fluxo contínuo para os componentes de ingestão.

A geração contínua é especialmente importante para os testes da camada de streaming, pois permite observar o processamento dos eventos enquanto eles são produzidos.

---

## Formato dos Dados

Os eventos são serializados em **JSON**, permitindo que sejam transportados facilmente entre os componentes da arquitetura.

O JSON também facilita:

- inspeção manual dos eventos;
- testes locais;
- depuração;
- integração com Apache Flume;
- parsing no Apache Flink;
- persistência em camadas de armazenamento;
- processamento posterior pelo Apache Spark.

Um evento é tratado como uma unidade independente de informação dentro do fluxo.

---

## Integração com Apache Flume

Após serem gerados, os eventos são encaminhados para a camada de ingestão da pipeline.

O Apache Flume atua como mecanismo de coleta e transporte entre a origem e os componentes seguintes.

O fluxo pode ser representado como:

```text
Data Generator
      │
      │ JSON
      ▼
Apache Flume Source
      │
      ▼
Flume Channel
      │
      ▼
Flume Sink
      │
      ▼
Pipeline de processamento
```

Essa separação permite que o gerador permaneça desacoplado da lógica de ingestão.

O `data_generator` não precisa conhecer detalhes internos do Flume, do Flink, do Spark ou das tecnologias de armazenamento.

---

## Integração com Streaming

Os eventos produzidos pelo gerador alimentam a camada de processamento em tempo real.

O Apache Flink pode utilizar os timestamps presentes nos eventos para determinar o tempo de evento e realizar operações como:

```text
Eventos
   │
   ▼
Parsing
   │
   ▼
Extração do timestamp
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
Alertas / Persistência
```

Essa integração permite utilizar os dados sintéticos para validar o comportamento do pipeline diante de eventos contínuos.

---

## Integração com Batch

Embora o gerador tenha como principal objetivo alimentar o processamento em tempo real, os eventos também podem ser persistidos nas camadas de armazenamento e posteriormente utilizados pelo processamento batch.

Nesse cenário:

```text
Data Generator
      │
      ▼
Ingestion
      │
      ▼
Storage
      │
      ├──► HDFS
      │
      ├──► HBase
      │
      └──► Hive
             │
             ▼
          Spark
             │
             ▼
        ETL / SQL / RDD
```

Isso permite utilizar a mesma origem de eventos para demonstrar tanto processamento **streaming** quanto processamento **batch**.

---

## Controle do Volume de Dados

A geração contínua permite produzir diferentes volumes de eventos durante os testes da pipeline.

O comportamento pode ser utilizado para validar:

- ingestão contínua;
- capacidade de processamento;
- crescimento dos arquivos;
- persistência no HDFS;
- gravação no HBase;
- consultas no Hive;
- execução de jobs Spark;
- comportamento das janelas do Flink;
- métricas de throughput.

Durante uma execução completa, o número de registros produzidos pode ser acompanhado pelas ferramentas de monitoramento da pipeline.

---

## Testes e Validação

O `data_generator` também funciona como uma ferramenta de teste para os demais componentes.

A validação pode ser realizada em diferentes níveis.

### Validação da geração

Verificar se os eventos estão sendo produzidos:

```bash
docker logs -f ecommerce-generator
```

### Validação do fluxo

Verificar se os eventos estão chegando à camada de ingestão:

```bash
docker logs -f ecommerce-flume
```

### Validação do processamento

Acompanhar a aplicação no Apache Flink e verificar:

- eventos recebidos;
- operadores em execução;
- processamento das janelas;
- watermarks;
- checkpoints;
- métricas dos operadores.

### Validação do armazenamento

Os dados processados podem posteriormente ser verificados nas respectivas camadas:

```text
HDFS
HBase
Hive
```

---

## Execução com Docker

O gerador é executado como parte do ambiente containerizado da pipeline.

A inicialização completa do ambiente pode ser realizada através do Docker Compose utilizado pelo projeto.

Após a inicialização, o status dos serviços pode ser verificado com:

```bash
docker compose -f docker/docker-compose.yml ps
```

Para acompanhar os logs do gerador:

```bash
docker logs -f ecommerce-generator
```

Para interromper a visualização dos logs sem necessariamente parar o container:

```text
Ctrl + C
```

---

## Dependências

O componente depende principalmente do ambiente Python utilizado pelo projeto e das configurações definidas para execução da pipeline.

As dependências devem permanecer centralizadas nos arquivos de configuração do projeto, evitando que versões sejam definidas de forma independente dentro deste módulo.

A execução containerizada garante um ambiente reprodutível para a geração dos eventos.

---

## Observabilidade

O gerador pode ser acompanhado através dos logs do container.

Os logs permitem verificar:

- inicialização do processo;
- geração dos eventos;
- erros de serialização;
- interrupções;
- falhas durante a execução;
- continuidade do fluxo.

A observabilidade do gerador é complementada pelo monitoramento das camadas seguintes da pipeline.

Isso permite diferenciar problemas na **origem dos dados** de problemas ocorridos durante ingestão, processamento ou armazenamento.

---

## Tratamento de Falhas

Como o gerador representa a origem da pipeline, falhas nessa camada podem interromper o fluxo de eventos para todos os componentes downstream.

Durante a execução, devem ser observados principalmente:

```text
Generator
    │
    ├── Geração
    ├── Serialização
    ├── Emissão
    └── Continuidade
          │
          ▼
       Flume
```

Caso o gerador seja interrompido, os componentes posteriores podem permanecer em execução, porém não receberão novos eventos provenientes dessa fonte.

A separação entre os componentes facilita a identificação do ponto de falha.

---

## Papel no Projeto

O `data_generator` possui uma função fundamental na arquitetura: **simular a origem operacional dos dados**.

Ele permite que o projeto reproduza, em ambiente controlado, características encontradas em uma plataforma de e-commerce:

- eventos gerados continuamente;
- múltiplos tipos de eventos;
- informação temporal;
- fluxo de vendas;
- comportamento de usuários;
- atualizações logísticas;
- crescimento contínuo do volume de dados.

A partir desses eventos, as demais camadas da arquitetura conseguem demonstrar diferentes paradigmas de processamento e armazenamento.

---

## Tecnologias

| Tecnologia | Utilização |
|---|---|
| Python 3 | Implementação do gerador |
| JSON | Formato dos eventos |
| Docker | Execução isolada do componente |
| Apache Flume | Ingestão dos eventos gerados |
| Apache Flink | Processamento em tempo real |
| Apache Spark | Processamento batch |
| HDFS | Armazenamento distribuído |
| HBase | Armazenamento orientado a colunas |
| Hive | Consulta e organização dos dados |

---

# Resumo 

O módulo `data_generator` permanece desacoplado das etapas de processamento. Sua responsabilidade termina na produção dos eventos, enquanto os módulos seguintes são responsáveis pela ingestão, processamento, persistência, análise e observabilidade.

