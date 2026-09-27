# Domain

## Visão Geral

O módulo `domain` concentra os **modelos e estruturas conceituais do domínio de negócio** utilizados pela pipeline de Big Data.

Sua responsabilidade é representar de forma consistente os eventos produzidos pelo sistema de e-commerce e as principais entidades associadas ao fluxo de vendas e logística.

```text
Evento JSON
    │
    ▼
 Domain Model
    │
    ├── Data Generator
    ├── Ingestion
    ├── Streaming
    └── Batch
```

## Responsabilidades

O módulo é responsável por:

- representar eventos do domínio;
- definir estruturas de dados;
- padronizar atributos utilizados pelos componentes;
- manter uma representação comum dos eventos;
- reduzir duplicação de modelos entre módulos;
- facilitar validação e transformação dos dados.

Os modelos devem permanecer independentes da infraestrutura específica de armazenamento ou processamento.

## Eventos

O domínio considera os principais eventos produzidos pela aplicação de e-commerce, incluindo conceitos relacionados a:

- cliques;
- carrinho;
- pedidos;
- entregas;
- alterações de status;
- timestamps dos eventos.

A estrutura definitiva dos eventos deve seguir os modelos implementados no código.

## Integração

Os modelos de domínio são utilizados por diferentes etapas:

```text
domain
  │
  ├── data_generator
  │
  ├── ingestion
  │
  ├── streaming
  │
  └── batch
```

O `domain` não executa a geração, ingestão ou processamento. Ele fornece as estruturas utilizadas por essas camadas.

## Separação de responsabilidades

A arquitetura mantém a seguinte divisão:

| Módulo | Responsabilidade |
|---|---|
| `domain` | Modelos e estruturas |
| `data_generator` | Geração dos eventos |
| `ingestion` | Entrada e transporte |
| `streaming` | Processamento em tempo real |
| `batch` | Processamento histórico |
| `storage` | Persistência |

Essa separação evita que regras de infraestrutura sejam incorporadas aos modelos de domínio.

## Validação

Os modelos devem permitir verificar a estrutura mínima esperada dos eventos antes que eles sejam utilizados pelas etapas posteriores.

A validação pode considerar:

- campos obrigatórios;
- tipos de dados;
- identificação do evento;
- timestamp;
- tipo do evento;
- informações específicas da entidade.

## Princípios

O módulo segue principalmente:

- simplicidade;
- baixo acoplamento;
- reutilização;
- representação consistente;
- independência de infraestrutura.

## Papel na Pipeline

O `domain` funciona como uma camada comum de representação dos dados.

```text
             DOMAIN
                │
       ┌────────┼────────┐
       ▼        ▼        ▼
   Streaming   Batch   Storage
```

Dessa forma, diferentes componentes podem trabalhar sobre uma estrutura conceitual consistente sem depender diretamente uns dos outros.

## Resumo

O módulo `domain` centraliza a representação dos eventos e entidades do e-commerce, fornecendo uma base comum para geração, ingestão, processamento e persistência dos dados.