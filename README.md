<p align="center">
  <strong>Pipeline de Monitoramento Varejista</strong>
</p>

<p align="center">
  Pipeline que combina processamento em tempo real (streaming) e em lote (batch) para monitorar cliques, carrinho e status de entrega.
</p><br>

<p align="center">
  <img src="./docs/imagens/fig/Python.png" width="70" alt="python"/>
  <img src="./docs/imagens/fig/flume-logo.png" width="70" alt="flume"/>
  <img src="./docs/imagens/fig/flink_squirrel_1000.png" width="70" alt="flink">
  <img src="./docs/imagens/fig/apacheSparkLogoBranco.png" width="70" alt="spark"/>
  <img src="./docs/imagens/fig/hfs.png" width="70" alt="hdfs"/>
  <img src="./docs/imagens/fig/Apache_Hive_logo.svg" width="70" alt="hive"/>
  <img src="./docs/imagens/fig/h.png" width="70" alt="hbase"/>
</p>

<p align="center">
  <img src="https://img.shields.io/badge/Big%20Data-Data%20Engineering-purple.svg" alt="Data Engineering"/>
  <img src="https://img.shields.io/badge/Apache-Flink-blue.svg" alt="Apache Flink"/>
  <img src="https://img.shields.io/badge/Apache-Spark-orange.svg" alt="Apache Spark"/>
  <img src="https://img.shields.io/badge/Apache-Flume-red.svg" alt="Apache Flume"/>
  <img src="https://img.shields.io/badge/Hadoop-HDFS-green.svg" alt="Hadoop HDFS"/>
  <img src="https://img.shields.io/badge/HBase-NoSQL-yellow.svg" alt="Apache HBase"/>
  <img src="https://img.shields.io/badge/Hive-Data%20Warehouse-blue.svg" alt="Apache Hive"/>
  <img src="https://img.shields.io/badge/Python-3.x-3776AB.svg" alt="Python 3"/>
</p>

<p align="center">
  <a href="./docs/orientacion/principal.md">Equipe</a> • <a href="https://www.youtube.com/SEU_VIDEO">Video</a>
</p>

> **Documentação viva:** esta documentação encontra-se em evolução contínua e pode sofrer alterações conforme novos componentes, modelos são implementados.

---


## 📖 Visão Geral

A pipeline percore todas as etapas do ciclo de vida do dado, da geração ao armazenamento analítico. Cada camada abaixo corresponde a uma frente técnica do trabalho.

Esse laboratorio utiliza diferentes tecnologias e ferramentas nao só pelas funcionalidades, mas sim porque cada componente possui uma
responsabilidade específica no ciclo de vida do dado.

Outra observação importante é que nenhuma das tecnologias anda competindo, pois cada uma atende padrões de processamento diferentes, como por exemplo: Flink trata o fluxo contínuo de eventos em tempo real, enquanto Spark processa o histórico em batch para consolidação analítica no Hive.


## 🏗️ Principios de Arquitetura
| Princípio                                       | Descrição                                                                                                          | Implementação na Plataforma                                                                                         |
| ----------------------------------------------- | ------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------- |
| Geração de Dados                      | Geração contínua e controlada de eventos representando o comportamento de usuários, vendas e operações logísticas. | data_generator utiliza Python 3 para produzir eventos JSON de cliques, carrinhos, pedidos e entregas, controlando volume, velocidade e timestamps.                  |
| Ingestão                          | Recepção, desacoplamento e distribuição contínua dos eventos para as camadas de processamento e armazenamento.                                         | Apache Flume coleta os eventos gerados e realiza a ingestão no ecossistema Hadoop, direcionando os dados para processamento e persistência.           |
|  Streaming                     | Processamento de eventos em tempo real considerando o tempo do evento e a chegada de dados fora de ordem.                                 | Apache Flink utiliza Event Time, Watermarks e Sliding Windows para processar eventos continuamente, identificar padrões e gerar alertas em tempo real.                       |
|    Batch                | Processamento histórico dos dados para limpeza, transformação, junção e geração de informações analíticas consolidadas.                                           | Apache Spark executa jobs de ETL utilizando RDDs, Spark SQL, agregações e joins, processando os dados históricos armazenados no HDFS.                                                |
|     Armazenamento                          | Separação das necessidades de armazenamento bruto, acesso de baixa latência e análise histórica.                                          | HDFS armazena os dados brutos em grande escala, HBase mantém alertas e resultados para consultas rápidas, e Hive organiza os dados consolidados como Data Warehouse.          |

### Camadas da Arquitetura

| Docs do Serviço | Responsabilidade |
| :--- | :--- |
| [`data-generator`](./src/data_generator/README.md) | Geração contínua de eventos JSON de cliques, carrinhos, pedidos e entregas. | PostgreSQL 
| [`ingestion`](./src/ingestion/README.md) | Ingestão e validação dos eventos utilizando Apache Flume. | PostgreSQL
| [`streaming`](./src/streaming/README.md) | Processamento em tempo real com Flink, watermarks, janelas deslizantes e alertas. | PostgreSQL 
| [`batch`](./src/batch/README.md) | Processamento histórico com Spark, ETL, RDDs, Spark SQL e joins. | PostgreSQL 
| [`storage`](./src/storage/READMEPRINCIPAL.md) | Integração com as camadas de armazenamento HDFS, HBase e Hive. | PostgreSQL 
| [`domain`](./src/domain/README.md) | Modelos e estruturas que representam os eventos e entidades do domínio. | MongoDB 
| [`monitoring`](./src/monitoring/README.md) | Métricas, alertas e monitoramento da execução da pipeline. | MongoDB 
| [`orchestration`](./src/orchestration/README.md) | Coordenação, execução e gerenciamento dos jobs de streaming e processamento batch da pipeline. | MongoDB 
| [`config`](./configs/README.md) | Configurações dos serviços e componentes da infraestrutura Big Data. | MongoDB 


---


# 🧭 Arquitetura, Fluxos e Diagramas

Esta seção apresenta os principais fluxos, componentes e decisões arquiteturais implementados na plataforma até o momento.
As imagens abaixo representam diferentes estágios de desenvolvimento e teste e destinam-se a fornecer evidência visual da plataforma operando com sucesso.

Os diagramas têm como objetivo facilitar a compreensão das interações entre serviços, infraestrutura e componentes da plataforma, servindo também como referência durante o desenvolvimento e evolução da arquitetura.

> Os screenshots são intencionalmente apresentados como evidência de implementação em vez de estarem atrelados a uma categoria específica de documentação. No entanto, 
cada camada tem suas imagens e explicaçao em suas devidas configurações.

> Nota: Os padrões apresentados nesta seção representam apenas os principais conceitos arquiteturais utilizados no desenvolvimento. A documentação completa de cada domínio pode conter outros padrões e estratégias específicas. Para conhecer as demais implementações, consulte os links disponíveis nas respectivas seções e documentações das suas respectivas camadas.

---

continuamos apartir daqui: 