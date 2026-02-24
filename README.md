\# 🚀 Data Engineering Pipeline: Master Data Management \& Entity Resolution



\## 📋 Sobre o Projeto

Este projeto consiste no desenvolvimento de um pipeline robusto de Engenharia de Dados para a consolidação de "Visão Única do Estabelecimento" (Master Data Management - MDM). O desafio abrange a integração de bases de dados heterogêneas, integrações via API e a resolução de entidades (Fuzzy Matching) em cenários de ausência de chaves primárias fortes (como o CNPJ).



\## 🎯 Arquitetura e Desafios Resolvidos



O pipeline é dividido em duas frentes principais de processamento:



\### 1. MDM \& Data Consolidation (Bases Governamentais x API Interna)

Cruzamento da base nacional da Receita Federal (+1.5 milhão de registros) com a base do Cadastur e dados dinâmicos do sistema SIGA (Abrasel).

\* \*\*Integração de API Rest:\*\* Extração automatizada de dados em formato JSON com injeção de headers de autenticação.

\* \*\*Otimização de Memória (Big Data):\*\* Utilização de processamento em lotes (\*Chunking\* de 100k linhas) no Pandas para evitar `MemoryError` ao ler bases massivas localmente.

\* \*\*Data Cleansing:\*\* Expressões Regulares (RegEx) avançadas para padronização de chaves primárias (CNPJ), garantindo a preservação de zeros à esquerda e prevenindo distorções em notação científica.



\### 2. Entity Resolution \& Fuzzy Matching (Ticket x Receita Federal)

O maior desafio técnico do projeto: cruzar milhares de arquivos descentralizados (raspagem de dados web) com a base oficial da Receita, \*\*sem a existência da chave CNPJ\*\* na base de origem.

\* \*\*Inteligência Léxica:\*\* Implementação da biblioteca `thefuzz` (Levenshtein Distance) para calcular a similaridade entre strings (Nome Fantasia / Razão Social).

\* \*\*Bloqueio Geográfico (Blocking):\*\* Algoritmo otimizado para recortar a base nacional iterativamente por cidade/estado do arquivo em processamento, reduzindo o tempo de complexidade computacional de semanas para minutos.

\* \*\*Dupla Validação (Trava de Segurança):\*\* Implementação de regras de negócios rigorosas para evitar Falsos Positivos. Aprovação baseada em similaridade forte (>= 85%) ou similaridade média (>= 60%) condicionada ao \*match\* exato do número do endereço extraído via RegEx.

\* \*\*Processamento em Lote (Batch):\*\* Script autônomo ("Trator") capaz de varrer mais de 5.400 arquivos locais, aplicar a inferência léxica e consolidar o resultado em um \*Dataframe Master\*.



\## 🛠️ Tecnologias Utilizadas

\* \*\*Linguagem:\*\* Python 3.11+

\* \*\*Manipulação de Dados:\*\* Pandas, Numpy

\* \*\*Integração Web:\*\* Requests, urllib3

\* \*\*Entity Resolution:\*\* thefuzz (`thefuzz\[speedup]`), python-Levenshtein

\* \*\*Processamento:\*\* Chunking, Regex (re)

