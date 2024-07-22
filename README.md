# Desafio_ETL

Bem-vindo ao repositório do Desafio_ETL! 🚀

Este projeto faz parte do Desafio de Engenharia de Dados para extração, transformação e carregamento de dados de proposições legislativas do estado de Minas Gerais. Utilizamos uma combinação de ferramentas, incluindo Dagster, PostgreSQL e pgAdmin, todas containerizadas para garantir um ambiente de desenvolvimento robusto e portável.

## Visão Geral

O objetivo deste projeto é criar um pipeline de dados para extrair informações sobre proposições legislativas de 2023, realizar a limpeza dos dados e carregá-los em um banco de dados relacional. A arquitetura utiliza Docker Compose para orquestração dos contêineres.

## Ferramentas Utilizadas

- **Dagster**: Orquestração de fluxos de dados.
- **PostgreSQL**: Armazenamento de dados estruturados.
- **pgAdmin**: Interface de gerenciamento do PostgreSQL.
- **Docker**: Containerização da aplicação e dos serviços.

## Estrutura do Diretório

- **desafio_etl**: Módulo principal com os arquivos responsáveis pelo pipeline de dados.
- **notebooks**: Histórico de exploração dos dados.
- **requirements.txt**: Arquivo de dependências do pip.
- **docker-compose.yml**: Arquivo de configuração do Docker Compose.
- **README.md**: Este arquivo.

## Configuração do Ambiente

### Pré-requisitos

Certifique-se de ter o Docker e o Docker Compose instalados em sua máquina.

### Passos para Configuração

1. **Clonar o Repositório**: Clone este repositório para o seu ambiente local.
   ```bash
   git clone https://github.com/seu_usuario/desafio_etl.git
   cd desafio_etl
   ```

2. **Configurar Variáveis de Ambiente**: Renomeie o arquivo `.env.example` para `.env` e configure as variáveis de ambiente necessárias, como informações do banco de dados PostgreSQL.

3. **Iniciar os Contêineres**: Navegue até o diretório principal do projeto e execute o seguinte comando para iniciar os contêineres:
   ```bash
   docker-compose up --build
   ```

4. **Configuração Adicional do Ambiente de Desenvolvimento**:
   - Instale as dependências Python:
     ```bash
     pip install -r requirements.txt
     ```
   - Execute o Dagster em modo de desenvolvimento:
     ```bash
     dagster dev -m desafio_etl
     ```

## Acesso aos Serviços

- **Dagster**: Acesse a interface do Dagster em `http://localhost:3000`.
- **pgAdmin**: Acesse o pgAdmin em `http://localhost:2000`.

Se você tiver dúvidas ou precisar de ajuda, fique à vontade para criar uma Issue neste repositório. 🚀