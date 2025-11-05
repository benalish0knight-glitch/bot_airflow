# Projeto Airflow DAG: Chatbot Mensageria

Este projeto contém uma Directed Acyclic Graph (DAG) do Apache Airflow em Python para orquestrar a execução de um chatbot que coleta e processa dados, enviando-os posteriormente para um grupo de mensageria (como Telegram, Slack, ou outro serviço).

## Estrutura do Projeto

O projeto segue a estrutura recomendada para projetos Airflow, adaptada para Docker:

```
airflow_chatbot_dag/
├── dags/
│   └── chatbot_data_pipeline.py  # A definição da DAG
├── modules/
│   └── messaging_service.py     # Módulo Python para interação com o serviço de mensageria
├── scripts/
│   └── chatbot_script.py        # Script Python principal do chatbot
├── .env                         # Variáveis de ambiente para o Docker Compose
├── docker-compose.yml           # Definição dos serviços Airflow (Webserver, Scheduler, DB)
├── requirements.txt             # Dependências Python
└── README.md                    # Este arquivo
```

## Requisitos

Para rodar este projeto, você precisará de:
*   **Docker** e **Docker Compose** instalados.

## Configuração do Ambiente Docker

1.  **Variável de Ambiente (AIRFLOW_UID):**
    *   Edite o arquivo `.env` e defina o `AIRFLOW_UID`. Se você estiver no Linux, pode usar `$(id -u)` para obter seu UID atual. Se estiver em Mac/Windows, o valor padrão `50000` geralmente funciona para o container Airflow.

2.  **Inicialização:**
    *   Navegue até o diretório `airflow_chatbot_dag/` e execute o comando:
        ```bash
        docker compose up -d
        ```
    *   Este comando irá:
        *   Baixar as imagens do PostgreSQL e Airflow.
        *   Inicializar o banco de dados.
        *   Rodar a migração do banco de dados e criar o usuário `admin` (`admin` / `admin`).
        *   Iniciar o Webserver e o Scheduler do Airflow.

3.  **Acesso ao Airflow:**
    *   O Webserver estará acessível em `http://localhost:8080`.
    *   Faça login com `admin` / `admin`.

## Configurações no Airflow (Webserver)

Antes de rodar a DAG, você precisa configurar duas coisas no Webserver do Airflow:

1.  **Conexão de Mensageria:**
    *   Vá em **Admin** -> **Connections** -> **+** (Criar).
    *   **ID da Conexão:** `messaging_conn_id`
    *   **Tipo da Conexão:** `Generic` (ou o tipo específico, se usar um Hook do Airflow, ex: `Slack`).
    *   **Host:** O URL base da API/Webhook do seu serviço de mensageria (ex: `https://api.telegram.org/bot<TOKEN>`).
    *   **Password:** O token de autenticação ou chave secreta (se aplicável).
    *   *Nota: Adapte os campos `Host` e `Password` conforme a necessidade do seu `modules/messaging_service.py`.*

2.  **Variável do Grupo de Mensageria:**
    *   Vá em **Admin** -> **Variables** -> **+** (Criar).
    *   **Chave:** `messaging_group_id`
    *   **Valor:** O ID do chat/canal para onde a mensagem será enviada (ex: `@meu_canal` ou um ID numérico).

## A DAG

A DAG `chatbot_data_pipeline` é definida em `dags/chatbot_data_pipeline.py` e possui a seguinte estrutura de tarefas:

1.  **`extract_and_process_data_task`**: Executa a função `extract_and_process_data` do `scripts/chatbot_script.py`.
2.  **`send_message_to_group_task`**: Executa a função `send_message_to_group` do `modules/messaging_service.py`, enviando o resultado da tarefa anterior.

## Próximos Passos (Implementação)

O código base é fornecido com placeholders. Você deve customizar os seguintes arquivos para a sua necessidade:

*   **`scripts/chatbot_script.py`**: Implementar a lógica real de extração e processamento dos dados do seu chatbot.
*   **`modules/messaging_service.py`**: Adaptar a lógica de envio de mensagem (`requests.post`, payload, headers) para o seu serviço de mensageria específico (Telegram, Slack, etc.).

---
*Este projeto foi gerado automaticamente como um ponto de partida para a orquestração do seu chatbot com Airflow via Docker.*
# bot_airflow
