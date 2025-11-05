import logging
import requests
from airflow.hooks.base import BaseHook

logging.basicConfig(level=logging.INFO)

def send_message_to_group(message_content: str, conn_id: str, group_id: str):
    """
    Envia o conteúdo da mensagem para o grupo de mensageria configurado.
    
    ESTE É UM PLACEHOLDER.
    Você deve adaptar esta função para o seu serviço de mensageria específico
    (ex: Telegram Bot API, Slack Webhook, Discord Webhook, etc.).
    
    Args:
        message_content: O texto formatado a ser enviado.
        conn_id: O ID da conexão do Airflow que contém o endpoint/token.
        group_id: O ID do grupo/canal de destino.
    """
    logging.info(f"Tentando enviar mensagem para o grupo {group_id} usando a conexão {conn_id}...")
    
    try:
        # 1. Obter a conexão configurada no Airflow
        hook = BaseHook(conn_id=conn_id)
        connection = hook.get_connection(conn_id)
        
        # O host da conexão pode ser o URL base do webhook ou API
        api_url = connection.host
        
        # O password da conexão pode ser o token ou chave secreta
        auth_token = connection.password 
        
        # --- Lógica de Envio (Exemplo com um Webhook genérico) ---
        
        # Montar o payload (depende do serviço de mensageria)
        payload = {
            "chat_id": group_id, # Parâmetro específico do Telegram ou outro
            "text": message_content,
            "parse_mode": "Markdown" # Exemplo de formatação
        }
        
        # Montar o cabeçalho (se necessário, para autorização)
        headers = {
            "Content-Type": "application/json",
            "Authorization": f"Bearer {auth_token}" # Exemplo de autorização
        }
        
        # Fazer a requisição POST
        response = requests.post(f"{api_url}/send_message", json=payload, headers=headers)
        response.raise_for_status() # Levanta exceção para códigos de status HTTP ruins
        
        logging.info(f"Mensagem enviada com sucesso! Resposta da API: {response.json()}")
        
    except Exception as e:
        logging.error(f"Erro ao enviar mensagem para o grupo de mensageria: {e}")
        # Em um ambiente de produção, você pode querer re-levantar a exceção
        # para que a tarefa do Airflow falhe, ou tratar o erro de forma mais suave.
        raise

if __name__ == "__main__":
    # Este bloco não será executado pelo Airflow, mas é útil para testes locais
    print("Este módulo deve ser executado pelo Airflow.")
    # Exemplo de uso (requer configuração manual da conexão no Airflow)
    # send_message_to_group("Teste de mensagem", "messaging_conn_id", "123456789")
