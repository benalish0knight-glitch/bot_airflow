from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv

# Carregar variáveis do .env
load_dotenv()

# Configurações do Telegram
TELEGRAM_TOKEN = os.getenv('TOKEN')
CHAT_ID = os.getenv('CHAT_ID')


def enviar_mensagem_telegram(mensagem):
    """
    Envia mensagem para o bot do Telegram via GET
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    params = {
        'chat_id': CHAT_ID,
        'text': mensagem
    }
    
    try:
        response = requests.get(url, params=params)
        response.raise_for_status()
        print(f"✅ Mensagem enviada com sucesso: {mensagem}")
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao enviar mensagem: {e}")
        raise


def task_notificar_inicio():
    """Task para notificar início da DAG"""
    mensagem = "🚀 DAG iniciada com sucesso!"
    enviar_mensagem_telegram(mensagem)


def task_processar_dados():
    """Task de exemplo para processamento"""
    print("Processando dados...")
    # Seu código de processamento aqui
    mensagem = "⚙️ Dados processados com sucesso!"
    enviar_mensagem_telegram(mensagem)


def task_notificar_conclusao():
    """Task para notificar conclusão da DAG"""
    mensagem = "✅ DAG concluída com sucesso!"
    enviar_mensagem_telegram(mensagem)


# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Definição da DAG
with DAG(
    'get_message_telegram_notifications',
    default_args=default_args,
    description='DAG com notificações via Telegram',
    schedule_interval='@daily',  # Ajuste conforme necessário
    catchup=False,
    tags=['telegram', 'notificação'],
) as dag:

    # Tasks
    inicio = PythonOperator(
        task_id='notificar_inicio',
        python_callable=task_notificar_inicio,
    )

    processar = PythonOperator(
        task_id='processar_dados',
        python_callable=task_processar_dados,
    )

    conclusao = PythonOperator(
        task_id='notificar_conclusao',
        python_callable=task_notificar_conclusao,
    )

    # Definir ordem de execução
    inicio >> processar >> conclusao