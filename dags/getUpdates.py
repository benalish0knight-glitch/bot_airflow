from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import os
from dotenv import load_dotenv
import json

# Carregar variáveis do .env
load_dotenv()

# Configurações do Telegram
TELEGRAM_TOKEN = os.getenv('TOKEN')


def verificar_updates():
    """
    Verifica atualizações/mensagens recebidas pelo bot do Telegram
    """
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getUpdates"
    
    try:
        response = requests.get(url)
        response.raise_for_status()
        
        data = response.json()
        
        print("=" * 50)
        print("📩 VERIFICANDO UPDATES DO TELEGRAM")
        print("=" * 50)
        
        if data['ok']:
            updates = data.get('result', [])
            print(f"\n✅ Total de updates: {len(updates)}\n")
            
            if updates:
                for update in updates:
                    print("-" * 50)
                    print(f"Update ID: {update.get('update_id')}")
                    
                    if 'message' in update:
                        msg = update['message']
                        print(f"Chat ID: {msg['chat']['id']}")
                        print(f"De: {msg['from'].get('first_name', 'N/A')}")
                        print(f"Username: @{msg['from'].get('username', 'N/A')}")
                        print(f"Mensagem: {msg.get('text', 'N/A')}")
                        print(f"Data: {datetime.fromtimestamp(msg['date'])}")
                    print("-" * 50)
            else:
                print("ℹ️ Nenhum update novo encontrado")
        
        # Retorna o JSON completo para inspeção
        print("\n📄 Resposta completa (JSON):")
        print(json.dumps(data, indent=2, ensure_ascii=False))
        
        return data
        
    except requests.exceptions.RequestException as e:
        print(f"❌ Erro ao verificar updates: {e}")
        raise


# Argumentos padrão da DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
}

# Definição da DAG
with DAG(
    'telegram_getupdates',
    default_args=default_args,
    description='Verifica updates/mensagens do bot Telegram',
    schedule_interval='@hourly',  # Verifica a cada hora
    catchup=False,
    tags=['telegram', 'monitoring'],
) as dag:

    verificar = PythonOperator(
        task_id='verificar_updates_telegram',
        python_callable=verificar_updates,
    )