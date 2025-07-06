import os
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
import gspread
from io import StringIO
import json

def load_gdrive_files(folder_id, creds_json):
    """Загружаем CSV из Google Drive"""
    creds = service_account.Credentials.from_service_account_info(
        json.loads(creds_json),
        scopes=['https://www.googleapis.com/auth/drive']
    )
    client = gspread.authorize(creds)
    
    dfs = []
    for file in client.list_spreadsheet_files(folder_id=folder_id):
        if file['mimeType'] == 'text/csv':
            content = client.export(file['id'], 'text/csv').decode('windows-1251')
            dfs.append(pd.read_csv(StringIO(content), sep=';', encoding='windows-1251'))
    
    return pd.concat(dfs) if dfs else None

def upload_to_mysql(df, db_config):
    """Загрузка данных в MySQL"""
    engine = create_engine(
        f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
        f"@{db_config['host']}/{db_config['database']}"
    )
    df.to_sql(db_config['table'], engine, if_exists='replace', index=False)

if __name__ == "__main__":
    # Конфигурация через переменные окружения
    gdrive_creds = os.getenv('GDRIVE_CREDS')
    folder_id = os.getenv('GDRIVE_FOLDER_ID')
    
    db_config = {
        'user': os.getenv('DB_USER'),
        'password': os.getenv('DB_PASSWORD'),
        'host': os.getenv('DB_HOST'),
        'database': os.getenv('DB_NAME'),
        'table': "ComissionOtto"
    }
    
    print("🔄 Загрузка данных из Google Drive...")
    df = load_gdrive_files(folder_id, gdrive_creds)
    
    if df is not None:
        print(f"📊 Загружено {len(df)} строк")
        upload_to_mysql(df, db_config)
        print("✅ Данные успешно сохранены в MySQL")
    else:
        print("⚠️ Файлы не найдены или не содержат данных")
