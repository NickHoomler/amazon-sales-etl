import os
import json
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
import gspread
from io import StringIO
import time

def load_gdrive_files(folder_id, creds_json):
    """Загружаем файлы из Google Drive с обработкой ошибок"""
    try:
        creds_dict = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        client = gspread.authorize(creds)
        
        print(f"🔍 Поиск файлов в папке ID: {folder_id}")
        files = client.list_spreadsheet_files(folder_id=folder_id)
        
        if not files:
            print("⚠️ В папке не найдено файлов")
            return None

        dfs = []
        for file in files:
            if file['mimeType'] == 'text/csv':
                print(f"📥 Загрузка файла: {file['name']} (ID: {file['id']})")
                content = client.export(file['id'], 'text/csv').decode('windows-1251')
                dfs.append(pd.read_csv(StringIO(content), sep=';', encoding='windows-1251'))
        
        return pd.concat(dfs) if dfs else None

    except Exception as e:
        print(f"❌ Ошибка при работе с Google Drive: {str(e)}")
        return None

def upload_to_mysql(df, db_config):
    """Загрузка данных в MySQL с обработкой ошибок"""
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}/{db_config['database']}"
        )
        df.to_sql(db_config['table'], engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"❌ Ошибка при загрузке в MySQL: {str(e)}")
        return False

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        print("🔄 Начало синхронизации...")
        
        # 1. Загрузка данных
        df = load_gdrive_files(
            os.getenv('GDRIVE_FOLDER_ID'),
            os.getenv('GDRIVE_CREDS')
        )
        
        if df is None:
            raise Exception("Не удалось загрузить данные из Google Drive")
        
        print(f"📊 Загружено {len(df)} строк")
        
        # 2. Загрузка в MySQL
        db_config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'database': os.getenv('DB_NAME'),
            'table': "ComissionOtto"
        }
        
        if upload_to_mysql(df, db_config):
            print("✅ Данные успешно сохранены в MySQL")
        
    except Exception as e:
        print(f"🔥 Критическая ошибка: {str(e)}")
    finally:
        print(f"⏱ Общее время выполнения: {time.time() - start_time:.2f} секунд")
