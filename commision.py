import os
import json
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from io import BytesIO, StringIO
import time

def load_gdrive_files(folder_id, creds_json):
    """Новая реализация с использованием googleapiclient"""
    try:
        # Инициализация клиента
        creds_dict = json.loads(creds_json)
        creds = service_account.Credentials.from_service_account_info(
            creds_dict,
            scopes=['https://www.googleapis.com/auth/drive']
        )
        
        service = build('drive', 'v3', credentials=creds)
        
        # Поиск CSV файлов в указанной папке
        query = f"'{folder_id}' in parents and mimeType='text/csv'"
        results = service.files().list(
            q=query,
            fields="files(id, name)"
        ).execute()
        
        files = results.get('files', [])
        
        if not files:
            print("ℹ️ В папке не найдено CSV файлов")
            return None

        dfs = []
        for file in files:
            print(f"📥 Загружаем файл: {file['name']} ({file['id']})")
            
            # Скачивание файла
            request = service.files().get_media(fileId=file['id'])
            fh = BytesIO()
            downloader = MediaIoBaseDownload(fh, request)
            
            done = False
            while not done:
                _, done = downloader.next_chunk()
            
            # Чтение CSV
            content = fh.getvalue().decode('windows-1251')
            dfs.append(pd.read_csv(StringIO(content), sep=';', encoding='windows-1251'))
        
        return pd.concat(dfs) if dfs else None

    except Exception as e:
        print(f"❌ Ошибка Google Drive API: {type(e).__name__}: {str(e)}")
        return None

def upload_to_mysql(df, db_config):
    """Загрузка в MySQL (без изменений)"""
    try:
        engine = create_engine(
            f"mysql+pymysql://{db_config['user']}:{db_config['password']}"
            f"@{db_config['host']}/{db_config['database']}"
        )
        df.to_sql(db_config['table'], engine, if_exists='replace', index=False)
        return True
    except Exception as e:
        print(f"❌ Ошибка MySQL: {str(e)}")
        return False

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        print("🔄 Начало синхронизации...")
        
        # Загрузка данных
        df = load_gdrive_files(
            os.getenv('GDRIVE_FOLDER_ID'),
            os.getenv('GDRIVE_CREDS')
        )
        
        if df is None:
            raise Exception("Нет данных для загрузки")
        
        print(f"📊 Получено {len(df)} строк")
        
        # Загрузка в БД
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
        print(f"🔥 Ошибка: {str(e)}")
    finally:
        print(f"⏱ Время выполнения: {time.time() - start_time:.2f} сек")
