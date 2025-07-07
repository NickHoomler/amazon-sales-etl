import os
import json
import pandas as pd
from sqlalchemy import create_engine
from google.oauth2 import service_account
import gspread
from io import StringIO
import time

def load_gdrive_files(folder_id, creds_json):
    """Загружаем файлы из Google Drive с улучшенной обработкой ошибок"""
    try:
        # Проверка входных данных
        if not folder_id or not creds_json:
            raise ValueError("Не указан folder_id или creds_json")
            
        print(f"🔍 Попытка подключения к Google Drive...")
        
        # Загрузка учетных данных
        try:
            creds_dict = json.loads(creds_json)
            creds = service_account.Credentials.from_service_account_info(
                creds_dict,
                scopes=['https://www.googleapis.com/auth/drive']
            )
            client = gspread.authorize(creds)
        except Exception as auth_error:
            raise Exception(f"Ошибка авторизации: {str(auth_error)}")

        print(f"🔍 Поиск файлов в папке ID: {folder_id}")
        
        try:
            # Получаем список файлов с обработкой ошибок
            files = client.list_spreadsheet_files(folder_id=folder_id)
            if not files:
                print(f"ℹ️ Папка {folder_id} пуста или не найдена")
                return None

            dfs = []
            for file in files:
                try:
                    if file['mimeType'] == 'text/csv':
                        print(f"📥 Найден CSV файл: {file['name']} (ID: {file['id']})")
                        content = client.export(file['id'], 'text/csv').decode('windows-1251')
                        dfs.append(pd.read_csv(StringIO(content), sep=';', encoding='windows-1251'))
                except Exception as file_error:
                    print(f"⚠️ Ошибка обработки файла {file.get('name')}: {str(file_error)}")
                    continue
            
            return pd.concat(dfs) if dfs else None

        except Exception as drive_error:
            raise Exception(f"Ошибка Google Drive API: {str(drive_error)}")

    except Exception as e:
        print(f"❌ Критическая ошибка в load_gdrive_files: {str(e)}")
        return None

# ... (остальные функции остаются без изменений)

if __name__ == "__main__":
    start_time = time.time()
    
    try:
        print("🔄 Начало синхронизации...")
        
        # Получаем переменные окружения
        folder_id = os.getenv('GDRIVE_FOLDER_ID')
        creds_json = os.getenv('GDRIVE_CREDS')
        
        print(f"ℹ️ Проверка переменных окружения...")
        print(f"Folder ID: {'установлен' if folder_id else 'не установлен'}")
        print(f"Credentials: {'установлены' if creds_json else 'не установлены'}")
        
        # 1. Загрузка данных
        df = load_gdrive_files(folder_id, creds_json)
        
        if df is None:
            raise Exception("Не удалось загрузить данные из Google Drive")
        
        print(f"📊 Успешно загружено {len(df)} строк")
        
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
