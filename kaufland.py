import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

# Получаем URL из переменных окружения
FEE_URL = os.environ.get('FEE_URL')
ALL_FEE_URL = os.environ.get('ALL_FEE_URL')

# Create a function to read csv-link
def ReadAndWrite(url, NameOfTable):
    try:
        df = pd.read_csv(url)
        print(df.head())
        print(f"Размер данных: {df.shape}")
        
    # If something went wrong return empty DataFrame and show a mistake
    except Exception as e:
        print(f"✗ Ошибка загрузки: {e}")
        return
    
    # Checking that DataFrame is correct then write date to database    
    if df is not None:
        
        
        DB_CONFIG = {
            'user': os.environ['DB_USER'],
            'password': os.environ['DB_PASSWORD'],
            'host': os.environ['DB_HOST'],
            'database': os.environ['DB_NAME'],
            'table': NameOfTable
        }

        try:
            engine = create_engine(
                f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
                f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
            )
            
            with engine.connect() as conn:
                print("✅ Подключение к MySQL успешно!")
            
            df.to_sql(
                name=DB_CONFIG['table'],
                con=engine,
                if_exists='replace',
                index=False,
                chunksize=1000,
                method='multi'
            )
            print(f"✅ Данные успешно записаны в таблицу {DB_CONFIG['table']}")
            print(f"Всего строк: {len(df)}")

        except SQLAlchemyError as e:
            print(f"❌ Ошибка: {str(e)}")

        finally:
            if 'engine' in locals():
                engine.dispose()
            print("Работа завершена")

ReadAndWrite(FEE_URL, "Fee")
ReadAndWrite(AALL_FEE_URL, "AllFee")

