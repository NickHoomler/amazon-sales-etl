import os
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql

start_time = time.time()

# Получаем URL из переменных окружения
Vioks_dates = os.getenv('VIOKS_URL')
Hanza_dates = os.getenv('HANZA_URL')
GR_Trade_Dates = os.getenv('GR_TRADE_URL')

Dict_date = {
    Vioks_dates: 636,
    Hanza_dates: 1,
    GR_Trade_Dates: 636
}   # Словарь для последующей маркировки данных

needed_columns = ['Date', 'SalesOrganic', 'SalesPPC', 'UnitsOrganic', 'UnitsPPC', 'GrossProfit']

final_df = pd.DataFrame()

# Цикл по ссылкам и значениям словаря
for url, company_id in Dict_date.items():
    try:
        # Загрузка CSV
        df = pd.read_csv(url)
        
        # Оставляем только нужные столбцы
        df_filtered = df[needed_columns].copy()
        
        # Добавляем столбцы Platform и Company
        df_filtered['Platform'] = "amazon"
        df_filtered['Company'] = company_id
        
        # Добавляем данные в итоговый DataFrame
        final_df = pd.concat([final_df, df_filtered], ignore_index=True)
        
        print(f"Данные из {url} успешно обработаны!")
    
    except Exception as e:
        print(f"Ошибка при обработке {url}: {e}")

# Получаем конфигурацию БД из переменных окружения
DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'table': "SellerBoard"
} 

try:
    # 1. Создаем подключение
    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    )
    
    # 2. Проверяем подключение
    with engine.connect() as conn:
        print("✅ Подключение к MySQL успешно!")
    
    # 3. Записываем DataFrame в базу данных 
    final_df.to_sql(
        name=DB_CONFIG['table'],
        con=engine,
        if_exists='replace',  
        index=False,
        chunksize=1000,
        method='multi'  # Ускорение записи
    )
    print(f"✅ Данные успешно записаны в таблицу {DB_CONFIG['table']}")
    print(f"Всего строк: {len(final_df)}")

except SQLAlchemyError as e:
    print(f"❌ Ошибка: {str(e)}")
    if "Unknown database" in str(e):
        print("Проверьте имя базы данных")
    elif "Access denied" in str(e):
        print("Проверьте логин/пароль")
    elif "Table" in str(e) and "doesn't exist" in str(e):
        print("Укажите правильное имя таблицы или используйте if_exists='replace'")
        
finally:
    if 'engine' in locals():
        engine.dispose()
    print("Работа завершена")
    
end_time = time.time()
execution_time = end_time - start_time
print(f"Время выполнения: {execution_time} секунд")
