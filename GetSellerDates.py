# -*- coding: utf-8 -*-
"""
Created on Sun Jul  6 00:16:02 2025

@author: User
"""
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql

start_time = time.time()

Vioks_dates = "https://app.sellerboard.com/ru/automation/reports?id=56e1dbce79d548818a3260f99ffcb2a0&format=csv&t=8361556bbd5d48b6bf34bf155fd93cc4"
Hanza_dates = "https://app.sellerboard.com/ru/automation/reports?id=6e929303e5b947099a80037c91b0eaea&format=csv&t=4e24b88d08a24a1e950aca3d3c58a610"
GR_Trade_Dates = "https://app.sellerboard.com/ru/automation/reports?id=ba3f19be5a044ae4b9b315311d863ea5&format=csv&t=94fa57e661fb4246b77f9ad944f735b9"

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

DB_CONFIG = {
'user' : "n.osipov",
'password': "Yt9rGxHTUnwx",
'host' : "194.36.145.26",  # или IP-адрес сервера
'database' : "db1",
'table': "SellerBoard"} 

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
        if_exists='append',  
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