import time
import pandas as pd
import urllib.parse
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import os

# Засекаем общее время выполнения
start_time = time.time()

# 1. Настройки подключения
SHEET_URL = os.environ['SHEET_URL']  # Берём из секретов
SHEET_NAME_hansa = "HansaPart"
SHEET_NAME_vioks = "VIOKS"
SHEET_NAME_grtrade = "GR_Trade"
SHEET_NAME_Olev = "Olev"
SHEET_NAME_Metro = "METRO"
SHEET_NAME_Shegira = "Shegira"
SHEET_NAME_Ebay = "eBay"
SHEET_NAME_otto = "OTTO"
SHEET_NAME_kaufland = "Kaufland.de"
SHEET_NAME_woocommerce = "woocommerceErsatzteilCheck"
SHEET_NAME_woocommerce_aquade = "woocommerceAQUADE"
SHEET_NAME_allegro = "Allegro"

# Подключение для страницы Hanza ----------------------------------------------
csv_url_hansa = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_hansa)}"
df_hansa = pd.read_csv(csv_url_hansa)

df_hansa.rename(columns={
    'Active (FBA/FBM)': 'Active',
}, inplace=True)

df_hansa['Платформа'] = 'amazon'
df_hansa['Company_id'] = '1'

# Подключение для страницы Vioks ---------------------------------------------
csv_url_vioks = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_vioks)}"
df_vioks = pd.read_csv(csv_url_vioks)
df_vioks.rename(columns={
    'Active (FBA/FBM)': 'Active',
}, inplace=True)

df_vioks['Платформа'] = 'amazon'
df_vioks['Company_id'] = '636'

# Подключение для GR-Trade----------------------------------------------------
csv_url_grtrade = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_grtrade)}"
df_grtrade = pd.read_csv(csv_url_grtrade)
df_grtrade.rename(columns={
    'Active (FBA/FBM)': 'Active',
}, inplace=True)

df_grtrade['Платформа'] = 'amazon'
df_grtrade['Company_id'] = '1511'

# Подключение Olev-------------------------------------------------------------
csv_url_olev = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_Olev)}"
df_olev= pd.read_csv(csv_url_olev)

df_olev.rename(columns={
    'Sales €': 'Sales',
    'Units ordered': 'Units'
}, inplace=True)

df_olev['Платформа'] = 'amazon'
df_olev['Company_id'] = '8'

# Подключение Metro -----------------------------------------------------------
csv_url_metro = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_Metro)}"
df_metro= pd.read_csv(csv_url_metro)

df_metro['Платформа'] = 'metro'
df_metro['Company_id'] = '1'

# Подключен Shegira -----------------------------------------------------------
csv_url_shegira = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_Shegira)}"
df_shegira = pd.read_csv(csv_url_shegira)

df_shegira.rename(columns={
    'Active (FBA/FBM)': 'Active'
}, inplace=True)

df_shegira['Платформа'] = 'amazon'
df_shegira['Company_id'] = '1731'

# Подключение Ebay -----------------------------------------------------------
csv_url_ebay = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_Ebay)}"
df_ebay = pd.read_csv(csv_url_ebay)

df_ebay.rename(columns={
    'Sales (EUR)': 'Sales'
}, inplace=True)

df_ebay['Платформа'] = 'ebay'
df_ebay['Company_id'] = '1'

# Подключение Otto------------------------------------------------------------
csv_url_otto = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_otto)}"
df_otto = pd.read_csv(csv_url_otto)

df_otto.rename(columns={
    'Units ordered': 'Units',
    'Units returned': 'Returned'
}, inplace=True)

df_otto['Платформа'] = 'otto'
df_otto['Company_id'] = '1'

# Подключение Kaufland---------------------------------------------------------
csv_url_kaufland = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_kaufland)}"
df_kaufland = pd.read_csv(csv_url_kaufland)

df_kaufland.rename(columns={
    'Sales (EUR)': 'Sales',
}, inplace=True)

df_kaufland['Платформа'] = 'kaufland'
df_kaufland['Company_id'] = '1'

# Подключение woocommerce.erztail---------------------------------------------
csv_url_wooerz = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_woocommerce)}"
df_wocommerceerztail = pd.read_csv(csv_url_wooerz)

df_wocommerceerztail.rename(columns={
    'Sales (EUR)': 'Sales',
}, inplace=True)

df_wocommerceerztail['Платформа'] = 'ersatzteil'
df_wocommerceerztail['Company_id'] = '6'

# Подключение allegro-----------------------------------------------
csv_url_allegro = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_allegro)}"
df_allegro = pd.read_csv(csv_url_allegro)

df_allegro.rename(columns={
    'Active (FBA/FBM)': 'Active',
}, inplace=True)

df_allegro['Платформа'] = 'allegro'
df_allegro['Company_id'] = '1'

# Создаём подключение wocommere_aquade-----------------------------------------
csv_url_erz = f"{SHEET_URL}/gviz/tq?tqx=out:csv&sheet={urllib.parse.quote(SHEET_NAME_woocommerce_aquade)}"
df_erz = pd.read_csv(csv_url_erz)

df_erz.rename(columns={
    'Sales (EUR)': 'Sales',
}, inplace=True)

df_erz['Платформа'] = 'aquade'
df_erz['Company_id'] = '1'

# Собираем финальный DateFrame------------------------------------------------
df_final = []

df_final.append(df_hansa)
df_final.append(df_vioks)
df_final.append(df_grtrade)
df_final.append(df_olev)
df_final.append(df_metro)
df_final.append(df_shegira)
df_final.append(df_otto)
df_final.append(df_ebay)
df_final.append(df_kaufland)
df_final.append(df_wocommerceerztail)
df_final.append(df_allegro)
df_final.append(df_erz)

combined_df = pd.concat(df_final, ignore_index=True)

nessery_columns = ['Date', 'Sales', 'Units', 'Inactive', 'Active', 
                   'Платформа', 'Company_id', 'Залистовано', 
                   'Оптимизировано', 'Active ', 'Returns']


available_columns = [col for col in nessery_columns if col in combined_df.columns]
combined_df = combined_df[available_columns]
# 3. Пример использования
print(combined_df.head())
print(combined_df.columns.tolist())
# Учетные данные из секретов
DB_CONFIG = {
    'user': os.environ['DB_USER'],
    'password': os.environ['DB_PASSWORD'],
    'host': os.environ['DB_HOST'],
    'database': os.environ['DB_NAME'],
    'table': "GoggleDocAccaunts"
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
    combined_df.to_sql(
        name=DB_CONFIG['table'],
        con=engine,
        if_exists='replace',  # ← Новое значение
        index=False,
        chunksize=1000,
        method='multi'
    )
    print(f"✅ Данные успешно записаны в таблицу {DB_CONFIG['table']}")
    print(f"Всего строк: {len(combined_df)}")

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
