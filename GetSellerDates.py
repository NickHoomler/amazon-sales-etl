import os
import time
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
import pymysql

start_time = time.time()

source_a_url = os.getenv('SOURCE_A_URL')
source_b_url = os.getenv('SOURCE_B_URL')
source_c_url = os.getenv('SOURCE_C_URL')

source_mapping = {
    source_a_url: 101,
    source_b_url: 102,
    source_c_url: 103
}

required_columns = ['Date', 'SalesOrganic', 'SalesPPC', 'UnitsOrganic', 'UnitsPPC', 'GrossProfit']

final_df = pd.DataFrame()

for url, company_code in source_mapping.items():
    try:
        df = pd.read_csv(url)
        df_filtered = df[required_columns].copy()
        df_filtered['Platform'] = "online_marketplace"
        df_filtered['Seller_Code'] = company_code
        final_df = pd.concat([final_df, df_filtered], ignore_index=True)
        print(f"Data from {url} processed successfully!")
    except Exception as e:
        print(f"Error processing {url}: {e}")

DB_CONFIG = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'database': os.getenv('DB_NAME'),
    'table': "Sales_Data"
}

try:
    engine = create_engine(
        f"mysql+pymysql://{DB_CONFIG['user']}:{DB_CONFIG['password']}"
        f"@{DB_CONFIG['host']}/{DB_CONFIG['database']}"
    )
    
    with engine.connect() as conn:
        print("✅ Database connection successful!")
    
    final_df.to_sql(
        name=DB_CONFIG['table'],
        con=engine,
        if_exists='replace',  
        index=False,
        chunksize=1000,
        method='multi'
    )
    print(f"✅ Data successfully written to table {DB_CONFIG['table']}")
    print(f"Total rows: {len(final_df)}")

except SQLAlchemyError as e:
    print(f"❌ Database error: {str(e)}")
    if "Unknown database" in str(e):
        print("Check database name")
    elif "Access denied" in str(e):
        print("Check username/password")
    elif "Table" in str(e) and "doesn't exist" in str(e):
        print("Check table name or use if_exists='replace'")
        
finally:
    if 'engine' in locals():
        engine.dispose()
        print("Process completed")
    
end_time = time.time()
print(f"Execution time: {end_time - start_time:.2f} seconds")
