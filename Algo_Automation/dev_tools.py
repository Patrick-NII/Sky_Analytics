import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta, date
from sqlalchemy import create_engine, inspect, text
import re
import warnings

# Ignorer les FutureWarnings
warnings.simplefilter(action='ignore', category=FutureWarning)

# Configuration de la connexion à la base de données MySQL
db_user = 'Top_gun'
db_password = 'zg6N&284Bb<w'
db_host = '212.227.48.180'
db_port = '3306'
db_name = 'Top_gun'
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def table_exists(engine, table_name):
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def get_last_date(engine, table, date_col):
    if not table_exists(engine, table):
        return None
    query = text(f"SELECT MAX({date_col}) FROM {table}")
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
    return result

def load_data(date):
    logs_url = f"http://sc-e.fr/docs/logs_vols_{date}.csv"
    degrade_url = f"http://sc-e.fr/docs/degradations_{date}.csv"
   
    logs = pd.read_csv(logs_url)
    degrades = pd.read_csv(degrade_url)
    print(f"Données chargées depuis les URLs pour la date {date}")
    
    logs = pd.DataFrame()
    degrades = pd.DataFrame()
        
    return logs, degrades

def clean_logs(df):
    df['sensor_data'] = df['sensor_data'].apply(lambda x: json.loads(x.replace('\'', '\"')) if pd.notnull(x) else {})
    sensor_data_df = pd.json_normalize(df['sensor_data'])
    df = df.drop(columns=['sensor_data'])
    df = pd.concat([df, sensor_data_df], axis=1)
    
    df['jour_vol'] = pd.to_datetime(df['jour_vol'], format='%Y-%m-%d', errors='coerce').dt.date
    df['temp'] = df['temp'].str.replace('°C', '').astype(float)
    df['pressure'] = df['pressure'].str.replace('hPa', '').astype(float)
    df['vibrations'] = df['vibrations'].str.replace('m/s²', '').astype(float)
    
    df.rename(columns={
        'temp': 'temp en °C',
        'pressure': 'pressure en hPa',
        'vibrations': 'vibrations en m/s²'
    }, inplace=True)
    
    return df

def clean_degrades(df):
    df['measure_day'] = pd.to_datetime(df['measure_day'], format='%Y-%m-%d', errors='coerce').dt.date
    df['need_replacement'] = df['need_replacement'].replace({True: 1, False: 0}).astype(int)
    df['usure_nouvelle'] = df['usure_nouvelle'].round(0)
    df = df[df['linked_aero'] != 'E170_5551']
    return df

def init_db(engine, table, file_path, clean_fn):
    if not table_exists(engine, table):
        df = pd.read_csv(file_path)
        cleaned_data = clean_fn(df)
        cleaned_data.to_sql(name=table, con=engine, if_exists='replace', index=False)
        print(f"Base de données initialisée avec le fichier {file_path} pour la table {table}")
    else:
        print(f"La table {table} existe déjà,pas d'initialisation")

def update_data(engine, start_date, end_date, table, clean_fn, load_fn):
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        logs, degrades = load_fn(date_str)
        if table == 'logs_vols':
            new_data = clean_fn(logs)
        else:
            new_data = clean_fn(degrades)
        if not new_data.empty:
            with engine.connect() as conn:
                existing_dates = pd.read_sql(f"SELECT DISTINCT {new_data.columns[0]} FROM {table}", conn)
                new_data = new_data[~new_data[new_data.columns[0]].isin(existing_dates[existing_dates.columns[0]])]
                if not new_data.empty:
                    new_data.to_sql(name=table, con=engine, if_exists='append', index=False)
                    print(f"Données pour {table} du {date_str} ajoutées à la base de données")
                else:
                    print(f"Pas de donnée à mettre à jour pour {table} du {date_str}")
        else:
            print(f"Pas de donnée à mettre à jour {table} du {date_str}")
        current_date += timedelta(days=1)

def main():
    # Initialiser les tables statiques
    init_db(engine, 'aeronefs', r'E:\Sky_Analytics\Datasets\df_aeronef\aeronefs_2024-06-02.csv', lambda df: df)
    init_db(engine, 'composants', r'E:\Sky_Analytics\Datasets\df_composants\composants_2024-06-02.csv', lambda df: df)

    # Initialiser les tables dynamiques
    init_db(engine, 'degradations', r'E:\Sky_Analytics\Datasets\df_degradations\degradations_2024-06-02.csv', clean_degrades)
    init_db(engine, 'logs_vols', r'E:\Sky_Analytics\Datasets\df_logs_vols\logs_vols_2024-06-02.csv', clean_logs)
    
    # Date du jour
    today = datetime.today().date()
    
    # Récupérer la dernière date de mise à jour dans les tables logs_vols et degradations
    last_logs_date = get_last_date(engine, 'logs_vols', 'jour_vol') or today
    last_degrades_date = get_last_date(engine, 'degradations', 'measure_day') or today
    
    # Convertir en date uniquement pour la comparaison
    if isinstance(last_logs_date, datetime):
        last_logs_date = last_logs_date.date()
    if isinstance(last_degrades_date, datetime):
        last_degrades_date = last_degrades_date.date()
    
    # Déterminer la date de début pour l'actualisation (le jour suivant la dernière date connue)
    start_logs_date = last_logs_date + timedelta(days=1)
    start_degrades_date = last_degrades_date + timedelta(days=1)
    
    # Mettre à jour les tables dynamiques avec les données manquantes
    update_data(engine, start_logs_date, today, 'logs_vols', clean_logs, load_data)
    update_data(engine, start_degrades_date, today, 'degradations', clean_degrades, load_data)

if __name__ == "__main__":
    main()


# Utilisation du cron pour automatiser l'exécution du script tous les jours à midi
# 0 12 * * * /user/bin/python3 /d:/Sky_Analytics/Preprocessing/dev_tools.py
