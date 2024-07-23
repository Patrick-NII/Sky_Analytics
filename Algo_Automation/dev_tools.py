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

"""
Information de connexion à la base de données MySQL sur le serveur distant

username = 'remote_user'
password = 'remote_password'
host = '31.38.158.71'
database = 'Sky_Analytics'

"""


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

def get_last_date(engine, table_name, date_column):
    if not table_exists(engine, table_name):
        return None
    query = text(f"SELECT MAX({date_column}) FROM {table_name}")
    with engine.connect() as connection:
        result = connection.execute(query).scalar()
    return result

def load_data_url(date):
    logs_url = f"http://sc-e.fr/docs/logs_vols_{date}.csv"
    degrade_url = f"http://sc-e.fr/docs/degradations_{date}.csv"
    
        logs_vols = pd.read_csv(logs_url)
        df_degrade = pd.read_csv(degrade_url)
        print(f"Données chargées depuis les URLs pour la date {date}")
        logs_vols = pd.DataFrame()
        df_degrade = pd.DataFrame()
    return logs_vols, df_degrade

def clean_logs_vols(df):
    def fix_json_format(x):
        return x.replace('\'', '\"')
    
    df['sensor_data'] = df['sensor_data'].apply(lambda x: json.loads(fix_json_format(x)) if pd.notnull(x) else {})
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

def clean_degradations(df):
    df['measure_day'] = pd.to_datetime(df['measure_day'], format='%Y-%m-%d', errors='coerce').dt.date
    df['need_replacement'] = df['need_replacement'].replace({True: 1, False: 0}).astype(int)
    df['usure_nouvelle'] = df['usure_nouvelle'].round(0)
    df = df[df['linked_aero'] != 'E170_5551']
    return df

def initialize_database(engine, table_name, file_path, clean_function):
    if not table_exists(engine, table_name):
        df = pd.read_csv(file_path)
        cleaned_data = clean_function(df)
        cleaned_data.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
        print(f"Base de données initialisée avec le fichier {file_path} pour la table {table_name}.")
    else:
        print(f"La table {table_name} existe déjà, aucune initialisation nécessaire.")

def update_daily_data(engine, start_date, end_date, table_name, clean_function, url_function):
    current_date = start_date
    while current_date <= end_date:
        date_str = current_date.strftime('%Y-%m-%d')
        logs_vols, degradations = load_data_url(date_str)
        if table_name == 'logs_vols':
            cleaned_data = clean_function(logs_vols)
        else:
            cleaned_data = clean_function(degradations)
        if not cleaned_data.empty:
            cleaned_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
            print(f"Données pour {table_name} du {date_str} ajoutées à la base de données.")
        else:
            print(f"Aucune donnée à ajouter pour {table_name} du {date_str}.")
        current_date += timedelta(days=1)

def main():
    # Initialiser les tables statiques
    initialize_database(engine, 'aeronefs', r'E:\Sky_Analytics\Datasets\df_aeronef\aeronefs_2024-06-02.csv', lambda df: df)
    initialize_database(engine, 'composants', r'E:\Sky_Analytics\Datasets\df_composants\composants_2024-06-02.csv', lambda df: df)

    # Initialiser les tables dynamiques
    initialize_database(engine, 'degradations', r'E:\Sky_Analytics\Datasets\df_degradations\degradations_2024-06-02.csv', clean_degradations)
    initialize_database(engine, 'logs_vols', r'E:\Sky_Analytics\Datasets\df_logs_vols\logs_vols_2024-06-02.csv', clean_logs_vols)
    
    # Date du jour
    today = datetime.today().date()
    
    # Récupérer la dernière date de mise à jour dans les tables logs_vols et degradations
    last_logs_date = get_last_date(engine, 'logs_vols', 'jour_vol') or today
    last_degradations_date = get_last_date(engine, 'degradations', 'measure_day') or today
    
    # Convertir en date uniquement pour la comparaison
    if isinstance(last_logs_date, datetime):
        last_logs_date = last_logs_date.date()
    if isinstance(last_degradations_date, datetime):
        last_degradations_date = last_degradations_date.date()
    
    # Déterminer la date de début pour l'actualisation (le jour suivant la dernière date connue)
    start_logs_date = last_logs_date + timedelta(days=1)
    start_degradations_date = last_degradations_date + timedelta(days=1)
    
    # Mettre à jour les tables dynamiques avec les données manquantes
    update_daily_data(engine, start_logs_date, today, 'logs_vols', clean_logs_vols, load_data_url)
    update_daily_data(engine, start_degradations_date, today, 'degradations', clean_degradations, load_data_url)

if __name__ == "__main__":
    main()

# Utilisation du cron pour automatiser l'exécution du script tous les jours à midi
# 0 12 * * * /user/bin/python3 /d:/Sky_Analytics/Preprocessing/dev_tools.py

