import os
import json
import pandas as pd
import requests
from datetime import datetime
from sqlalchemy import create_engine, inspect
import re

# Configuration de la connexion à la base de données MySQL
db_user = 'root'
db_password = 'user_42'
db_host = '127.0.0.1'
db_port = '3306'  #
db_name = 'localhost'
engine = create_engine(f'mysql+mysqlconnector://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

def table_exists(engine, table_name):
    inspector = inspect(engine)
    return inspector.has_table(table_name)

def load_data_url(date):
    logs_url = f"http://sc-e.fr/docs/logs_vols_{date}.csv"
    degrade_url = f"http://sc-e.fr/docs/degradations_{date}.csv"
    try:
        logs_vols = pd.read_csv(logs_url)
        df_degrade = pd.read_csv(degrade_url)
        print(f"Données chargées depuis les URLs pour la date {date}")
    except Exception as e:
        print(f"Erreur lors du chargement des données depuis les URLs : {e}")
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
    
    df['jour_vol'] = pd.to_datetime(df['jour_vol'], format='%Y-%m-%d', errors='coerce')
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

def update_daily_data(engine, date, table_name, clean_function, url_function):
    data = url_function(date)
    cleaned_data = clean_function(data)
    if not cleaned_data.empty:
        cleaned_data.to_sql(name=table_name, con=engine, if_exists='append', index=False)
        print(f"Données pour {table_name} du {date} ajoutées à la base de données.")
    else:
        print(f"Aucune donnée à ajouter pour {table_name} du {date}.")

def main():
    # Initialiser les tables statiques
    initialize_database(engine, 'aeronefs', r'D:\Sky_Analytics\Datasets\aeronefs.csv', lambda df: df)
    initialize_database(engine, 'composants', r'D:\Sky_Analytics\Datasets\composants.csv', lambda df: df)

    # Initialiser les tables dynamiques
    initialize_database(engine, 'degradations', r'D:\Sky_Analytics\Datasets\df_degradations\degradations_2024-06-02.csv', clean_degradations)
    initialize_database(engine, 'logs_vols', r'D:\Sky_Analytics\Datasets\df_logs_vols\logs_vols_2024-06-02.csv', clean_logs_vols)
    
    # Date du jour
    today_str = datetime.today().strftime('%Y-%m-%d')

    # Mettre à jour les tables dynamiques avec les données du jour
    logs_vols, degradations = load_data_url(today_str)
    cleaned_logs_vols = clean_logs_vols(logs_vols)
    cleaned_degradations = clean_degradations(degradations)
    
    cleaned_logs_vols.to_sql(name='logs_vols', con=engine, if_exists='append', index=False)
    cleaned_degradations.to_sql(name='degradations', con=engine, if_exists='append', index=False)

if __name__ == "__main__":
    main()
