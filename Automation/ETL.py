import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import SQLAlchemyError
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

def table_exists(engine, table):
    inspector = inspect(engine)
    return inspector.has_table(table)

def get_last_date(engine, table, date_col):
    if not table_exists(engine, table):
        return None
    query = text(f"SELECT MAX({date_col}) FROM {table}")
    with engine.connect() as conn:
        result = conn.execute(query).scalar()
    return result

def check_url_exists(url):
    try:
        response = requests.head(url)
        return response.status_code == 200
    except requests.RequestException:
        return False

# ETL Process
class ETLProcess:
    def __init__(self, engine, start_date, end_date, table, clean_fn):
        self.engine = engine
        self.start_date = start_date
        self.end_date = end_date
        self.table = table
        self.clean_fn = clean_fn

    def extract(self, date):
        logs_url = f"http://sc-e.fr/docs/logs_vols_{date}.csv"
        degrade_url = f"http://sc-e.fr/docs/degradations_{date}.csv"
        try:
            logs_vols = pd.read_csv(logs_url)
            df_degrade = pd.read_csv(degrade_url)
            print(f"Données chargées depuis les URLs pour la date {date}")
        except Exception as e:
            print(f"Erreur lors du chargement des données depuis les URLs : {e} 😞")
            logs_vols = pd.DataFrame()
            df_degrade = pd.DataFrame()
        return logs_vols, df_degrade

    def transform(self, data):
        if not data.empty:
            cleaned_data = self.clean_fn(data)
            cleaned_data.drop_duplicates(inplace=True)
            return cleaned_data
        else:
            return pd.DataFrame()

    def load(self, data):
        if not data.empty:
            try:
                with self.engine.begin() as conn:
                    data.to_sql(name=self.table, con=conn, if_exists='append', index=False)
                    print(f"Données pour {self.table} ajoutées à la base de données avec succès 😊")
            except SQLAlchemyError as e:
                print(f"Erreur lors du chargement des données dans la table {self.table} : {e}")
        else:
            print(f"Aucune donnée à ajouter pour la table {self.table}")

    def run(self):
        current_date = self.start_date
        while current_date <= self.end_date:
            date_str = current_date.strftime('%Y-%m-%d')
            logs_vols, degradations = self.extract(date_str)
            if self.table == 'logs_vols':
                new_data = self.transform(logs_vols)
            else:
                new_data = self.transform(degradations)
            self.load(new_data)
            current_date += timedelta(days=1)

def clean_logs(df):
    required_columns = ['jour_vol', 'temp', 'pressure', 'vibrations']
    
    if not all(col in df.columns for col in required_columns):
        print("Colonnes manquantes dans le DataFrame")
        return pd.DataFrame()  # Retourner un DataFrame vide si des colonnes nécessaires sont manquantes
    
    if 'sensor_data' in df.columns:
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

def clean_degrades(df):
    required_columns = ['measure_day', 'need_replacement', 'usure_nouvelle', 'linked_aero']
    
    if not all(col in df.columns for col in required_columns):
        print("Colonnes manquantes dans le DataFrame")
        return pd.DataFrame()  # Retourner un DataFrame vide si des colonnes nécessaires sont manquantes
    
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
        print(f"Base de données initialisée avec le fichier {file_path} pour la table {table}.")
    else:
        print(f"La table {table} existe déjà, aucune initialisation nécessaire.")

def drop_dup(engine, table):
    try:
        with engine.begin() as conn:
            df = pd.read_sql_table(table, conn)
            if not df.empty:
                df.drop_duplicates(inplace=True)
                df.to_sql(name=table, con=conn, if_exists='replace', index=False)
                print(f"Suppression des doublons pour la table {table}.")
            else:
                print(f"La table {table} est vide. Aucune suppression des doublons effectuée.")
    except SQLAlchemyError as e:
        print(f"Erreur lors de la suppression des doublons pour la table {table} : {e}")

def reload_historical_data(engine, start_date, end_date, table, clean_fn):
    etl_process = ETLProcess(engine, start_date, end_date, table, clean_fn)
    etl_process.run()

def main():
    # Initialiser les tables statiques
    init_db(engine, 'aeronefs', r'E:\Sky_Analytics\Datasets\df_aeronef\aeronefs_2024-06-02.csv', lambda df: df)
    init_db(engine, 'composants', r'E:\Sky_Analytics\Datasets\df_composants\composants_2024-06-02.csv', lambda df: df)

    # Initialiser les tables dynamiques
    init_db(engine, 'degradations', r'E:\Sky_Analytics\Datasets\df_degradations\degradations_2024-06-02.csv', clean_degrades)
    init_db(engine, 'logs_vols', r'E:\Sky_Analytics\Datasets\df_logs_vols\logs_vols_2024-06-02.csv', clean_logs)
    
    # Supprimer les doublons avant la mise à jour
    drop_dup(engine, 'logs_vols')
    drop_dup(engine, 'degradations')
    
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
    logs_etl = ETLProcess(engine, start_logs_date, today, 'logs_vols', clean_logs)
    logs_etl.run()
    
    degrades_etl = ETLProcess(engine, start_degrades_date, today, 'degradations', clean_degrades)
    degrades_etl.run()
    
    # Supprimer les doublons après la mise à jour
    drop_dup(engine, 'logs_vols')
    drop_dup(engine, 'degradations')
    
    # Recharger les données historiques si nécessaire
    historical_start_date = datetime(2024, 6, 1).date()  # Date de début historique
    reload_historical_data(engine, historical_start_date, today, 'logs_vols', clean_logs)
    reload_historical_data(engine, historical_start_date, today, 'degradations', clean_degrades)

if __name__ == "__main__":
    main()

# Utilisation du cron pour automatiser l'exécution du script tous les jours à midi
# 0 12 * * * /user/bin/python3 /e/Sky_Analytics/Algo_Automation/dev_tools.py






"""
Classe ETLProcess :

__init__() : Initialise les paramètres nécessaires pour l'ETL.
extract() : Extrait les données des sources.
transform() : Transforme et nettoie les données.
load() : Charge les données transformées dans la base de données.
run() : Gère le processus ETL pour les dates spécifiées.



Fonctions de nettoyage :
clean_logs() et clean_degrades() : Nettoient et transforment les données.


Fonction drop_dup() :
Supprime les doublons dans une table donnée.



Main Function :

Initialisation des tables statiques.
Suppression des doublons avant la mise à jour.
Exécution du processus ETL pour les tables dynamiques.
Suppression des doublons après la mise à jour.
Exécution du script avec le cron pour automatiser le processus.

"""