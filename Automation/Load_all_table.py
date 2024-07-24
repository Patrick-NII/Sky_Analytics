
import os
import json
import pandas as pd
import requests
from datetime import datetime, timedelta
from sqlalchemy import create_engine
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

def load_data_url(date):
    degrade_url = f"http://sc-e.fr/docs/degradations_{date}.csv"
    
    try:
        df_degrade = pd.read_csv(degrade_url)
        print(f"Données chargées depuis {degrade_url} pour la date {date}")
    except Exception as e:
        print(f"Erreur lors du chargement des données depuis {degrade_url} : {e}")
        df_degrade = pd.DataFrame()
    
    return df_degrade

def autoclean_degradations(df):
    df['measure_day'] = pd.to_datetime(df['measure_day'], format='%Y-%m-%d', errors='coerce')
    df['need_replacement'] = df['need_replacement'].replace({True: 1, False: 0}).astype(int)
    df['usure_nouvelle'] = round(df['usure_nouvelle'], 0)
    return df

def save_to_database(df, table_name, engine):
    try:
        with engine.begin() as conn:
            df.to_sql(name=table_name, con=conn, if_exists='append', index=False)
            print(f"Données ajoutées à la table {table_name} dans la base de données avec succès.")
    except SQLAlchemyError as e:
        print(f"Erreur lors de l'insertion des données dans la table {table_name} : {e}")

def main():
    # Date de début historique
    start_date = datetime(2024, 6, 2).date()
    # Date du jour
    today = datetime.today().date()
    
    # Itérer sur chaque date de la plage de temps
    current_date = start_date
    while current_date <= today:
        date_str = current_date.strftime('%Y-%m-%d')
        
        # Téléchargement des nouvelles données de dégradations depuis l'URL
        df_degrade = load_data_url(date_str)
        
        # Nettoyage des données de dégradations
        if not df_degrade.empty:
            cleaned_df_degrade = autoclean_degradations(df_degrade)
            
            # Enregistrement des données nettoyées dans la base de données
            save_to_database(cleaned_df_degrade, 'degradations', engine)
        else:
            print(f"Aucune donnée de dégradations à ajouter pour la date {date_str}.")
        
        # Passer à la date suivante
        current_date += timedelta(days=1)

if __name__ == "__main__":
    main()

print("Fin du programme")
