import pandas as pd
import os
import schedule
import time
from skimpy import skim
from datetime import datetime

# Fonction pour récupérer les données à partir de la bas de données
def keep_data():
    
    today = datetime.today().strftime('%Y-%m-%d')
    
    logs_url = f"http://sc-e.fr/docs/logs_vols_{today}.csv"
    degradations_url = f"http://sc-e.fr/docs/degradations_{today}.csv"
    
    
    logs_data = pd.read_csv(logs_url)
    degradations_data = pd.read_csv(degradations_url)
    logs_vols = pd.concat([logs_data, logs_vols], axis=0)
    df_degrade = pd.concat([degradations_data, degradations], axis=0)




# Fonction de nettoyage
def autoclean_dataset(data):
     # Exploration des données
    print("Résumé des données :")
    print(skim(data))
    print("Valeurs manquantes :")
    print(data.isnull().sum())
    print("Valeurs uniques par colonne :")
    print(data.nunique())
    
    # Copie du dataset original pour les modifications
    cleaned_data = data.copy()
   
    # Automatisation des tâches
    def main_task():
        
        keep_data_data()
        
        autoclean_dataset("data_clean.csv")

# Définir la planification des tâches
schedule.every().day.at("09:30").do(main_task)

# Boucle pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)
