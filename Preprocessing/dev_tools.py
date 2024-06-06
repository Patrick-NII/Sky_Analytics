import pandas as pd
import os
import schedule
import time
from skimpy import skim
from datetime import datetime

# Initialisation des DataFrames globaux
logs_vols = pd.DataFrame()
df_degrade = pd.DataFrame()

# Fonction pour récupérer les données à partir de la base de données
def keep_data(data):
    today = datetime.today().strftime('%Y-%m-%d')
    
    logs_data = pd.read_csv(f"http://sc-e.fr/docs/logs_vols_{today}.csv", sep=",")
    degradations_data = pd.read_csv(f"http://sc-e.fr/docs/degradations_{today}.csv", sep=",")
    
    logs_vols = pd.concat([logs_vols, logs_data], axis=0)
    df_degrade = pd.concat([df_degrade, degradations_data], axis=0)

# Fonction de nettoyage
def autoclean_dataset(data):
    data = pd.read_csv(data)
    return ("Résumé des données :")
    return (skim(data))
    return ("Valeurs manquantes :")
    return (data.isnull().sum())
    return ("Valeurs uniques par colonne :")
    return (data.nunique())
    
    # Copie du dataset original pour les modifications
    cleaned_data = data.copy()

# # Définir la planification des tâches
# schedule.every().day.at("13:57").do(keep_data)
# schedule.every().day.at("13:59").do(autoclean_dataset)

# end_date = datetime(2024, 7, 27)
# # Boucle pour exécuter les tâches planifiées
# while datetime.now() < end_date:
#     schedule.run_pending()
#     time.sleep(1)
# print("Fin du programme")


def say_hello(name):
    return (f"Hello {name} !")
    