# Algorithmes d'automation de taches

import schedule
import time
from datetime import datetime



def current_time():
    now = datetime.now()
    print('What is the current time?', now.strftime('%H:%M:%S'))

def hello():
    print("hello, how are you?")

# Définir les tâches pour qu'elles se lancent toutes les 10 secondes
schedule.every(1).days.do(current_time)
schedule.every(1).days.do(hello)

# Lancer la boucle pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)


# Exécution à une heure précise
schedule.every().day.at("10:55").do(current_time)

while True:
    schedule.run_pending()
    time.sleep(1)
    
    
# Avec choix du jour de la semaine
schedule.every().wednesday.at("16:00").do(current_time)

while True:
    schedule.run_pending()
    time.sleep(1)
    
    
    
    
    
# Algotithme de nettoyage de dataset

def autoclean_dataset(dataset_path):
    try:
        # Chargement du dataset
        data = pd.read_csv(dataset_path)
    except FileNotFoundError:
        print("Le fichier n'existe pas.")
        return None
    except Exception:
        print("Erreur lors du chargement du dataset :")
        return None
    
    # Suppression des doublons
    original_shape = data.shape
    data.drop_duplicates(inplace=True)
    duplicates_removed = original_shape[0] - data.shape[0]
    if duplicates_removed > 0:
        print(f"{duplicates_removed} Doublons supprimées.")
    else:
        print("Aucun doublons trouvée.")
        
        
    # Exploration des données
    print("Résumé des données :")
    print(skim(data))
    print("Valeurs manquantes :")
    print(data.isnull().sum())
    print("Valeurs uniques par colonne :")
    print(data.nunique())
    
    # Copie du dataset original pour les modifications
    cleaned_data = data.copy()
    
    # Sauvegarde du dataset nettoyé
    output_folder = "df_clean"
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, "df_clean.csv")
    data.to_csv(output_path, index=False)
    print(f"Dataset nettoyé sauvegardé sous : {output_path}")
    
    return data




import pandas as pd
import os
import schedule
import time
from datetime import datetime

# Fonction pour récupérer les données à partir de la bas de données
def retrieve_data():
    
    today = datetime.today().strftime('%Y-%m-%d')
    
    logs_url = f"http://sc-e.fr/docs/logs_vols_{today}.csv"
    degradations_url = f"http://sc-e.fr/docs/degradations_{today}.csv"
    
    
    logs_data = pd.read_csv(logs_url)
    degradations_data = pd.read_csv(degradations_url)
    

# Fonction de nettoyage
def autoclean_dataset(dataset_path):
    # Ajoutez ici votre fonction de nettoyage de données existante

    # Automatisation des tâches
    def main_task():
        # Récupérez les données
        retrieve_data()
        # Nettoyez les données
        autoclean_dataset("data_clean.csv")

# Définir la planification des tâches
schedule.every().day.at("09:30").do(main_task)

# Boucle pour exécuter les tâches planifiées
while True:
    schedule.run_pending()
    time.sleep(1)
