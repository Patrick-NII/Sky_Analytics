# import pandas as pd
# import os
# import schedule
# import time
# from skimpy import skim
# from datetime import datetime

# # Initialisation des DataFrames globaux
# logs_vols = pd.DataFrame()
# df_degrade = pd.DataFrame()

# # Fonction pour récupérer les données à partir de la base de données
# def keep_data(data):
#     today = datetime.today().strftime('%Y-%m-%d')
    
#     logs_data = pd.read_csv(f"http://sc-e.fr/docs/logs_vols_{today}.csv", sep=",")
#     degradations_data = pd.read_csv(f"http://sc-e.fr/docs/degradations_{today}.csv", sep=",")
    
#     logs_vols = pd.concat([logs_vols, logs_data], axis=0)
#     df_degrade = pd.concat([df_degrade, degradations_data], axis=0)

# # Fonction de nettoyage
# def autoclean_dataset(data):
#     data = pd.read_csv(data)
#     return ("Résumé des données :")
#     return (skim(data))
#     return ("Valeurs manquantes :")
#     return (data.isnull().sum())
#     return ("Valeurs uniques par colonne :")
#     return (data.nunique())
    
#     # Copie du dataset original pour les modifications
#     cleaned_data = data.copy()

# # Définir la planification des tâches
# schedule.every().day.at("11:17").do(keep_data)
# schedule.every().day.at("11:19").do(autoclean_dataset)

# end_date = datetime(2024, 7, 27)
# # Boucle pour exécuter les tâches planifiées
# while datetime.now() < end_date:
#     schedule.run_pending()
#     time.sleep(1)
# print("Fin du programme")


# def say_hello(name):
#     return (f"Hello {name} !")
    
    
    
    
import pandas as pd
import os
from skimpy import skim
from datetime import datetime

# Fonction pour récupérer les données à partir des URLs initiales
def load_data_url():
    today = datetime.today().strftime('%Y-%m-%d')
    logs_url = f"http://sc-e.fr/docs/logs_vols_{today}.csv"
    degrade_url = f"http://sc-e.fr/docs/degradations_{today}.csv"
    
    try:
        logs_vols = pd.read_csv(logs_url)
        df_degrade = pd.read_csv(degrade_url)
    except Exception as e:
        print(f"Erreur lors du chargement des données depuis les URLs : {e}")
        logs_vols = pd.DataFrame()
        df_degrade = pd.DataFrame()
    
    return logs_vols, df_degrade

# Fonction pour charger les derniers fichiers locaux
def local_data():
    logs_vols_path = sorted([f for f in os.listdir('D:\\Top_gun\\Datasets\\df_logs_vols') if f.startswith('logs_vols_')])[-1]
    degradations_path = sorted([f for f in os.listdir('D:\\Top_gun\\Datasets\\df_degradations') if f.startswith('degradations_')])[-1]
    
    logs_vols = pd.read_csv(f"D:\\Top_gun\\Datasets\\df_logs_vols\\{logs_vols_path}")
    df_degrade = pd.read_csv(f"D:\\Top_gun\\Datasets\\df_degradations\\{degradations_path}")
    
    return logs_vols, df_degrade

# Fonction pour sauvegarder les données nettoyées
def save_cleaned_data(df, file_path):
    cleaned_dir = os.path.dirname(file_path)
    if not os.path.exists(cleaned_dir):
        os.makedirs(cleaned_dir)
    df.to_csv(file_path, index=False)
    print(f"Fichier nettoyé sauvegardé à {file_path}")

# Fonction pour nettoyer les datasets
def autoclean_dataset(df):
    print("Résumé des données :")
    print(skim(df))
    print("Valeurs manquantes :")
    print(df.isnull().sum())
    print("Valeurs uniques par colonne :")
    print(df.nunique())
    
    # Copie du dataset original pour les modifications
    cleaned_data = df.copy()
  
    
    return cleaned_data

# Fonction principale pour récupérer, concaténer et nettoyer les données
def main():
    # Charger les derniers fichiers locaux
    local_logs_vols, local_df_degrade = local_data()
    
    # Charger les nouveaux fichiers depuis les URLs
    new_logs_vols, new_df_degrade = load_data_url()
    
    # Concaténer les nouveaux fichiers avec les anciens
    logs_vols = pd.concat([local_logs_vols, new_logs_vols], axis=0)
    df_degrade = pd.concat([local_df_degrade, new_df_degrade], axis=0)
    
    # Nettoyer les fichiers concaténés
    cleaned_logs_vols = autoclean_dataset(logs_vols)
    cleaned_df_degrade = autoclean_dataset(df_degrade)
    
    # Chemins vers les fichiers à écraser
    logs_vols_file_path = f"D:\\Top_gun\\Datasets\\df_logs_vols\\logs_vols_{datetime.today().strftime('%Y-%m-%d')}.csv"
    degradations_file_path = f"D:\\Top_gun\\Datasets\\df_degradations\\degradations_{datetime.today().strftime('%Y-%m-%d')}.csv"
    
    # Sauvegarder les fichiers nettoyés
    save_cleaned_data(cleaned_logs_vols, logs_vols_file_path)
    save_cleaned_data(cleaned_df_degrade, degradations_file_path)

if __name__ == "__main__":
    main()

print("Fin du programme")
