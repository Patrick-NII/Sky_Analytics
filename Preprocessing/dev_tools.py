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
    
    
    
import os
import os
import json
import pandas as pd
from datetime import datetime

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

def get_latest_file_path(directory, prefix):
    files = sorted([f for f in os.listdir(directory) if f.startswith(prefix)])
    return os.path.join(directory, files[-1]) if files else None

def save_cleaned_data(df, file_path):
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    df.to_csv(file_path, index=False)
    print(f"Fichier nettoyé et sauvegardé dans {file_path}")


def autoclean_logs_vols(df):
    def fix_json_format(x):
        return x.replace('\'', '\"')
    
    df['sensor_data'] = df['sensor_data'].apply(lambda x: json.loads(fix_json_format(x)))
    sensor_data_df = pd.json_normalize(df['sensor_data'])
    df = df.drop(columns=['sensor_data'])
    
    # Réinitialisation index avant concaténation pour éviter les doublons ce bout de code a revoir
    # car il ne fonctionne pas possible de la replacer autre part
    df.reset_index(drop=True, inplace=True)
    sensor_data_df.reset_index(drop=True, inplace=True)
    df = pd.concat([df, sensor_data_df], axis=1)
    # format datetime à revoir car il n'est pas pris en compte
    df['jour_vol'] = pd.to_datetime(df['jour_vol'], format='%Y-%m-%d', errors='coerce')
    df['temp en °C'] = df['temp'].replace('°C', '', regex=True).astype(float).infer_objects(copy=False)
    df['pressure en hPa'] = df['pressure'].replace('hPa', '', regex=True).astype(float).infer_objects(copy=False)
    df['vibrations en m/s²'] = df['vibrations'].replace('m/s²', '', regex=True).astype(float).round(3).infer_objects(copy=False)
    df.drop(columns=['temp', 'pressure', 'vibrations'], inplace=True)
    
    return df


def autoclean_degradations(df):
    # le format datetime aussi a revoir car il n'est pas pris en compte
    df['measure_day'] = pd.to_datetime(df['measure_day'], format='%Y-%m-%d', errors='coerce')
    df['need_replacement'] = df['need_replacement'].replace({True: 1, False: 0}).infer_objects(copy=False)
    df['usure_nouvelle'] = round(df['usure_nouvelle'], 0)
    
    return df

def main():
    # Chemins vers les fichiers locaux
    logs_vols_path = get_latest_file_path(r'D:\Top_gun\Datasets\df_logs_vols', 'all_data_logs_vols.csv')
    degradations_path = get_latest_file_path(r'D:\Top_gun\Datasets\df_degradations', 'all_data_degradations.csv')
    
    # Chargement des données locales
    if logs_vols_path and degradations_path:
        local_logs_vols = pd.read_csv(logs_vols_path)
        local_df_degrade = pd.read_csv(degradations_path)
    else:
        local_logs_vols, local_df_degrade = pd.DataFrame(), pd.DataFrame()

    # Téléchargement des nouvelles données depuis les URLs
    new_logs_vols, new_df_degrade = load_data_url()
    
    # Concaténation des données locaux et nouveaux
    logs_vols = pd.concat([local_logs_vols, new_logs_vols], axis=0)
    df_degrade = pd.concat([local_df_degrade, new_df_degrade], axis=0)
    
    # Nettoyage des données concaténés
    cleaned_logs_vols = autoclean_logs_vols(logs_vols)
    cleaned_df_degrade = autoclean_degradations(df_degrade)
    
    # Export des données nettoyées
    today_str = datetime.today().strftime('%Y-%m-%d')
    save_cleaned_data(cleaned_logs_vols, f"D:\\Top_gun\\Data_clean\\Update_logs_vols_{today_str}.csv")
    save_cleaned_data(cleaned_df_degrade, f"D:\\Top_gun\\Data_clean\\Update_degradations_{today_str}.csv")

if __name__ == "__main__":
    main()

print("Fin du programme")
