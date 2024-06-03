def autoclean_dataset(dataset_path):
    try:
        # Chargement du dataset
        data = pd.read_csv(dataset_path)
    except FileNotFoundError:
        print("Le fichier spécifié n'existe pas.")
        return None
    except Exception as e:
        print("Une erreur s'est produite lors du chargement du dataset :", e)
        return None
    
    # Suppression des doublons
    original_shape = data.shape
    data.drop_duplicates(inplace=True)
    duplicates_removed = original_shape[0] - data.shape[0]
    if duplicates_removed > 0:
        print(f"\n{duplicates_removed} lignes en double ont été supprimées.")
    else:
        print("\nAucune ligne en double n'a été trouvée.")
        
        
    # Exploration des données
    print("Informations sur le dataset :")
    print(data.info())
    print("\nRésumé des données :")
    print(skim(data))
    print("\nValeurs manquantes :")
    print(data.isnull().sum())
    print("\nValeurs uniques par colonne :")
    print(data.nunique())
    
    # Copie du dataset original pour les modifications
    cleaned_data = data.copy()
    
    '''Identifiez et résolvez les problèmes potentiels
    (Exemples: mise en unité, normalisation des codes, détection des valeurs aberrantes)
    Standardisez le processus (Exemple: automatiser le nettoyage)
    
    Visualisation des données (Exemples: histogrammes, nuages de points, etc.)
    Effectuez une vérification orthographique (si applicable)
    Nettoyez les caractères non reconnus
    
    Uniformisation des données temporelles
    Uniformisation des données chiffrées
    
    Parsing (utilisation des expressions régulières pour extraire des informations)
    Transformation de données (par ex. extraction d'années à partir de dates)
    
    Renforcement des contraintes d'intégrité
    Méthode statistique (ex: détection et correction d'erreurs avec des méthodes statistiques)'''
    
    # Sauvegarde du dataset nettoyé
    output_folder = "df_clean"
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, "df_clean.csv")
    data.to_csv(output_path, index=False)
    print(f"\nDataset nettoyé sauvegardé sous : {output_path}")
    
    # Retourner le dataset nettoyé
    return data

# Exemple d'utilisation de la fonction
dataset_path = r"D:\Sky_Analytics\df_aeronef.csv"
data_nettoyee = autoclean_dataset(dataset_path)
if data_nettoyee is not None:
    print("\nTaille du dataset nettoyé :", data_nettoyee.shape)
    
    



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
schedule.every(10).seconds.do(current_time)
schedule.every(10).seconds.do(hello)

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