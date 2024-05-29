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
    
    # Exploration des données
    print("Informations sur le dataset :")
    print(data.info())
    print("\nRésumé des données :")
    print(data.skim())
    print("\nValeurs manquantes :")
    print(data.isnull().sum())
    
    # Copie du dataset original pour les modifications
    cleaned_data = data.copy()
    
    # Nettoyage des données
    # Remplacer les valeurs manquantes par la moyenne (pour les colonnes numériques)
    for col in cleaned_data.select_dtypes(include=np.number):
        cleaned_data[col].fillna(cleaned_data[col].mean(), inplace=True)
    
    # Gestion des valeurs aberrantes (optionnel, à adapter en fonction du dataset)
    # Exemple: cleaned_data = nettoyer_valeurs_aberrantes(cleaned_data)
    
    # Prétraitement des données (optionnel, à adapter en fonction du dataset)
    # Exemple: cleaned_data = preprocess_data(cleaned_data)
    
    # Sauvegarde du dataset nettoyé
    output_folder = "df_clean"
    os.makedirs(output_folder, exist_ok=True)
    output_path = os.path.join(output_folder, "df_clean.csv")
    cleaned_data.to_csv(output_path, index=False)
    print(f"\nDataset nettoyé sauvegardé sous : {output_path}")
    
    # Retourner le dataset nettoyé
    return cleaned_data

# Exemple d'utilisation de la fonction
dataset_path = r"D:\Sky_Analytics\df_aeronef.csv"
data_nettoyee = autoclean_dataset(dataset_path)
if data_nettoyee is not None:
    print("\nTaille du dataset nettoyé :", data_nettoyee.shape)
