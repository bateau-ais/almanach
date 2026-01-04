import os
import pandas as pd
from read_ais.parseur import AIS_data


input_file = "/media/elou/windows/Users/Elouarn/Documents/Lycee/SEMESTRE_9_5A/Projet_5A/Donnees_AIS/AIS_UNACORN_Seatracks_past12-hours.csv"

def read_ais_frames(file_path: str):
    """
    Lit un fichier CSV contenant des données AIS et retourne un générateur
    d'objets AIS_data représentant les trames AIS une par une.
    """
    if os.path.isfile(file_path) and file_path.endswith('.csv'):  # vérification si le fichier existe et est un CSV
        data = pd.read_csv(file_path)
        data.columns = data.columns.str.strip()  # Supprimer les espaces autour des noms de colonnes s'il y en a
        
        print("Colonnes disponibles :", list(data.columns))
        
        for _, row in data.iterrows():
            ais = AIS_data()
            ais.parse_from_csv_raw(row)
            yield ais
    
    else:
        raise ValueError(f"Le fichier {file_path} n'existe pas ou n'est pas un fichier CSV.")


if __name__ == "__main__":
    # Exemple d'utilisation : récupérer les 5 premières trames
    frames_generator = read_ais_frames(input_file)
    for i, frame in enumerate(frames_generator):
        if i >= 5:
            break
        print(vars(frame))
