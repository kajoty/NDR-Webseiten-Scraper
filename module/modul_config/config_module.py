# config_module.py

import json
import csv
import os

# Funktion zum Laden der Konfiguration aus der config.json
def load_config():
    try:
        # Den absoluten Pfad zur Konfigurationsdatei ermitteln
        config_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../config/config.json'))
        print(f"Versuche, Konfigurationsdatei zu laden von: {config_path}")
        
        with open(config_path, 'r') as config_file:
            config = json.load(config_file)
        
        return config
    except Exception as e:
        print(f"Fehler beim Laden der Konfigurationsdatei: {e}")
        return None

# Funktion zum Laden der Stations aus der stations.csv
def load_stations():
    stations = []
    
    try:
        # Den absoluten Pfad zur Stationsdatei ermitteln
        stations_path = os.path.abspath(os.path.join(os.path.dirname(__file__), '../../stations.csv'))
        print(f"Versuche, Stations-Datei zu laden von: {stations_path}")
        
        with open(stations_path, 'r') as stations_file:
            csv_reader = csv.DictReader(stations_file)
            for row in csv_reader:
                stations.append(row['Station'])
        
        return stations
    except Exception as e:
        print(f"Fehler beim Laden der Stationsdatei: {e}")
        return []
