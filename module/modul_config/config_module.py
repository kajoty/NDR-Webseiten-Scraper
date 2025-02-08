import json
import csv

# Konfiguration aus config.json laden
def load_config():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

# Stationen aus der CSV-Datei laden
def load_stations():
    stations = []
    with open('stations.csv', 'r') as stations_file:
        csv_reader = csv.DictReader(stations_file)
        for row in csv_reader:
            stations.append(row['Station'])
    return stations
