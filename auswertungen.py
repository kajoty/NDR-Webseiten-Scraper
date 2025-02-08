# auswertungen.py

import os
import importlib
from module.modul_config.config_module import load_config  # Pfad zum Konfigurationsmodul
from module.modul_config.influxdb_module import initialize_influxdb, get_artist_frequency, print_most_frequent_artists_for_station

def load_modules_from_directory(directory):
    # Alle Python-Dateien im Verzeichnis "directory" auflisten
    modules = []
    for filename in os.listdir(directory):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # Entfernen der ".py"-Erweiterung
            modules.append(module_name)
    return modules

def main():
    # Konfiguration laden
    config = load_config()
    
    # InfluxDB-Client initialisieren
    client = initialize_influxdb(config)

    # Verzeichnis für Module angeben
    module_directory = 'module/auswertungen'
    
    # Alle verfügbaren Module laden
    available_modules = load_modules_from_directory(module_directory)
    
    # Verfügbare Module anzeigen
    print("Verfügbare Module:")
    for idx, module in enumerate(available_modules, 1):
        print(f"{idx}. {module}")
    
    # Benutzerwunsch für Module eingeben
    try:
        choice = int(input(f"Bitte ein Modul auswählen (1-{len(available_modules)}): "))
        if 1 <= choice <= len(available_modules):
            selected_module = available_modules[choice - 1]
            print(f"Lade Modul: {selected_module}...")
            
            # Dynamisch das ausgewählte Modul importieren
            module = importlib.import_module(f"module.auswertungen.{selected_module}")
            
            # Hier kannst du spezifische Funktionen des Moduls ausführen, z.B.:
            if hasattr(module, 'run'):
                module.run()
            else:
                print(f"Modul '{selected_module}' hat keine 'run' Funktion.")
        else:
            print("Ungültige Auswahl.")
    except ValueError:
        print("Bitte eine gültige Zahl eingeben.")
    
    # Beispiel für Künstlerhäufigkeit abfragen und anzeigen
    station_name = input("Bitte den Namen der Station eingeben: ")
    artist_counter = get_artist_frequency(client, station_name)
    print_most_frequent_artists_for_station(artist_counter, station_name)

# Skript ausführen
if __name__ == "__main__":
    main()
