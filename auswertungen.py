import os
import importlib
from module.modul_config.config_module import load_config  # Pfad zum Konfigurationsmodul
from module.modul_config.influxdb_module import initialize_influxdb


def load_modules_from_directory(directory):
    """
    Listet alle Python-Dateien im angegebenen Verzeichnis auf, die Module darstellen.
    :param directory: Das Verzeichnis, in dem die Module liegen.
    :return: Eine Liste der Module (Dateinamen ohne .py-Erweiterung).
    """
    modules = []
    for filename in os.listdir(directory):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # Entfernen der ".py"-Erweiterung
            modules.append(module_name)
    return modules


def main():
    """
    Lädt die Konfiguration, zeigt die verfügbaren Module und führt das ausgewählte Modul aus.
    """
    # Konfiguration laden
    config = load_config()

    # InfluxDB-Client initialisieren
    client = initialize_influxdb(config)
    
    # Verzeichnis für Module angeben
    module_directory = 'module'  # Standardverzeichnis für Module
    
    # Alle verfügbaren Module laden
    available_modules = load_modules_from_directory(module_directory)
    
    # Verfügbare Module anzeigen
    print("Verfügbare Module:")
    for idx, module in enumerate(available_modules, 1):
        print(f"{idx}. {module}")
    
    # Benutzerwunsch für Modul eingeben
    try:
        choice = int(input(f"Bitte ein Modul auswählen (1-{len(available_modules)}): "))
        if 1 <= choice <= len(available_modules):
            selected_module = available_modules[choice - 1]
            print(f"Lade Modul: {selected_module}...")

            # Dynamisch das ausgewählte Modul importieren
            module = importlib.import_module(f"module.{selected_module}")
            
            # Prüfen, ob das Modul eine 'run' Funktion hat und diese ausführen
            if hasattr(module, 'run'):
                module.run()
            else:
                print(f"Modul '{selected_module}' hat keine 'run' Funktion.")
        else:
            print("Ungültige Auswahl.")
    except ValueError:
        print("Bitte eine gültige Zahl eingeben.")

    # Skript endet hier, nachdem das ausgewählte Modul ausgeführt wurde


# Skript ausführen
if __name__ == "__main__":
    main()
