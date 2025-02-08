import os
import importlib
import config_module
import influxdb_module

def load_modules_from_directory(directory):
    # Alle Python-Dateien im Verzeichnis "directory" auflisten
    modules = []
    for filename in os.listdir(directory):
        if filename.endswith('.py') and filename != '__init__.py':
            module_name = filename[:-3]  # Entfernen der ".py"-Erweiterung
            modules.append(module_name)
    return modules

def main():
    # Verzeichnis für Module angeben
    module_directory = 'module'
    
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
            module = importlib.import_module(f"module.{selected_module}")
            
            # Hier können wir spezifische Funktionen des Moduls ausführen, z.B.:
            # Wenn das Modul eine Funktion 'run' hat, die ausgeführt werden soll:
            if hasattr(module, 'run'):
                module.run()
            else:
                print(f"Modul '{selected_module}' hat keine 'run' Funktion.")
        else:
            print("Ungültige Auswahl.")
    except ValueError:
        print("Bitte eine gültige Zahl eingeben.")

# Skript ausführen
if __name__ == "__main__":
    main()
