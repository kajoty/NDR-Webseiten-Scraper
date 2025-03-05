import json
from influxdb import InfluxDBClient
from datetime import datetime

# Deine Konfiguration aus der config.json laden
def load_config():
    with open('config/config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

# Funktion zum Abrufen der letzten 300 Datensätze aus InfluxDB und als JSON speichern
def dump_last_300_records_to_json():
    # Lade Konfiguration
    config = load_config()

    # Verbindungsdaten für InfluxDB
    influxdb_url = config['influx_host']
    influxdb_user = config['influx_user']
    influxdb_password = config['influx_password']
    influxdb_name = config['influx_db']
    measurement_name = "music_playlist"  # Das Measurement, das du sichern möchtest

    # Erstelle eine Verbindung zur InfluxDB
    influx_client = InfluxDBClient(
        host=influxdb_url,
        port=config['influx_port'],
        username=influxdb_user,
        password=influxdb_password,
        database=influxdb_name
    )

    # Abfrage der letzten 300 Datensätze aus InfluxDB
    query = f"SELECT * FROM \"{measurement_name}\" ORDER BY time DESC LIMIT 300"
    result = influx_client.query(query)

    # Wenn keine Daten vorhanden sind, breche die Migration ab
    if not result:
        print("Keine Daten gefunden, Migration abgebrochen.")
        return

    # Daten aus InfluxDB extrahieren
    points = list(result.get_points(measurement=measurement_name))

    # Speichern der Daten als JSON-Datei
    with open('last_300_records.json', 'w') as json_file:
        json.dump(points, json_file, indent=4)
    
    print(f"Dump der letzten 300 Datensätze als JSON abgeschlossen.")

if __name__ == "__main__":
    dump_last_300_records_to_json()
