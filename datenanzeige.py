import json
from influxdb import InfluxDBClient

# Lade Konfiguration aus config.json
def load_config():
    with open('config.json', 'r') as config_file:
        config = json.load(config_file)
    return config

# InfluxDB-Client initialisieren
def initialize_influxdb(config):
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_user'],
        password=config['influx_password'],
        database=config['influx_db']
    )

# Abfrage der letzten X Einträge aus der InfluxDB
def get_last_entries(client, num_entries):
    query = f'SELECT * FROM "music_playlist" ORDER BY time DESC LIMIT {num_entries}'
    result = client.query(query)
    return result.get_points()

# Ausgabe der Einträge
def display_entries(entries):
    for entry in entries:
        print(f"Time: {entry['time']}, Station: {entry['station']}, Artist: {entry['artist']}, Title: {entry['title']}")

# Hauptfunktion
def main():
    # Konfiguration laden
    config = load_config()

    # Anzahl der Einträge aus config.json holen
    num_entries = config.get('num_entries', 20)  # Standardwert: 20, wenn nicht in config.json gesetzt

    # InfluxDB-Client initialisieren
    client = initialize_influxdb(config)

    # Letzte Einträge abrufen
    entries = get_last_entries(client, num_entries)

    # Einträge anzeigen
    display_entries(entries)

    # InfluxDB-Client schließen
    client.close()

# Skript ausführen
if __name__ == "__main__":
    main()
