import json
from influxdb import InfluxDBClient
from collections import Counter
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

# InfluxDB-Client initialisieren
def initialize_influxdb(config):
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_user'],
        password=config['influx_password'],
        database=config['influx_db']
    )

# Künstlerhäufigkeit für eine bestimmte Station abfragen
def get_artist_frequency(client, station_name):
    # InfluxDB-Abfrage: Alle Künstler für die angegebene Station
    query = f'''
    SELECT "artist" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    GROUP BY "artist"
    '''
    result = client.query(query)

    # Ergebnisse extrahieren und Häufigkeiten zählen
    artists = [point['artist'] for point in result.get_points()]
    artist_counter = Counter(artists)

    return artist_counter

# Ausgabe der häufigsten Künstler für eine Station
def print_most_frequent_artists_for_station(artist_counter, station_name, top_n=10):
    print(f"\nTop {top_n} Artists for {station_name}:")
    # Die Künstler nach Häufigkeit sortieren und die Top N ausgeben
    for artist, count in artist_counter.most_common(top_n):
        print(f"{artist}: {count} times")

# Hauptfunktion
def main():
    # Konfiguration und Stationen laden
    config = load_config()
    stations = load_stations()

    # InfluxDB-Client initialisieren
    client = initialize_influxdb(config)

    # Für jede Station die Künstlerhäufigkeit abrufen und ausgeben
    for station_name in stations:
        artist_counter = get_artist_frequency(client, station_name)
        print_most_frequent_artists_for_station(artist_counter, station_name)

    # InfluxDB-Client schließen
    client.close()

# Skript ausführen
if __name__ == "__main__":
    main()
