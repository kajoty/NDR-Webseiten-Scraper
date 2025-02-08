import influxdb_module
import config_module
from collections import Counter

def get_song_frequency(client, station_name):
    # InfluxDB-Abfrage: Alle Titel für die angegebene Station
    query = f'''
    SELECT "title" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    GROUP BY "title"
    '''
    result = client.query(query)

    # Ergebnisse extrahieren und Häufigkeiten zählen
    titles = [point['title'] for point in result.get_points()]
    title_counter = Counter(titles)

    return title_counter

def print_most_frequent_songs_for_station(title_counter, station_name, top_n=10):
    print(f"\nTop {top_n} Songs for {station_name}:")
    # Die Titel nach Häufigkeit sortieren und die Top N ausgeben
    for title, count in title_counter.most_common(top_n):
        print(f"{title}: {count} times")

def run():
    # Konfiguration und Stationen laden
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Für jede Station die Titelhäufigkeit abrufen und ausgeben
    for station_name in stations:
        title_counter = get_song_frequency(client, station_name)
        print_most_frequent_songs_for_station(title_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
