import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module
from collections import Counter

def get_artist_frequency(client, station_name):
    """
    Abfrage der Künstlerhäufigkeit für eine bestimmte Station.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station, für die die Künstlerhäufigkeit abgefragt wird
    :return: Ein Counter-Objekt mit den Künstlerhäufigkeiten
    """
    # InfluxDB-Abfrage: Alle Künstler für die angegebene Station
    query = f'''
    SELECT "artist" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    GROUP BY "artist"
    '''
    
    try:
        result = client.query(query)
    except Exception as e:
        print(f"Fehler bei der Abfrage für Station {station_name}: {e}")
        return Counter()

    # Ergebnisse extrahieren und Häufigkeiten zählen
    artists = [point['artist'] for point in result.get_points()]
    artist_counter = Counter(artists)

    return artist_counter

def print_most_frequent_artists_for_station(artist_counter, station_name, top_n=10):
    """
    Gibt die Top N der häufigsten Künstler für eine bestimmte Station aus.
    
    :param artist_counter: Ein Counter-Objekt mit Künstlerhäufigkeiten
    :param station_name: Der Name der Station
    :param top_n: Die Anzahl der zu zeigenden Top-Künstler
    """
    print(f"\nTop {top_n} Artists for {station_name}:")
    # Die Künstler nach Häufigkeit sortieren und die Top N ausgeben
    for artist, count in artist_counter.most_common(top_n):
        print(f"{artist}: {count} times")

def run():
    """
    Lädt die Konfiguration, die Stationen und führt die Analyse der Künstlerhäufigkeiten aus.
    """
    # Konfiguration und Stationen laden
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Für jede Station die Künstlerhäufigkeit abrufen und ausgeben
    for station_name in stations:
        artist_counter = get_artist_frequency(client, station_name)
        print_most_frequent_artists_for_station(artist_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
