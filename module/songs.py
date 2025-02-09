import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module
from collections import Counter

def get_song_frequency(client, station_name):
    """
    Abfrage der Songhäufigkeit (Titel und Künstler) für eine bestimmte Station.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station, für die die Titelhäufigkeit abgefragt wird
    :return: Ein Counter-Objekt mit den Titel-Künstler-Kombinationen
    """
    # InfluxDB-Abfrage: Alle Titel und Künstler für die angegebene Station
    query = f'''
    SELECT "artist", "title" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    GROUP BY "title", "artist"
    '''
    
    try:
        result = client.query(query)
    except Exception as e:
        print(f"Fehler bei der Abfrage für Station {station_name}: {e}")
        return Counter()

    # Ergebnisse extrahieren und Häufigkeiten zählen
    songs = [(point['artist'], point['title']) for point in result.get_points()]
    song_counter = Counter(songs)

    return song_counter

def print_most_frequent_songs_for_station(song_counter, station_name, top_n=10):
    """
    Gibt die Top N der häufigsten Songs (Titel und Künstler) für eine bestimmte Station aus.
    
    :param song_counter: Ein Counter-Objekt mit Titel-Künstler-Häufigkeiten
    :param station_name: Der Name der Station
    :param top_n: Die Anzahl der zu zeigenden Top-Songs
    """
    print(f"\nTop {top_n} Songs for {station_name}:")
    # Die Songs nach Häufigkeit sortieren und die Top N ausgeben
    for (artist, title), count in song_counter.most_common(top_n):
        print(f"{artist} - {title}: {count} times")

def run():
    """
    Lädt die Konfiguration, die Stationen und führt die Analyse der Songhäufigkeiten aus.
    """
    # Konfiguration und Stationen laden
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Für jede Station die Titel-Künstler-Häufigkeit abrufen und ausgeben
    for station_name in stations:
        song_counter = get_song_frequency(client, station_name)
        print_most_frequent_songs_for_station(song_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
