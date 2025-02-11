from collections import Counter
import pandas as pd

def get_song_frequency_by_day(client, station_name):
    """
    Abfrage der Songhäufigkeit für eine bestimmte Station nach Datum.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station
    :return: Ein Counter-Objekt mit den Titel-Künstler-Kombinationen pro Datum
    """
    query = f'''
    SELECT "artist", "title", "played_date" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    '''
    
    try:
        result = client.query(query)
    except Exception as e:
        print(f"Fehler bei der Abfrage für Station {station_name}: {e}")
        return Counter()

    # Ergebnisse extrahieren
    songs = [(point['artist'], point['title'], point['played_date']) for point in result.get_points()]
    
    # Songs nach Datum und Titel-Künstler-Kombination gruppieren
    song_counter = Counter(songs)
    
    return song_counter

def print_most_frequent_songs_by_day(song_counter, station_name):
    """
    Gibt die häufigsten Songs pro Datum für eine Station aus.
    
    :param song_counter: Ein Counter-Objekt mit Titel-Künstler-Häufigkeiten pro Datum
    :param station_name: Der Name der Station
    """
    print(f"\nHäufigste Songs für {station_name} pro Tag:")
    for (artist, title, played_date), count in song_counter.items():
        print(f"Am {played_date}: {artist} - {title}: {count} mal gespielt")
        
def run_by_day():
    """
    Lädt die Konfiguration und führt die Analyse der Songhäufigkeiten nach Datum aus.
    """
    config = config_module.load_config()
    stations = config_module.load_stations()

    client = influxdb_module.initialize_influxdb(config)

    for station_name in stations:
        song_counter = get_song_frequency_by_day(client, station_name)
        print_most_frequent_songs_by_day(song_counter, station_name)

    client.close()
