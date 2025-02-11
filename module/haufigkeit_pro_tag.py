import pandas as pd
from collections import Counter
import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module

def get_song_frequency_by_station_and_time_of_day(client, station_name):
    """
    Abfrage der Songhäufigkeit nach Station und Tageszeit.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station
    :return: Ein Counter-Objekt mit den Titel-Künstler-Kombinationen pro Tageszeit und Station
    """
    query = f'''
    SELECT "artist", "title", "played_time" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    '''
    
    try:
        result = client.query(query)
    except Exception as e:
        print(f"Fehler bei der Abfrage für Station {station_name}: {e}")
        return Counter()

    # Zuordnung der Zeitstempel in Tageszeiten
    def get_time_of_day(played_time):
        hour = int(played_time.split(":")[0])
        if 6 <= hour < 12:
            return 'Vormittags (6-12 Uhr)'
        elif 12 <= hour < 15:
            return 'Mittags (12-15 Uhr)'
        elif 15 <= hour < 18:
            return 'Nachmittags (15-18 Uhr)'
        elif 18 <= hour < 24:
            return 'Früherabends (18-00 Uhr)'
        else:
            return 'Nachts (00-6 Uhr)'

    # Songs nach Station und Tageszeit gruppieren
    song_counter = Counter()

    # Ergebnisse extrahieren und gruppieren
    for point in result.get_points():
        artist = point['artist']
        title = point['title']
        played_time = point['played_time']
        time_of_day = get_time_of_day(played_time)
        song_counter[(station_name, artist, title, time_of_day)] += 1

    return song_counter

def print_most_frequent_songs_by_station_and_time_of_day(song_counter):
    """
    Gibt die häufigsten Songs nach Station und Tageszeit aus.
    
    :param song_counter: Ein Counter-Objekt mit Titel-Künstler-Häufigkeiten pro Station und Tageszeit
    """
    print(f"\nHäufigste Songs nach Station und Tageszeit:")
    for (station_name, artist, title, time_of_day), count in song_counter.items():
        print(f"Station: {station_name} | Zeit: {time_of_day} | {artist} - {title}: {count} mal gespielt")

def run_by_station_and_time_of_day():
    """
    Lädt die Konfiguration und führt die Analyse der Songhäufigkeiten nach Station und Tageszeit aus.
    """
    # Konfiguration und Stationen laden
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Für jede Station die Titel-Künstler-Häufigkeit nach Station und Tageszeit abrufen und ausgeben
    song_counter = Counter()
    for station_name in stations:
        song_counter.update(get_song_frequency_by_station_and_time_of_day(client, station_name))
    
    print_most_frequent_songs_by_station_and_time_of_day(song_counter)

    # InfluxDB-Client schließen
    client.close()

def run():
    """
    Führt die Songhäufigkeit nach Station und Tageszeit Analyse aus.
    """
    run_by_station_and_time_of_day()

if __name__ == "__main__":
    run()
