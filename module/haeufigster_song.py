import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module
from collections import Counter

def get_title_frequency(client, station_name):
    """
    Abfrage der Songhäufigkeit (nur Titel) für eine bestimmte Station.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station, für die die Titelhäufigkeit abgefragt wird
    :return: Ein Counter-Objekt mit den Titelhäufigkeiten
    """
    # InfluxDB-Abfrage: Alle Titel für die angegebene Station
    query = f'''
    SELECT "title" FROM "music_playlist" 
    WHERE "station" = '{station_name}' 
    GROUP BY "title"
    '''
    
    try:
        result = client.query(query)
    except Exception as e:
        print(f"Fehler bei der Abfrage für Station {station_name}: {e}")
        return Counter()

    # Ergebnisse extrahieren und Häufigkeiten zählen
    titles = [point['title'] for point in result.get_points()]
    title_counter = Counter(titles)

    return title_counter

def print_most_frequent_titles_for_station(title_counter, station_name, top_n=10):
    """
    Gibt die Top N der häufigsten Songs (nur Titel) für eine bestimmte Station aus.
    
    :param title_counter: Ein Counter-Objekt mit Titelhäufigkeiten
    :param station_name: Der Name der Station
    :param top_n: Die Anzahl der zu zeigenden Top-Songs
    """
    print(f"\nTop {top_n} Songs for {station_name}:")
    # Die Titel nach Häufigkeit sortieren und die Top N ausgeben
    for title, count in title_counter.most_common(top_n):
        print(f"{title}: {count} times")

def run():
    """
    Lädt die Konfiguration, die Stationen und führt die Analyse der Songhäufigkeiten aus.
    """
    # Konfiguration und Stationen laden
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Für jede Station die Titelhäufigkeit abrufen und ausgeben
    for station_name in stations:
        title_counter = get_title_frequency(client, station_name)
        print_most_frequent_titles_for_station(title_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
