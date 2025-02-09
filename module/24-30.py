import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module  # Hier den Import hinzufügen
from collections import Counter
from datetime import datetime, timedelta

def get_song_frequency(client, station_name, time_range='24h'):
    """
    Abfrage der Songhäufigkeit für eine bestimmte Station innerhalb eines Zeitrahmens.
    
    :param client: Der InfluxDB-Client
    :param station_name: Der Name der Station, für die die Titelhäufigkeit abgefragt wird
    :param time_range: Der Zeitrahmen für die Abfrage (z.B. '24h' oder '30d')
    :return: Ein Counter-Objekt mit den Titelhäufigkeiten
    """
    # Berechne das Startdatum basierend auf dem Zeitrahmen
    if time_range == '24h':
        start_time = datetime.now() - timedelta(days=1)
    elif time_range == '30d':
        start_time = datetime.now() - timedelta(days=30)
    else:
        raise ValueError("Unsupported time range. Use '24h' or '30d'.")

    # Konvertiere das Startdatum in das richtige Format für InfluxDB
    start_time_str = start_time.strftime('%Y-%m-%dT%H:%M:%SZ')

    # InfluxDB-Abfrage: Alle Titel für die angegebene Station im gewünschten Zeitraum
    query = f'''
    SELECT "title" FROM "music_playlist" 
    WHERE "station" = '{station_name}' AND "played_date" >= '{start_time_str}' 
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

def print_most_frequent_songs_for_station(title_counter, station_name, top_n=10):
    """
    Gibt die Top N der häufigsten Songs für eine bestimmte Station aus.
    
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
    config = config_module.load_config()  # Lade die Konfiguration
    stations = config_module.load_stations()  # Lade die Stationen

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Benutzer wird aufgefordert, einen Zeitraum auszuwählen: 1 für 24h oder 2 für 30d
    print("Wählen Sie einen Zeitraum:")
    print("1. Letzte 24 Stunden")
    print("2. Letzte 30 Tage")
    try:
        choice = int(input("Geben Sie 1 oder 2 ein: ").strip())
        if choice == 1:
            time_range = '24h'
        elif choice == 2:
            time_range = '30d'
        else:
            print("Ungültige Auswahl. Standardmäßig wird '24h' verwendet.")
            time_range = '24h'
    except ValueError:
        print("Ungültige Eingabe. Standardmäßig wird '24h' verwendet.")
        time_range = '24h'

    # Für jede Station die Titelhäufigkeit im angegebenen Zeitraum abrufen und ausgeben
    for station_name in stations:
        title_counter = get_song_frequency(client, station_name, time_range)
        print_most_frequent_songs_for_station(title_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
