import influxdb_module
import config_module
from collections import Counter

def get_titles_by_artist(client, artist_name):
    # InfluxDB-Abfrage: Alle Titel für den angegebenen Künstler abfragen
    query = f'''
    SELECT "title" FROM "music_playlist"
    WHERE "artist" = '{artist_name}'
    '''
    result = client.query(query)

    # Titel sammeln
    titles = [point['title'] for point in result.get_points()]
    return titles

def print_titles_by_artist(titles, artist_name):
    print(f"\nTitles by {artist_name}:")
    if titles:
        # Titel und deren Häufigkeit zählen
        title_counter = Counter(titles)
        for title, count in title_counter.items():
            print(f"- {title} (played {count} times)")
    else:
        print(f"No titles found for {artist_name}.")

def run():
    # Konfiguration und Stationen laden
    config = config_module.load_config()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Künstlername vom Benutzer abfragen
    artist_name = input("Enter artist name: ")

    # Titel für den Künstler abfragen und anzeigen
    titles = get_titles_by_artist(client, artist_name)
    print_titles_by_artist(titles, artist_name)

    # InfluxDB-Client schließen
    client.close()
