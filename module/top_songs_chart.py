import module.modul_config.influxdb_module as influxdb_module
import module.modul_config.config_module as config_module
import plotext as plt
from collections import Counter

def get_song_frequency(client, station_name):
    """
    Abfrage der meistgespielten Songs für eine bestimmte Station.
    """
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

    titles = [point['title'] for point in result.get_points()]
    return Counter(titles)

def plot_top_songs(title_counter, station_name, top_n=10):
    """
    Erstellt ein ASCII-Balkendiagramm für die meistgespielten Songs.
    """
    top_songs = title_counter.most_common(top_n)
    titles = [song[0] for song in top_songs]
    counts = [song[1] for song in top_songs]

    plt.bar(titles, counts)
    plt.title(f"Top {top_n} Songs - {station_name}")
    plt.xlabel("Song Title")
    plt.ylabel("Play Count")
    plt.show()

def run():
    """
    Lädt die Konfiguration und erstellt ein Diagramm für jede Station.
    """
    config = config_module.load_config()
    stations = config_module.load_stations()
    client = influxdb_module.initialize_influxdb(config)

    for station_name in stations:
        title_counter = get_song_frequency(client, station_name)
        plot_top_songs(title_counter, station_name)

    client.close()
