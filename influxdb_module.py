from influxdb import InfluxDBClient
from collections import Counter

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
