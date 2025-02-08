import influxdb_module
import config_module
from datetime import datetime

def get_artist_by_time(client, station_name):
    query = f'''
    SELECT "artist", "time" FROM "music_playlist"
    WHERE "station" = '{station_name}'
    '''
    result = client.query(query)
    return [(point['time'], point['artist']) for point in result.get_points()]

def format_time(iso_time):
    dt_object = datetime.fromisoformat(iso_time.replace("Z", "+00:00"))
    return dt_object.strftime("%H:%M:%S")

def print_artist_time_analysis(artist_time_data):
    print("\nArtist Time Analysis:")
    if not artist_time_data:
        print("Keine Daten verfügbar.")
        return

    for time, artist in artist_time_data:
        formatted_time = format_time(time)
        print(f"{formatted_time}: {artist}")

def run():
    config = config_module.load_config()
    stations = config_module.load_stations()

    client = influxdb_module.initialize_influxdb(config)

    for station_name in stations:
        artist_time_data = get_artist_by_time(client, station_name)
        print(f"\nStation: {station_name}")
        print_artist_time_analysis(artist_time_data)

    client.close()
