import influxdb_module
import config_module
from collections import Counter

def get_station_count(client):
    # InfluxDB-Abfrage: Alle Stationen als Gruppen zählen
    query = '''
    SELECT COUNT("artist") FROM "music_playlist"
    GROUP BY "station"
    '''
    result = client.query(query)

    # Stationen sammeln und deren Einträge zählen
    station_counter = Counter()
    for station_group in result.raw.get('series', []):
        station_name = station_group['tags']['station']
        count = station_group['values'][0][1]  # count("artist") ist das zweite Element
        station_counter[station_name] = count

    return station_counter

def print_station_counts(station_counter):
    print(f"\nStation Counts:")
    for station, count in station_counter.items():
        print(f"{station}: {count} entries")

def run():
    # Konfiguration und Stationen laden
    config = config_module.load_config()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Stationen zählen und ausgeben
    station_counter = get_station_count(client)
    print_station_counts(station_counter)

    # InfluxDB-Client schließen
    client.close()
