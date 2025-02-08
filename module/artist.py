import influxdb_module
import config_module

def run():
    config = config_module.load_config()
    stations = config_module.load_stations()

    # InfluxDB-Client initialisieren
    client = influxdb_module.initialize_influxdb(config)

    # Künstlerhäufigkeit für jede Station abrufen und ausgeben
    for station_name in stations:
        artist_counter = influxdb_module.get_artist_frequency(client, station_name)
        influxdb_module.print_most_frequent_artists_for_station(artist_counter, station_name)

    # InfluxDB-Client schließen
    client.close()
