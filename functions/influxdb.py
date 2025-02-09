from influxdb import InfluxDBClient

def initialize_influxdb(config):
    """
    Initialisiert und gibt eine InfluxDBClient-Verbindung zurück.
    """
    return InfluxDBClient(
        host=config['influx_host'],
        port=config['influx_port'],
        username=config['influx_user'],
        password=config['influx_password'],
        database=config['influx_db']
    )

def write_to_influxdb(client, data):
    """
    Überprüft, ob der Datensatz bereits in der InfluxDB existiert, basierend auf der Checksumme.
    Wenn der Datensatz nicht existiert, wird er in die InfluxDB geschrieben.
    """
    try:
        for entry in data:
            checksum = entry['fields']['checksum']  # Holen des Checksumme-Werts zur Duplikatprüfung
            
            # Prüfen, ob der Checksumme-Wert bereits existiert (Abfrage auf vorhandene Daten)
            query = f'SELECT * FROM "music_playlist" WHERE "checksum" = \'{checksum}\' LIMIT 1'
            result = client.query(query)
            
            # Wenn das Ergebnis leer ist, können wir den Datensatz schreiben
            if len(list(result.get_points())) == 0:
                client.write_points([entry])
                print(f"Data written to InfluxDB for checksum {checksum}")
            else:
                print(f"Duplicate found for checksum {checksum}, skipping write.")
                
    except Exception as e:
        print(f"Error writing to InfluxDB: {e}")
