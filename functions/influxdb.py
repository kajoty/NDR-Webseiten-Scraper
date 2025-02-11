from influxdb import InfluxDBClient

# Funktion zur Initialisierung einer Verbindung zur InfluxDB
def initialize_influxdb(config):
    """
    Erstellt und gibt eine Verbindung zur InfluxDB zurück.
    Die Verbindungsdaten werden aus der übergebenen Konfiguration ausgelesen.
    """
    return InfluxDBClient(
        host=config['influx_host'],  # Hostname oder IP-Adresse des InfluxDB-Servers
        port=config['influx_port'],  # Port der InfluxDB
        username=config['influx_user'],  # Benutzername für die Authentifizierung
        password=config['influx_password'],  # Passwort für die Authentifizierung
        database=config['influx_db']  # Name der zu verwendenden Datenbank
    )

# Funktion zum Schreiben von Daten in die InfluxDB
def write_to_influxdb(client, data):
    """
    Überprüft, ob ein Datensatz bereits in der InfluxDB existiert, indem die Checksumme abgefragt wird.
    Falls der Datensatz noch nicht existiert, wird er in die Datenbank geschrieben.
    """
    try:
        for entry in data:
            checksum = entry['fields']['checksum']  # Extrahieren der Checksumme zur Duplikatsprüfung
            
            # Abfrage in der Datenbank, um zu prüfen, ob die Checksumme bereits existiert
            query = f'SELECT * FROM "music_playlist" WHERE "checksum" = \'{checksum}\' LIMIT 1'
            result = client.query(query)
            
            # Wenn kein Eintrag mit dieser Checksumme existiert, wird der Datensatz gespeichert
            if len(list(result.get_points())) == 0:
                client.write_points([entry])  # Schreiben des neuen Datensatzes in die Datenbank
                print(f"Daten wurden in InfluxDB für Checksumme {checksum} gespeichert.")
            else:
                print(f"Duplikat gefunden für Checksumme {checksum}, Eintrag wird übersprungen.")
                
    except Exception as e:
        print(f"Fehler beim Schreiben in die InfluxDB: {e}")
