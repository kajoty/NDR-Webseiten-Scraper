import hashlib
import datetime
from bs4 import BeautifulSoup

def generate_checksum(station_name, played_date, played_time, artist, title):
    """
    Erzeugt eine Checksumme basierend auf der Kombination von Station, Datum,
    Uhrzeit, Künstler und Titel.
    """
    combined_string = f"{station_name}_{played_date}_{played_time}_{artist}_{title}"
    
    # Erzeuge eine SHA256-Checksumme
    checksum = hashlib.sha256(combined_string.encode('utf-8')).hexdigest()
    return checksum

def scrape_playlist(html, station_name):
    """
    Extrahiert Playlist-Daten (Künstler, Titel, Uhrzeit) aus dem HTML und gibt sie
    als eine Liste von InfluxDB-Daten zurück.
    """
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        # Hole die Uhrzeit des Programms
        played_time = program.find('strong', class_='time').text.strip()
        
        # Hole Künstler und Titel
        artist_title = program.find('h3').text.strip().split(' - ')

        if len(artist_title) != 2:
            continue  # Überspringen, wenn das Format nicht stimmt
        
        artist, title = artist_title
        title = title if title else "Unbekannt"
        
        # Das Datum, an dem der Titel gespielt wurde (heute)
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # Generiere eine Checksumme aus der Station und den anderen relevanten Feldern
        checksum = generate_checksum(station_name, date_str, played_time, artist, title)

        # Erstelle die InfluxDB-Daten
        data.append({
            "measurement": "music_playlist",
            "tags": {
                "station": station_name  # Station als Tag
            },
            "fields": {
                "checksum": checksum,          # Checksumme als eindeutiger Identifier
                "played_date": date_str,       # Datum des Spielens
                "played_time": played_time,    # Uhrzeit des Spielens
                "artist": artist,              # Künstler
                "title": title                 # Titel
            }
        })
    return data
