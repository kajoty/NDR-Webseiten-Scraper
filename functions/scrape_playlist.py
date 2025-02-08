import datetime
from bs4 import BeautifulSoup

def scrape_playlist(html, station_name):
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        # Hole die Uhrzeit (nun umbenannt als 'played_time')
        played_time = program.find('strong', class_='time').text.strip()
        
        # Hole Künstler und Titel
        artist_title = program.find('h3').text.strip().split(' - ')

        if len(artist_title) != 2:
            continue  # Überspringen, wenn das Format nicht stimmt
        
        artist, title = artist_title
        title = title if title else "Unbekannt"
        
        # Das Datum, an dem der Titel gespielt wurde (heute)
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')
        
        # Der Index ist optional, könnte aber nützlich sein, falls du später eine eindeutige ID benötigst
        index = f"{station_name}_{datetime.datetime.now().strftime('%Y-%m-%dT%H:%M:%S')}"

        # Erstelle die InfluxDB-Daten
        data.append({
            "measurement": "music_playlist",
            "tags": {
                "station": station_name
            },
            "fields": {
                "index": index,                # Ein eindeutiger Index
                "played_date": date_str,        # Datum des Spielens
                "played_time": played_time,     # Uhrzeit des Spielens
                "artist": artist,               # Künstler
                "title": title                  # Titel
            }
        })
    return data
