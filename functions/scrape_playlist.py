import datetime
from bs4 import BeautifulSoup

def scrape_playlist(html, station_name):
    soup = BeautifulSoup(html, 'html.parser')
    programs = soup.find_all('li', class_='program')

    data = []
    for program in programs:
        time = program.find('strong', class_='time').text.strip()
        artist_title = program.find('h3').text.strip().split(' - ')

        if len(artist_title) != 2:
            continue  # Überspringen, wenn das Format nicht stimmt
        
        artist, title = artist_title
        title = title if title else "Unbekannt"
        date_str = datetime.datetime.now().strftime('%Y-%m-%d')

        data.append({
            "measurement": "music_playlist",
            "tags": {
                "station": station_name
            },
            "fields": {
                "date": date_str,
                "time": time,
                "artist": artist,
                "title": title
            }
        })
    return data
