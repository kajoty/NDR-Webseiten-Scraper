## NDR Radio Playlist Scraper

Ein einfacher Webscraper, der die Playlists von ndr.de herunterlädt und in einer Datenbank speichert. Nutzer können anschließend verschiedene Auswertungen auf die Daten durchführen.



# make virtual enviroment

```
python3 -m venv myenv
```

# activate

```
source myenv/bin/activate
```

# clone repositories

```
git clone https://github.com/kajoty/NDR-Webseiten-Scraper

cd NDR-Webseiten-Scraper
```

# install requirements

```
pip install -r requirements.txt
```

# config anpassen

```
nano config/config.json
```

"num_days": 50,  | 50 = Anzahl der Tage welche runtergeladen werden (es sind maximal 14 Tage möglich)


# starten

```
python3 abfrage.py
```

# Auswertung


```
python3 auswertungen.py
```