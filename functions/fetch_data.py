# fetch_data.py

import aiohttp

async def fetch_playlist(session, url):
    """
    Führt einen HTTP-GET-Request für die angegebene URL durch und gibt den Text der Antwort zurück.
    Etwaige Exceptions werden abgefangen und als None zurückgegeben.
    """
    try:
        async with session.get(url) as response:
            response.raise_for_status()
            return await response.text()
    except Exception as e:
        print(f"Error fetching {url}: {e}")
        return None
