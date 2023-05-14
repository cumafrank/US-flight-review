import requests
from bs4 import BeautifulSoup

# Send a GET request to the URL
url = 'https://en.wikipedia.org/wiki/List_of_airlines_of_the_United_States'
response = requests.get(url)
print(response.content)

# Parse the HTML using Beautiful Soup
soup = BeautifulSoup(response.content, 'html.parser')

table = soup.select_one('table.wikitable.sortable.jquery-tablesorter')
tbody = table.select_one('tbody')

for row in tbody.select('tr'):
    cells = row.select('td')
    if cells:
        airline = cells[0].text.strip()
        iata = cells[1].text.strip()
        icao = cells[2].text.strip()
        callsign = cells[3].text.strip()
        hub = cells[4].text.strip()
        notes = cells[5].text.strip()
        print(airline, iata, icao, callsign, hub, notes)