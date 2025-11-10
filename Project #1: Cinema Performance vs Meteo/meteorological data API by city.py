import requests
import pandas as pd
import time
import os
import urllib3
from datetime import datetime

# Disabilita warning SSL
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Anni da includere (escludiamo 2020 e 2021)
anni_validi = [y for y in range(2015, 2025) if y not in [2020, 2021]]

# Cartella di salvataggio e nome file
save_path = r"C:\Users\mattia.avorio\OneDrive - ODEON Cinemas Group\Documenti\Dashboards\Analisi di correlazione-meteo\API Meteo\DB meteo storico"
file_name = "Meteo_UCI_Storico_Filtrato.xlsx"
full_path = os.path.join(save_path, file_name)

# Codici meteo Open-Meteo
weather_map = {
    0: "Soleggiato", 1: "Prevalentemente sereno", 2: "Parzialmente nuvoloso",
    3: "Coperto", 45: "Nebbia", 48: "Nebbia con brina",
    51: "Pioviggine leggera", 53: "Pioviggine moderata", 55: "Pioviggine intensa",
    56: "Pioviggine gelata leggera", 57: "Pioviggine gelata intensa",
    61: "Pioggia leggera", 63: "Pioggia moderata", 65: "Pioggia intensa",
    66: "Pioggia gelata leggera", 67: "Pioggia gelata intensa",
    71: "Neve leggera", 73: "Neve moderata", 75: "Neve intensa",
    77: "Cristalli di ghiaccio", 80: "Rovesci leggeri", 81: "Rovesci moderati",
    82: "Rovesci forti", 85: "Rovesci di neve leggeri", 86: "Rovesci di neve forti",
    95: "Temporale", 96: "Temporale con grandine leggera", 99: "Temporale con grandine forte"
}

# Coordinate delle città (inserite in precedenza)
citta_uci = {
    "Alessandria": (44.9122, 8.6150),
    "Ancona": (43.6158, 13.5189),
    "Arezzo": (43.4631, 11.8797),
    "Bergamo": (45.6983, 9.6773),
    "Bicocca": (45.5110, 9.2130),
    "Cagliari": (39.2238, 9.1217),
    "Campi Bisenzio": (43.8250, 11.1090),
    "Casalecchio": (44.4770, 11.2830),
    "Catania": (37.5079, 15.0830),
    "Certosa": (45.5060, 9.1340),
    "Fano": (43.8420, 13.0190),
    "Ferrara": (44.8350, 11.6190),
    "Fiori": (45.4020, 9.1420),
    "Firenze": (43.7696, 11.2558),
    "Fiumara": (44.4230, 8.8890),
    "Fiume Veneto": (45.8700, 12.7500),
    "Fiumicino": (41.7700, 12.2300),
    "Gloria": (41.7700, 12.2300),
    "Gualtieri": (44.6980, 10.6310),
    "Jesi": (43.5220, 13.2430),
    "Lissone": (45.6100, 9.2430),
    "Marcon": (45.5580, 12.2980),
    "Messina": (38.1938, 15.5540),
    "Mestre": (45.4930, 12.2460),
    "Molfetta": (41.2000, 16.6000),
    "Moncallieri": (44.9990, 7.6820),
    "Montano Lucino": (45.7800, 9.0200),
    "maximo": (41.7700, 12.2300),
    "Napoli": (40.8522, 14.2681),
    "Palariviera": (44.6980, 10.6310),
    "Palermo": (38.1157, 13.3615),
    "Perugia": (43.1107, 12.3908),
    "Pesaro": (43.9090, 12.9140),
    "Piacenza": (45.0522, 9.6920),
    "Pioltello": (45.5000, 9.3330),
    "Porta di Roma": (41.7700, 12.2300),
    "Porto SantElpidio": (43.2500, 13.7500),
    "Reggio Emilia": (44.6980, 10.6310),
    "Rimini": (44.0600, 12.5650),
    "Roma Lunghezza": (41.7700, 12.2300),
    "Senigallia": (43.7180, 13.2140),
    "Sinalunga": (43.1253, 11.4442),
    "Torino Lingotto": (45.0300, 7.6700),
    "Verona": (45.4384, 10.9916),
    "Viale Marconi": (41.7700, 12.2300),
    "Villesse": (45.8800, 13.4600),
    "Vicenza": (45.5467, 11.5475),
    "Marcianise": (41.0160, 14.3000),
    "Bolzano": (46.4983, 11.3548),
    "Bari": (41.1171, 16.8719),
    "Matera": (40.6663, 16.6040),
    "Orio": (45.6680, 9.7000),
    "Gioia del colle": (40.8000, 16.9330),
    "Megalò": (42.2053, 14.0953)
}

# DataFrame aggregato
df_finale = pd.DataFrame()

# Scarico per ogni città e anno
for citta, (lat, lon) in citta_uci.items():
    print(f"\n📍 Elaborazione: {citta}")
    for anno in anni_validi:
        try:
            start = f"{anno}-01-01"
            end = f"{anno}-12-31"
            url = "https://archive-api.open-meteo.com/v1/archive"
            params = {
                "latitude": lat,
                "longitude": lon,
                "start_date": start,
                "end_date": end,
                "daily": "weathercode",
                "timezone": "Europe/Berlin"
            }

            response = requests.get(url, params=params, verify=False)
            response.raise_for_status()
            data = response.json()

            df = pd.DataFrame(data['daily'])
            df['Data'] = pd.to_datetime(df['time']).dt.date
            df['Codice Meteo'] = df['weathercode']
            df['Condizione Meteo'] = df['Codice Meteo'].map(weather_map).fillna("Altro")
            df['Città'] = citta

            df = df[['Città', 'Data', 'Condizione Meteo', 'Codice Meteo']]
            df_finale = pd.concat([df_finale, df], ignore_index=True)
            time.sleep(1)

        except Exception as e:
            print(f"❌ Errore {citta} {anno}: {e}")
            continue

# Salvataggio
df_finale.to_excel(full_path, index=False)
print(f"\n✅ File Excel creato con successo in:\n{full_path}")
