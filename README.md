# ProMGe— Prognozowanie produkcji energii z paneli fotowoltaicznych (Beta)

ProMGe to aplikacja napisana w Pythonie, która umożliwia prognozowanie produkcji energii elektrycznej w instalacji fotowoltaicznej na podstawie danych historycznych pogodowych oraz produkcyjnych. Projekt znajduje się w fazie beta.

Spis treści:
- Opis projektu
- Funkcjonalności
- Technologie
- Wymagania
- Instalacja
- Konfiguracja
- Użycie
- Testy
- Roadmap
- Znane problemy
- Licencja
- Autor

## Opis projektu

Aplikacja ProMGe pozwala na prognozowanie godzinowej produkcji energii elektrycznej z paneli fotowoltaicznych. Wykorzystuje modele uczenia maszynowego na podstawie danych historycznych o produkcji, danych meteorologicznych (np. nasłonecznienie, temperatura, zachmurzenie) oraz prognozy pogody, aby generować możliwie dokładnie produkcję.

Projekt powstaje z myślą o:
- właścicielach instalacji PV,
- firmach energetycznych,
- analitykach danych,
- osobach zainteresowanych modelowaniem produkcji energii.

## Funkcjonalności

- Import danych historycznych z plików CSV.
- Pobieranie danych pogodowych z API OpenMeteo.
- Trening modelu ML.
- Generowanie prognoz godzinowych.

## Technologie

| Obszar | Technologie |
|--------|-------------|
| Język | Python 3.10+ |
| ML | scikit-learn |
| Przetwarzanie danych | pandas, numpy, sqlalchemy |
| API pogodowe | requests |
| Inne | fastapi, pipenv |

## Wymagania

- Python 3.10 lub nowszy
- pipenv
- System: Linux / macOS / Windows

## Instalacja

git clone https://github.com/adamkwiatko/promge.git
cd promge

pipenv install
pipenv shell
uvicorn main:app --reload

## Konfiguracja

Utwórz plik .env:

API_URL_PSE=https://api.raporty.pse.pl/api/pk5l-wp
API_URL_METEO_HIST=https://archive-api.open-meteo.com/v1/archive
API_URL_METEO_FRCST=https://api.open-meteo.com/v1/forecast

## Użycie

Dostęp do GUI pod adresem podanym przez Uvicorn, np.:

INFO:     Uvicorn running on http://127.0.0.1:8000 

Dokumentacja API: http://127.0.0.1:8000/docs


## Przykładowe wyniki

|plan_dtime	|pv_output|
|---------------------|--------------|
|2026-01-30 01:00:00	|0.00|
|2026-01-30 02:00:00	|0.00|
|2026-01-30 03:00:00	|0.00|
|2026-01-30 04:00:00	|0.00|
|2026-01-30 05:00:00	|0.00|
|2026-01-30 06:00:00	|0.00|
|2026-01-30 07:00:00	|0.04|
|2026-01-30 08:00:00	|34.61|
|2026-01-30 09:00:00	|320.83|
|2026-01-30 10:00:00	|768.65|
|2026-01-30 11:00:00	|1442.48|


## Testy

Uruchamianie testów:

pytest tbd

## Roadmap

- [ ] Usuwanie rekordów z bazy
- [ ] Eksport wyników do pliku csv
- [ ] Zapis wyników do bazy
- [ ] Porównania wyników różnych modeli
- [ ] Porównanie z wykonaniem
- [ ] Automatyczne pobieranie danych z falownika
- [ ] Dodanie testów automatycznych

## Znane problemy

- Modele liniowe nie działają prawiłowo.
- Wersja beta — interfejs i struktura projektu mogą ulec zmianie.
- Brak obsługi danych z kilku lokalizacji jednocześnie.

## Licencja

Projekt dostępny na licencji MIT.

## Autor
Kontakt: adamkwiatko@proton.me
GitHub: https://github.com/adamkwiatko
