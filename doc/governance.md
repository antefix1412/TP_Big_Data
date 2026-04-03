# Gouvernance des donnees

## Sujet choisi
Analyse des stations de velos STAR a Rennes avec un pipeline de donnees complet combinant API et scraping web.

## Sources
- Source 1: API STAR Open Data
- Jeu de donnees: `vls-stations-etat-tr`
- Endpoint: `https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/vls-stations-etat-tr/records`
- Source 2: page meteo HTML pour Rennes
- URL de scraping: voir `.env.example`

## Ordre du pipeline
1. collecte via API
2. EDA sur brut
3. traitement Spark
4. chargement PostgreSQL
5. scraping meteo
6. visualisation

## Donnees collectees API
- identifiant de station
- nom de station
- velos disponibles
- places disponibles
- velos electriques disponibles
- etat de station
- terminal de paiement
- coordonnees
- horodatages

## Donnees scrapees
- temperature
- risque de pluie
- condition meteo
- resume meteo
- horodatage de scraping

## Qualite des donnees
- EDA dedie dans `notebook.py`
- traitement Spark dedie dans `src/traitement.py`
- suppression des lignes critiques sans identifiant ou nom
- imputation mediane sur numeriques
- encodage categoriel pour colonnes texte utiles
- stockage du scraping meme en cas d'erreur

## Stockage
- brut API: `data/raw/star_bikes_raw.parquet`
- brut scraping: `data/raw/weather_scraped_raw.parquet`
- EDA: `data/eda/`
- propre: `data/processed/star_bikes_weather.parquet`
- entrepot structure: PostgreSQL table `star_bikes_weather`

## Traçabilite
- collecte API: `src/get_api.py`
- EDA: `notebook.py`
- traitement: `src/traitement.py`
- chargement SQL: `src/load_postgres.py`
- scraping: `src/scrape_weather.py`
- visualisation: `dashboard/app.py`
