# Projet Big Data - Velos STAR et meteo

## Vue d'ensemble
Ce projet construit un pipeline de donnees complet autour des stations de velos STAR a Rennes et de la meteo du jour.

Le pipeline fait 5 choses :
- recupere l'etat des stations via l'API STAR
- scrape la meteo a Rennes
- produit une petite analyse exploratoire
- transforme et enrichit les donnees
- affiche le resultat dans un dashboard Streamlit

Le projet peut etre lance :
- soit en une seule commande avec `run_pipeline.ps1`
- soit de maniere planifiee avec Airflow toutes les 2 minutes
- soit etape par etape si tu veux comprendre ou deboguer chaque partie

## Technologies utilisees
- Python 3.12
- Pandas
- BeautifulSoup / Requests
- PySpark
- PostgreSQL
- Streamlit
- Plotly
- Docker Compose
- Apache Airflow

## Structure du projet
```text
.
|-- dashboard/
|   `-- app.py
|-- airflow/
|   |-- dags/
|   |   `-- velostar_pipeline_dag.py
|   |-- logs/
|   `-- plugins/
|-- data/
|   |-- eda/
|   |-- processed/
|   `-- raw/
|-- doc/
|   `-- governance.md
|-- src/
|   |-- config.py
|   |-- get_api.py
|   |-- load_postgres.py
|   |-- scrape_weather.py
|   `-- traitement.py
|-- tests/
|   `-- test_app.py
|-- .env.example
|-- docker-compose.yml
|-- notebook.py
|-- README.md
|-- requirements.txt
|-- run_pipeline.ps1
`-- start_airflow.ps1
```

## Fonctionnement du pipeline
```text
1. API STAR
   src/get_api.py
   -> recupere les stations velos

2. Scraping meteo
   src/scrape_weather.py
   -> recupere temperature, pluie, condition meteo

3. EDA
   notebook.py
   -> produit un rapport exploratoire

4. Traitement Spark
   src/traitement.py
   -> nettoie, enrichit, fusionne velo + meteo

5. Chargement PostgreSQL
   src/load_postgres.py
   -> pousse le jeu de donnees final en base

6. Dashboard
   dashboard/app.py
   -> affiche les indicateurs et graphiques
```

## Sources de donnees

### API velo STAR
- Endpoint par defaut :
  `https://data.explore.star.fr/api/explore/v2.1/catalog/datasets/vls-stations-etat-tr/records`
- Utilisation :
  etat des stations, disponibilite des velos, bornes, coordonnees, statut

### Meteo
- Source configurable dans `.env`
- Le scraping actuel cible la page MeteoArt de Rennes
- Les champs recuperes sont :
  `temperature_c`, `rain_probability`, `weather_condition`

## Prerequis
Avant de lancer le projet, il faut idealement avoir :
- Windows PowerShell
- Python 3.12 installe
- `pip`
- Docker Desktop si tu veux utiliser PostgreSQL localement via Docker
- Java installe si tu veux executer la partie Spark dans de bonnes conditions

## 1. Cloner le projet
Dans un terminal PowerShell :

```powershell
git clone https://github.com/antefix1412/TP_Big_Data.git
cd "<nom-du-dossier>"
```

## 2. Creer l'environnement virtuel
```powershell
py -3.12 -m venv .venv
```

## 3. Activer l'environnement virtuel
```powershell
.venv\Scripts\Activate.ps1
```

## 4. Installer les dependances
```powershell
pip install -r requirements.txt
```

Les principales bibliotheques installees sont :
- `pandas`
- `requests`
- `beautifulsoup4`
- `pyarrow`
- `sqlalchemy`
- `pg8000`
- `pyspark`
- `streamlit`
- `plotly`

## 5. Configurer le projet
Copie le fichier d'exemple :

```powershell
Copy-Item .env.example .env
```

### Variables importantes dans `.env`
- `BIKE_API_URL` : endpoint API velo
- `BIKE_API_LIMIT` : nombre maximum d'enregistrements demandes
- `WEATHER_URL` : URL de la page meteo
- `WEATHER_CITY` : nom de la ville
- `RAW_DATA_DIR` : dossier des donnees brutes
- `PROCESSED_DATA_DIR` : dossier des donnees traitees
- `EDA_OUTPUT_DIR` : dossier du rapport EDA
- `POSTGRES_HOST` : hote PostgreSQL
- `POSTGRES_PORT` : port PostgreSQL
- `POSTGRES_DB` : base PostgreSQL
- `POSTGRES_USER` : utilisateur PostgreSQL
- `POSTGRES_PASSWORD` : mot de passe PostgreSQL

### Valeurs PostgreSQL par defaut
Le projet est deja configure pour fonctionner avec le conteneur Docker fourni :
- host : `localhost`
- port : `5433`
- database : `velostar_lakehouse`
- user : `postgres`
- password : `postgres`

## 6. Lancer PostgreSQL avec Docker
Si tu utilises Docker :

```powershell
docker compose up -d
```

Pour verifier que le conteneur tourne :

```powershell
docker ps
```

Le service defini dans [docker-compose.yml](docker-compose.yml) expose PostgreSQL sur `localhost:5433`.

## 7. Lancer tout le projet automatiquement
Le plus simple est d'utiliser le script PowerShell fourni :

```powershell
.\run_pipeline.ps1
```

Ce script :
- verifie que l'environnement virtuel existe
- cree `.env` depuis `.env.example` si besoin
- demarre PostgreSQL avec Docker si Docker est disponible
- lance la recuperation API
- lance le scraping meteo
- genere l'EDA
- execute le traitement Spark
- charge les donnees dans PostgreSQL
- lance le dashboard Streamlit

## 7.b Automatiser le pipeline avec Airflow
Une stack Airflow Docker est fournie pour relancer le pipeline ETL toutes les 2 minutes.

Le DAG execute :
- `src/get_api.py`
- `src/scrape_weather.py`
- `notebook.py`
- `src/traitement.py`
- `src/load_postgres.py`

Le planning Airflow est :
- `*/2 * * * *`

### Demarrer Airflow
```powershell
.\start_airflow.ps1
```

Puis ouvre :
- `http://localhost:8080`

Identifiants par defaut :
- utilisateur : `admin`
- mot de passe : `admin`

### Services ajoutes dans Docker Compose
- `airflow-postgres` : base metadata d'Airflow
- `airflow-init` : initialise la base et cree l'utilisateur admin
- `airflow-webserver` : interface web Airflow
- `airflow-scheduler` : planification et execution du DAG

### Important
- Le DAG Airflow recharge les donnees en base toutes les 2 minutes.
- Le dashboard Streamlit n'est pas redemarre toutes les 2 minutes, mais il lira les nouvelles donnees rechargees.
- Dans les conteneurs Airflow, PostgreSQL applicatif est resolu via `postgres:5432`, meme si sur ta machine il reste expose en `localhost:5433`.

## 8. Lancer le projet manuellement, etape par etape
Si tu preferes tout faire toi-meme :

### 8.1 Recuperer les donnees velo
```powershell
.\.venv\Scripts\python.exe src/get_api.py
```

Sortie attendue :
- fichier brut velo dans `data/raw/`

### 8.2 Recuperer la meteo
```powershell
.\.venv\Scripts\python.exe src/scrape_weather.py
```

Sortie attendue :
- fichier brut meteo dans `data/raw/`

### 8.3 Produire le rapport exploratoire
```powershell
.\.venv\Scripts\python.exe notebook.py
```

Sorties attendues :
- `data/eda/eda_report.md`
- eventuels graphiques EDA

### 8.4 Lancer le traitement Spark
```powershell
$env:PYSPARK_SUBMIT_ARGS='--conf spark.driver.extraJavaOptions="-Djava.security.manager=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED" --conf spark.executor.extraJavaOptions="-Djava.security.manager=allow --add-opens=java.base/java.lang=ALL-UNNAMED --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.io=ALL-UNNAMED --add-opens=java.base/sun.security.action=ALL-UNNAMED" pyspark-shell'
.\.venv\Scripts\python.exe src/traitement.py
```

Sortie attendue :
- `data/processed/star_bikes_weather.parquet`

### 8.5 Charger PostgreSQL
```powershell
.\.venv\Scripts\python.exe src/load_postgres.py
```

Sortie attendue :
- table `public.star_bikes_weather` remplie dans PostgreSQL

### 8.6 Lancer le dashboard
```powershell
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.headless true --browser.gatherUsageStats false
```

Ensuite ouvre :
- `http://localhost:8501`

Pour arreter le dashboard :
- `Ctrl + C`

## 9. Fichiers generes

### Donnees brutes
- `data/raw/star_bikes_raw.*`
- `data/raw/weather_scraped_raw.*`

### Donnees traitees
- `data/processed/star_bikes_weather.parquet`

### Rapport exploratoire
- `data/eda/eda_report.md`

### Base de donnees
- table PostgreSQL :
  `public.star_bikes_weather`

## 10. Ce que fait chaque script

### `src/get_api.py`
- appelle l'API STAR
- nettoie les enregistrements minimums
- sauvegarde les donnees brutes velo

### `src/scrape_weather.py`
- telecharge la page meteo du jour
- extrait la temperature, le risque de pluie et la condition meteo
- sauvegarde un snapshot brut meteo

### `notebook.py`
- inspecte les colonnes
- mesure les valeurs manquantes
- calcule des stats descriptives
- produit un rapport EDA

### `src/traitement.py`
- lit les donnees brutes velo + meteo
- nettoie les colonnes
- traite certaines valeurs aberrantes
- enrichit les stations avec la meteo
- exporte le dataset final

### `src/load_postgres.py`
- lit le parquet final
- cree la table si besoin
- vide la table
- recharge toutes les lignes

### `dashboard/app.py`
- lit les donnees depuis PostgreSQL si elles existent
- bascule sur le parquet local si la base est vide ou indisponible
- affiche les KPIs et les graphiques

## 11. Exemple de parcours complet
Si tu veux juste tester rapidement le projet sur une machine neuve :

```powershell
git clone <URL_DU_REPO>
cd "<nom-du-dossier>"
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
docker compose up -d
.\run_pipeline.ps1
```

Puis ouvre :
- `http://localhost:8501`

## 12. Depannage

### Le dashboard affiche "Aucune donnee disponible"
Verifier :
- que `data/processed/star_bikes_weather.parquet` existe
- que `src/traitement.py` a bien ete execute
- que PostgreSQL tourne si tu veux utiliser la base

Le dashboard sait maintenant basculer sur le parquet local si PostgreSQL est vide ou indisponible.

### Le scraping meteo ne remonte rien
Verifier :
- la connexion reseau
- la valeur `WEATHER_URL` dans `.env`
- que la page source n'a pas change de structure

Commande utile :

```powershell
.\.venv\Scripts\python.exe src/scrape_weather.py
```

### PostgreSQL ne demarre pas
Verifier :
- que Docker Desktop est bien lance
- que le port `5433` n'est pas deja pris

Commandes utiles :

```powershell
docker compose up -d
docker ps
```

### Spark pose probleme
Verifier :
- que Java est installe
- que tu utilises bien les options Java prevues dans `run_pipeline.ps1`

Si le script automatique marche mieux que l'execution manuelle, prefere :

```powershell
.\run_pipeline.ps1
```

## 13. Notes utiles
- Le dashboard peut fonctionner sans PostgreSQL si le parquet final est present.
- La meteo est un snapshot partage par toutes les stations au moment du scraping.
- Le graphe pluie dans le dashboard represente donc l'usage des stations sous un meme contexte meteo courant.

## 14. Tests unitaires

Les tests couvrent les fonctions pures du dashboard : `format_temperature`, `format_rain`, `usage_label` et `load_bike_data`.

### Lancer les tests
```powershell
python -m pytest tests/test_app.py -v
```

### Ce qui est teste

| Fonction | Cas couverts |
|---|---|
| `format_temperature` | valeur normale, zero, negatif, entier, NaN, None, pd.NA |
| `format_rain` | valeur normale, zero, 100%, float tronque, NaN, None, pd.NA |
| `usage_label` | chaque seuil exact (0.4 / 0.6 / 0.8), valeurs entre seuils, zero, valeur negative |
| `load_bike_data` | source PostgreSQL, fallback parquet sur erreur DB, fallback parquet sur DB vide, tri decroissant, colonnes retournees |

### Dependance requise
```powershell
pip install pytest
```

## 15. Commandes resumees

### Installation
```powershell
py -3.12 -m venv .venv
.venv\Scripts\Activate.ps1
pip install -r requirements.txt
Copy-Item .env.example .env
docker compose up -d
```

### Pipeline complet
```powershell
.\run_pipeline.ps1
```

### Airflow
```powershell
.\start_airflow.ps1
docker compose logs -f airflow-scheduler
docker compose down
```

### Pipeline manuel
```powershell
.\.venv\Scripts\python.exe src/get_api.py
.\.venv\Scripts\python.exe src/scrape_weather.py
.\.venv\Scripts\python.exe notebook.py
.\.venv\Scripts\python.exe src/traitement.py
.\.venv\Scripts\python.exe src/load_postgres.py
.\.venv\Scripts\python.exe -m streamlit run dashboard/app.py --server.headless true --browser.gatherUsageStats false
```
