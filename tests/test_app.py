from __future__ import annotations

import os
import sys
import types
import unittest
from unittest.mock import MagicMock, patch

import pandas as pd

# ---------------------------------------------------------------------------
# Ajoute les répertoires nécessaires au path Python
# ---------------------------------------------------------------------------
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
DASHBOARD_DIR = os.path.join(BASE_DIR, "dashboard")
for _p in (BASE_DIR, DASHBOARD_DIR):
    if _p not in sys.path:
        sys.path.insert(0, _p)

# ---------------------------------------------------------------------------
# Mock streamlit AVANT l'import de app
# ---------------------------------------------------------------------------
_st = types.ModuleType("streamlit")
_st.set_page_config = MagicMock()
_st.markdown = MagicMock()
_st.write = MagicMock()
_st.warning = MagicMock()
_st.stop = MagicMock()
_st.metric = MagicMock()
_st.plotly_chart = MagicMock()

# multiselect renvoie toujours la valeur `default` pour que filtered ne soit pas vide
def _multiselect(label, options, default=None, **kwargs):
    return default if default is not None else list(options)

_st.multiselect = _multiselect

# columns(n) renvoie une liste de n MagicMocks dépaquetables
def _columns(n_or_spec):
    n = n_or_spec if isinstance(n_or_spec, int) else len(n_or_spec)
    cols = [MagicMock() for _ in range(n)]
    # Chaque colonne expose metric / plotly_chart
    for c in cols:
        c.metric = MagicMock()
        c.plotly_chart = MagicMock()
    return cols

_st.columns = _columns

sys.modules["streamlit"] = _st

# Mock plotly.express
_px = types.ModuleType("plotly.express")
_chart_mock = MagicMock(return_value=MagicMock())
_px.bar = _chart_mock
_px.pie = _chart_mock
_px.scatter = _chart_mock
sys.modules["plotly"] = types.ModuleType("plotly")
sys.modules["plotly.express"] = _px

# Mock sqlalchemy — create_engine lève une exception pour forcer le fallback parquet
_sqla = types.ModuleType("sqlalchemy")
_sqla.create_engine = MagicMock(side_effect=Exception("DB indisponible"))
sys.modules["sqlalchemy"] = _sqla

# Mock src.config
_src = types.ModuleType("src")
_config_mod = types.ModuleType("src.config")
_settings = MagicMock()
_settings.postgres_url = "postgresql+pg8000://u:p@localhost:5432/db"
_settings.postgres_schema = "public"
_settings.postgres_table = "star_bikes_weather"
_settings.processed_file_path = "/tmp/fake.parquet"
_config_mod.settings = _settings
_src.config = _config_mod
sys.modules["src"] = _src
sys.modules["src.config"] = _config_mod

# ---------------------------------------------------------------------------
# DataFrame minimal utilisé comme retour par défaut de pd.read_parquet
# ---------------------------------------------------------------------------
_COLS = [
    "station_id", "station_name", "available_bikes", "available_docks",
    "available_electric_bikes", "total_capacity", "bikes_used_proxy",
    "usage_rate", "station_status", "payment_terminal", "city",
    "temperature_c", "rain_probability", "weather_condition",
    "scrape_status", "record_timestamp", "collection_timestamp",
]
_DUMMY_ROW = dict.fromkeys(_COLS, None)
_DUMMY_ROW.update({
    "station_id": 1, "station_name": "TestStation",
    "available_bikes": 5, "available_docks": 10,
    "available_electric_bikes": 1, "total_capacity": 15,
    "bikes_used_proxy": 3, "usage_rate": 0.5,
    "station_status": "OK", "payment_terminal": True,
    "city": "Rennes", "temperature_c": 14.0,
    "rain_probability": 20, "weather_condition": "Nuageux",
    "scrape_status": "ok",
})
_DUMMY_DF = pd.DataFrame([_DUMMY_ROW], columns=_COLS)

# Import du module : on mock pd.read_parquet pour le call top-level
with patch("pandas.read_parquet", return_value=_DUMMY_DF):
    import app  # noqa: E402

from app import format_rain, format_temperature, load_bike_data, usage_label  # noqa: E402


# ===========================================================================
# Tests : format_temperature
# ===========================================================================
class TestFormatTemperature(unittest.TestCase):

    def test_valeur_normale(self):
        self.assertEqual(format_temperature(18.567), "18.6 C")

    def test_zero(self):
        self.assertEqual(format_temperature(0), "0.0 C")

    def test_negatif(self):
        self.assertEqual(format_temperature(-5.0), "-5.0 C")

    def test_entier(self):
        self.assertEqual(format_temperature(22), "22.0 C")

    def test_nan_retourne_na(self):
        self.assertEqual(format_temperature(float("nan")), "N/A")

    def test_none_retourne_na(self):
        self.assertEqual(format_temperature(None), "N/A")

    def test_pd_na_retourne_na(self):
        self.assertEqual(format_temperature(pd.NA), "N/A")


# ===========================================================================
# Tests : format_rain
# ===========================================================================
class TestFormatRain(unittest.TestCase):

    def test_valeur_normale(self):
        self.assertEqual(format_rain(45), "45%")

    def test_zero(self):
        self.assertEqual(format_rain(0), "0%")

    def test_cent(self):
        self.assertEqual(format_rain(100), "100%")

    def test_float_tronque(self):
        # int() tronque : 72.9 -> 72
        self.assertEqual(format_rain(72.9), "72%")

    def test_nan_retourne_na(self):
        self.assertEqual(format_rain(float("nan")), "N/A")

    def test_none_retourne_na(self):
        self.assertEqual(format_rain(None), "N/A")

    def test_pd_na_retourne_na(self):
        self.assertEqual(format_rain(pd.NA), "N/A")


# ===========================================================================
# Tests : usage_label
# ===========================================================================
class TestUsageLabel(unittest.TestCase):

    def test_tres_forte_seuil_exact(self):
        self.assertEqual(usage_label(0.8), "Tres forte")

    def test_tres_forte_au_dessus(self):
        self.assertEqual(usage_label(1.0), "Tres forte")

    def test_forte_seuil_exact(self):
        self.assertEqual(usage_label(0.6), "Forte")

    def test_forte_juste_sous_tres_forte(self):
        self.assertEqual(usage_label(0.799), "Forte")

    def test_moyenne_seuil_exact(self):
        self.assertEqual(usage_label(0.4), "Moyenne")

    def test_moyenne_juste_sous_forte(self):
        self.assertEqual(usage_label(0.599), "Moyenne")

    def test_faible_juste_sous_moyenne(self):
        self.assertEqual(usage_label(0.399), "Faible")

    def test_faible_zero(self):
        self.assertEqual(usage_label(0.0), "Faible")

    def test_faible_valeur_negative(self):
        # Valeur anormale : doit rester "Faible"
        self.assertEqual(usage_label(-0.1), "Faible")


# ===========================================================================
# Tests : load_bike_data
# ===========================================================================
class TestLoadBikeData(unittest.TestCase):

    def _make_df(self):
        row = dict.fromkeys(_COLS, None)
        row.update({
            "station_id": 1, "station_name": "GareDuNord",
            "available_bikes": 5, "available_docks": 10,
            "available_electric_bikes": 1, "total_capacity": 15,
            "bikes_used_proxy": 3, "usage_rate": 0.5,
            "station_status": "OK", "city": "Rennes",
        })
        return pd.DataFrame([row], columns=_COLS)

    def test_source_postgres_quand_db_disponible(self):
        df = self._make_df()
        with patch("app.create_engine"), \
             patch("app.pd.read_sql", return_value=df):
            result_df, source = load_bike_data()
        self.assertEqual(source, "PostgreSQL")
        self.assertFalse(result_df.empty)

    def test_fallback_parquet_si_erreur_db(self):
        df = self._make_df()
        with patch("app.create_engine", side_effect=Exception("connexion refusée")), \
             patch("app.pd.read_parquet", return_value=df):
            result_df, source = load_bike_data()
        self.assertEqual(source, "Parquet local")
        self.assertFalse(result_df.empty)

    def test_fallback_parquet_si_db_vide(self):
        df = self._make_df()
        with patch("app.create_engine"), \
             patch("app.pd.read_sql", return_value=pd.DataFrame()), \
             patch("app.pd.read_parquet", return_value=df):
            result_df, source = load_bike_data()
        self.assertEqual(source, "Parquet local")

    def test_parquet_trie_par_bikes_used_proxy_desc(self):
        row_base = self._make_df().iloc[0].to_dict()
        df = pd.DataFrame([
            {**row_base, "station_id": 1, "station_name": "B", "bikes_used_proxy": 1},
            {**row_base, "station_id": 2, "station_name": "A", "bikes_used_proxy": 5},
        ])
        with patch("app.create_engine", side_effect=Exception("ko")), \
             patch("app.pd.read_parquet", return_value=df):
            result_df, source = load_bike_data()
        self.assertEqual(source, "Parquet local")
        # Après tri décroissant, la station avec 5 vélos est en tête
        self.assertEqual(result_df.iloc[0]["bikes_used_proxy"], 5)

    def test_colonnes_retournees(self):
        df = self._make_df()
        with patch("app.create_engine"), \
             patch("app.pd.read_sql", return_value=df):
            result_df, _ = load_bike_data()
        self.assertEqual(set(result_df.columns), set(_COLS))


if __name__ == "__main__":
    unittest.main()
