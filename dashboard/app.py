from __future__ import annotations

import pandas as pd
import plotly.express as px
import streamlit as st
from sqlalchemy import create_engine

from src.config import settings


st.set_page_config(page_title="Velostar Dashboard", layout="wide")


def load_bike_data() -> tuple[pd.DataFrame, str]:
    def load_from_parquet() -> tuple[pd.DataFrame, str]:
        dataframe = pd.read_parquet(settings.processed_file_path)
        selected_columns = [
            "station_id",
            "station_name",
            "available_bikes",
            "available_docks",
            "available_electric_bikes",
            "total_capacity",
            "bikes_used_proxy",
            "usage_rate",
            "station_status",
            "payment_terminal",
            "city",
            "temperature_c",
            "rain_probability",
            "weather_condition",
            "scrape_status",
            "record_timestamp",
            "collection_timestamp",
        ]
        return (
            dataframe[selected_columns].sort_values(
                by=["bikes_used_proxy", "station_name"],
                ascending=[False, True],
            ),
            "Parquet local",
        )

    query = (
        f"SELECT station_id, station_name, available_bikes, available_docks, "
        f"available_electric_bikes, total_capacity, bikes_used_proxy, usage_rate, "
        f"station_status, payment_terminal, city, temperature_c, rain_probability, "
        f"weather_condition, scrape_status, record_timestamp, collection_timestamp "
        f"FROM {settings.postgres_schema}.{settings.postgres_table} "
        f"ORDER BY bikes_used_proxy DESC, station_name"
    )

    try:
        engine = create_engine(settings.postgres_url)
        dataframe = pd.read_sql(query, engine)
        if not dataframe.empty:
            return dataframe, "PostgreSQL"
    except Exception:
        pass

    return load_from_parquet()


def format_temperature(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{float(value):.1f} C"


def format_rain(value: object) -> str:
    if pd.isna(value):
        return "N/A"
    return f"{int(value)}%"


def usage_label(value: float) -> str:
    if value >= 0.8:
        return "Tres forte"
    if value >= 0.6:
        return "Forte"
    if value >= 0.4:
        return "Moyenne"
    return "Faible"


st.markdown(
    """
    <style>
    .stApp {
        background:
            radial-gradient(circle at top left, rgba(92, 145, 255, 0.16), transparent 28%),
            radial-gradient(circle at top right, rgba(29, 185, 84, 0.14), transparent 24%),
            linear-gradient(180deg, #0b1220 0%, #111827 52%, #0f172a 100%);
    }
    .block-container {
        max-width: 1280px;
        padding-top: 2rem;
        padding-bottom: 2rem;
    }
    .hero-card,
    .section-card {
        background: rgba(15, 23, 42, 0.78);
        border: 1px solid rgba(148, 163, 184, 0.16);
        border-radius: 24px;
        padding: 1.25rem 1.4rem;
        box-shadow: 0 20px 60px rgba(15, 23, 42, 0.28);
        backdrop-filter: blur(10px);
    }
    .hero-title {
        font-size: 3rem;
        font-weight: 800;
        line-height: 1;
        color: #f8fafc;
        margin: 0 0 0.7rem 0;
        letter-spacing: -0.04em;
    }
    .hero-subtitle {
        font-size: 1rem;
        color: #cbd5e1;
        margin: 0;
    }
    .chip-row {
        display: flex;
        gap: 0.6rem;
        flex-wrap: wrap;
        margin-top: 1rem;
    }
    .chip {
        display: inline-block;
        padding: 0.4rem 0.75rem;
        border-radius: 999px;
        font-size: 0.9rem;
        font-weight: 600;
        color: #e2e8f0;
        background: rgba(30, 41, 59, 0.9);
        border: 1px solid rgba(148, 163, 184, 0.18);
    }
    .chip.highlight {
        background: rgba(22, 163, 74, 0.18);
        border-color: rgba(34, 197, 94, 0.35);
        color: #dcfce7;
    }
    .section-title {
        color: #f8fafc;
        font-size: 1.15rem;
        font-weight: 700;
        margin-bottom: 0.2rem;
    }
    .section-text {
        color: #94a3b8;
        font-size: 0.95rem;
        margin-bottom: 0;
    }
    div[data-testid="stMetric"] {
        background: linear-gradient(180deg, rgba(15, 23, 42, 0.92), rgba(30, 41, 59, 0.9));
        border: 1px solid rgba(148, 163, 184, 0.14);
        border-radius: 22px;
        padding: 1rem 1.1rem;
        min-height: 135px;
        box-shadow: inset 0 1px 0 rgba(255, 255, 255, 0.04);
    }
    div[data-testid="stMetricLabel"] {
        color: #94a3b8;
        font-weight: 600;
    }
    div[data-testid="stMetricValue"] {
        color: #f8fafc;
        font-size: 2.1rem;
    }
    .stMultiSelect [data-baseweb="select"] {
        background: rgba(15, 23, 42, 0.88);
        border-radius: 16px;
    }
    </style>
    """,
    unsafe_allow_html=True,
)


bike_dataframe, data_source = load_bike_data()

st.markdown(
    f"""
    <div class="hero-card">
        <div class="hero-title">Tableau de bord Velos STAR</div>
        <p class="hero-subtitle">
            Lecture rapide de l'activite des stations et du contexte meteo a Rennes.
        </p>
        <div class="chip-row">
            <span class="chip highlight">Source active: {data_source}</span>
            <span class="chip">Ville: Rennes</span>
            <span class="chip">Snapshot stations + meteo</span>
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

st.write("")

if bike_dataframe.empty:
    st.warning("Aucune donnee disponible dans PostgreSQL ni dans le fichier enrichi.")
else:
    status_values = sorted(bike_dataframe["station_status"].dropna().unique().tolist())
    selected_status = st.multiselect(
        "Etat des stations",
        status_values,
        default=status_values,
    )

    filtered = bike_dataframe[bike_dataframe["station_status"].isin(selected_status)]
    if filtered.empty:
        st.warning("Aucune donnee ne correspond aux filtres selectionnes.")
        st.stop()

    weather_snapshot = filtered.iloc[0]
    filtered = filtered.copy()
    filtered["usage_band"] = filtered["usage_rate"].fillna(0).apply(usage_label)

    st.markdown(
        """
        <div class="section-card">
            <div class="section-title">Vue d'ensemble</div>
            <p class="section-text">
                Les cartes ci-dessous resumment l'activite velo et la meteo partagee par les stations de Rennes.
            </p>
        </div>
        """,
        unsafe_allow_html=True,
    )

    overview_col1, overview_col2, overview_col3, overview_col4 = st.columns(4)
    overview_col1.metric("Stations actives", len(filtered))
    overview_col2.metric("Velos disponibles", int(filtered["available_bikes"].fillna(0).sum()))
    overview_col3.metric("Velos utilises (proxy)", int(filtered["bikes_used_proxy"].fillna(0).sum()))
    overview_col4.metric(
        "Utilisation moyenne",
        f"{filtered['usage_rate'].fillna(0).mean() * 100:.1f}%",
    )

    weather_col1, weather_col2, weather_col3 = st.columns(3)
    weather_col1.metric("Temperature actuelle", format_temperature(weather_snapshot.get("temperature_c")))
    weather_col2.metric("Risque de pluie", format_rain(weather_snapshot.get("rain_probability")))
    weather_col3.metric("Condition meteo", weather_snapshot.get("weather_condition") or "indisponible")

    st.write("")

    chart_col1, chart_col2 = st.columns([1.35, 1])

    top_stations = filtered.nlargest(12, "bikes_used_proxy").sort_values("bikes_used_proxy", ascending=True)
    usage_chart = px.bar(
        top_stations,
        x="bikes_used_proxy",
        y="station_name",
        orientation="h",
        color="usage_rate",
        color_continuous_scale=["#38bdf8", "#22c55e", "#f59e0b", "#ef4444"],
        text="bikes_used_proxy",
        title="Stations les plus sollicitees",
        labels={
            "bikes_used_proxy": "Velos utilises (proxy)",
            "station_name": "",
            "usage_rate": "Taux d'utilisation",
        },
    )
    usage_chart.update_traces(textposition="outside")
    usage_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.35)",
        font_color="#e5e7eb",
        coloraxis_colorbar_title="Usage",
        margin=dict(l=10, r=10, t=60, b=10),
        height=470,
    )

    status_chart = px.pie(
        filtered,
        names="station_status",
        title="Repartition des stations",
        hole=0.62,
        color_discrete_sequence=["#38bdf8", "#f59e0b", "#ef4444", "#22c55e"],
    )
    status_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font_color="#e5e7eb",
        margin=dict(l=10, r=10, t=60, b=10),
        height=470,
        showlegend=True,
    )

    chart_col1.plotly_chart(usage_chart, width="stretch")
    chart_col2.plotly_chart(status_chart, width="stretch")

    st.write("")

    rain_section_left, rain_section_right = st.columns([1.4, 1])

    rain_chart = px.scatter(
        filtered.sort_values("bikes_used_proxy", ascending=False),
        x="rain_probability",
        y="bikes_used_proxy",
        color="usage_band",
        size="available_bikes",
        hover_name="station_name",
        hover_data={
            "usage_rate": ":.2f",
            "available_bikes": True,
            "rain_probability": True,
            "usage_band": True,
        },
        title="Velos utilises en fonction de la pluie",
        labels={
            "rain_probability": "Risque de pluie (%)",
            "bikes_used_proxy": "Velos utilises (proxy)",
            "usage_band": "Niveau d'usage",
        },
        color_discrete_map={
            "Faible": "#38bdf8",
            "Moyenne": "#22c55e",
            "Forte": "#f59e0b",
            "Tres forte": "#ef4444",
        },
    )
    rain_chart.update_traces(marker=dict(line=dict(color="rgba(255,255,255,0.18)", width=1)))
    rain_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.35)",
        font_color="#e5e7eb",
        margin=dict(l=10, r=10, t=60, b=10),
        height=470,
    )

    summary_data = pd.DataFrame(
        [
            {"indicateur": "Meteo affichee", "valeur": weather_snapshot.get("weather_condition") or "Indisponible"},
            {"indicateur": "Temperature", "valeur": format_temperature(weather_snapshot.get("temperature_c"))},
            {"indicateur": "Risque de pluie", "valeur": format_rain(weather_snapshot.get("rain_probability"))},
            {
                "indicateur": "Station la plus sollicitee",
                "valeur": filtered.sort_values("bikes_used_proxy", ascending=False).iloc[0]["station_name"],
            },
            {
                "indicateur": "Taux moyen",
                "valeur": f"{filtered['usage_rate'].fillna(0).mean() * 100:.1f}%",
            },
        ]
    )

    summary_chart = px.bar(
        summary_data,
        x="valeur",
        y="indicateur",
        orientation="h",
        title="Lecture rapide du snapshot",
        color="indicateur",
        color_discrete_sequence=["#38bdf8", "#22c55e", "#f59e0b", "#ef4444", "#a78bfa"],
    )
    summary_chart.update_layout(
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(15,23,42,0.35)",
        font_color="#e5e7eb",
        margin=dict(l=10, r=10, t=60, b=10),
        height=470,
        showlegend=False,
        xaxis_title="",
        yaxis_title="",
    )

    rain_section_left.plotly_chart(rain_chart, width="stretch")
    rain_section_right.plotly_chart(summary_chart, width="stretch")
