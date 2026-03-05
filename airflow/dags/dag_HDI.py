"""
DAG : Collecte des données HDI (Human Development Index) du PNUD
Source : API HDRO — https://hdr.undp.org
Stockage : MinIO bucket 01-raw/pnud/hdi/
Fréquence : Annuelle (le rapport HDR sort en fin d'année)
"""

from datetime import datetime, timedelta
import json
import logging
import os
from io import BytesIO

import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable
from minio import Minio
from minio.error import S3Error

# ─────────────────────────────────────────────────────────────
# CONFIGURATION
# ─────────────────────────────────────────────────────────────

# Pays ciblés (codes ISO3)
TARGET_COUNTRIES = {
    "TUN": "Tunisie",
    "MAR": "Maroc",
    # Ajouter d'autres pays si besoin :
    # "DZA": "Algerie",
    # "EGY": "Egypte",
}

# Indicateurs HDRO à collecter
# L'ID 137506 = HDI value (indice principal)
INDICATORS = {
    "137506": "hdi_value",
    "72206":  "health_index",
    "103706": "education_index",
    "103606": "income_index",
    "69206":  "life_expectancy",
    "103006": "mean_years_schooling",
    "69706":  "expected_years_schooling",
    "141706": "gni_per_capita_ppp",
}

# API HDRO officielle
HDRO_API_BASE = "http://ec2-52-1-168-42.compute-1.amazonaws.com/version/1"

# MinIO
MINIO_ENDPOINT   = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ROOT_USER", "minio")
MINIO_SECRET_KEY = os.getenv("MINIO_ROOT_PASSWORD", "minio123")
MINIO_BUCKET     = "01-raw"
MINIO_PREFIX     = "pnud/hdi"

# ─────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────

def get_minio_client():
    return Minio(
        MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )


def upload_to_minio(client, data: dict, object_name: str):
    """Sérialise un dict en JSON et l'upload dans MinIO."""
    payload = json.dumps(data, ensure_ascii=False, indent=2).encode("utf-8")
    buf = BytesIO(payload)
    client.put_object(
        bucket_name=MINIO_BUCKET,
        object_name=object_name,
        data=buf,
        length=len(payload),
        content_type="application/json",
    )
    logging.info(f"Upload OK → s3://{MINIO_BUCKET}/{object_name}")


# ─────────────────────────────────────────────────────────────
# TÂCHES
# ─────────────────────────────────────────────────────────────

def fetch_hdi_data(**context):
    """
    Interroge l'API HDRO pour récupérer tous les indicateurs
    pour les pays ciblés, sur toutes les années disponibles.
    Stocke la réponse brute JSON dans MinIO.
    """
    run_date = context["ds"]  # YYYY-MM-DD
    client = get_minio_client()

    country_codes = ",".join(TARGET_COUNTRIES.keys())
    indicator_ids = ",".join(INDICATORS.keys())

    # Appel API unique pour tous les pays + indicateurs
    url = (
        f"{HDRO_API_BASE}"
        f"/country_code/{country_codes}"
        f"/indicator_id/{indicator_ids}"
        f"?structure=ciy&gzip=false"
    )

    logging.info(f"Appel API HDRO : {url}")

    try:
        resp = requests.get(url, timeout=60)
        resp.raise_for_status()
        raw_json = resp.json()
    except requests.exceptions.JSONDecodeError as e:
        raise ValueError(f"JSONDecodeError — réponse non décodable : {e}\nContenu: {resp.text[:500]}")
    except requests.exceptions.RequestException as e:
        raise ConnectionError(f"Erreur réseau API HDRO : {e}")

    # Sauvegarde brute
    raw_object = f"{MINIO_PREFIX}/raw/hdi_raw_{run_date}.json"
    upload_to_minio(client, raw_json, raw_object)

    # Passe la clé de l'objet à la tâche suivante
    context["ti"].xcom_push(key="raw_object", value=raw_object)
    context["ti"].xcom_push(key="raw_json", value=raw_json)
    logging.info(f"Données brutes sauvegardées : {raw_object}")


def transform_and_store(**context):
    """
    Parse le JSON imbriqué de l'API HDRO et produit un fichier
    JSON à plat avec les champs : country_code, country_name,
    year, indicator_id, indicator_name, value, source.
    """
    run_date = context["ds"]
    client = get_minio_client()
    raw_json = context["ti"].xcom_pull(key="raw_json")

    if not raw_json:
        raise ValueError("Pas de données reçues de la tâche fetch.")

    records = []

    # Structure retournée par l'API HDRO avec structure=ciy :
    # {
    #   "indicator_value": {
    #     "TUN": {
    #       "137506": { "1990": {"value": 0.514}, "2000": {"value": 0.651}, ... },
    #       ...
    #     },
    #     ...
    #   },
    #   "country_name": { "TUN": "Tunisia", ... },
    #   "indicator_name": { "137506": "Human development index (HDI)", ... }
    # }

    indicator_values = raw_json.get("indicator_value", {})
    country_names    = raw_json.get("country_name", {})
    indicator_names  = raw_json.get("indicator_name", {})

    if not indicator_values:
        raise ValueError(
            "Le champ 'indicator_value' est absent ou vide dans la réponse API.\n"
            f"Clés disponibles : {list(raw_json.keys())}"
        )

    for country_code, indicators_data in indicator_values.items():
        country_name = country_names.get(country_code, country_code)

        for indicator_id, years_data in indicators_data.items():
            indicator_name = indicator_names.get(indicator_id, indicator_id)
            field_name     = INDICATORS.get(indicator_id, f"indicator_{indicator_id}")

            if not isinstance(years_data, dict):
                logging.warning(f"Format inattendu pour {country_code}/{indicator_id} : {years_data}")
                continue

            for year, year_data in years_data.items():
                # La valeur peut être directement un float ou dans {"value": ...}
                if isinstance(year_data, dict):
                    value = year_data.get("value")
                else:
                    value = year_data

                if value is None or value == "":
                    continue

                records.append({
                    "country_code":   country_code,
                    "country_name":   country_name,
                    "year":           int(year),
                    "indicator_id":   indicator_id,
                    "indicator_name": indicator_name,
                    "field_name":     field_name,
                    "value":          float(value),
                    "source":         "UNDP HDRO API",
                    "source_url":     "https://hdr.undp.org",
                    "extracted_at":   run_date,
                })

    if not records:
        raise ValueError(
            "Aucun enregistrement extrait apres transformation.\n"
            f"Contenu brut : {json.dumps(raw_json)[:1000]}"
        )

    # Tri par pays → année → indicateur
    records.sort(key=lambda r: (r["country_code"], r["year"], r["indicator_id"]))

    logging.info(f"{len(records)} enregistrements extraits.")

    # Résumé par pays
    for cc, name in TARGET_COUNTRIES.items():
        country_records = [r for r in records if r["country_code"] == cc]
        years = sorted(set(r["year"] for r in country_records))
        logging.info(f"  {name} ({cc}) : {len(country_records)} obs, années {min(years)}-{max(years)}")

    # Upload JSON plat
    flat_object = f"{MINIO_PREFIX}/processed/hdi_flat_{run_date}.json"
    upload_to_minio(client, {"records": records, "count": len(records), "extracted_at": run_date}, flat_object)

    # Upload un fichier par pays pour faciliter l'accès
    for country_code, country_name in TARGET_COUNTRIES.items():
        country_records = [r for r in records if r["country_code"] == country_code]
        if country_records:
            country_object = f"{MINIO_PREFIX}/by_country/{country_code.lower()}/hdi_{country_code.lower()}_{run_date}.json"
            upload_to_minio(
                client,
                {
                    "country_code": country_code,
                    "country_name": country_name,
                    "records": country_records,
                    "count": len(country_records),
                    "extracted_at": run_date,
                },
                country_object,
            )

    logging.info("Transformation et stockage termines.")


def validate_data(**context):
    """
    Vérifie que les données HDI sont présentes et cohérentes.
    Alerte si des valeurs sont manquantes pour les années récentes.
    """
    run_date = context["ds"]
    client = get_minio_client()

    issues = []

    for country_code, country_name in TARGET_COUNTRIES.items():
        object_name = f"{MINIO_PREFIX}/by_country/{country_code.lower()}/hdi_{country_code.lower()}_{run_date}.json"
        try:
            response = client.get_object(MINIO_BUCKET, object_name)
            data = json.loads(response.read().decode("utf-8"))
            records = data.get("records", [])

            # Vérifie les données HDI récentes (2015+)
            hdi_records = [r for r in records if r["indicator_id"] == "137506"]
            recent_hdi  = [r for r in hdi_records if r["year"] >= 2015]

            if not recent_hdi:
                issues.append(f"{country_name} : Pas de donnees HDI apres 2015")
            else:
                latest = max(recent_hdi, key=lambda r: r["year"])
                logging.info(
                    f"OK — {country_name} : HDI {latest['year']} = {latest['value']:.3f}"
                )

        except S3Error as e:
            issues.append(f"{country_name} : Fichier manquant dans MinIO ({e})")
        except Exception as e:
            issues.append(f"{country_name} : Erreur validation ({e})")

    if issues:
        raise ValueError("Problemes detectes lors de la validation :\n" + "\n".join(issues))

    logging.info("Validation OK — toutes les donnees sont presentes et coherentes.")


# ─────────────────────────────────────────────────────────────
# DAG
# ─────────────────────────────────────────────────────────────

default_args = {
    "owner": "itceq",
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": timedelta(minutes=10),
    "retry_exponential_backoff": True,
    "email_on_failure": False,
}

with DAG(
    dag_id="pnud_hdi_ingestion",
    description="Collecte annuelle des donnees HDI (PNUD) pour la Tunisie et le Maroc → MinIO 01-raw",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval="0 6 1 1 *",  # Chaque 1er janvier à 6h (données publiées en fin d'année)
    catchup=False,
    max_active_runs=1,
    tags=["pnud", "hdi", "international", "ingestion", "raw"],
) as dag:

    t1 = PythonOperator(
        task_id="fetch_hdi_from_api",
        python_callable=fetch_hdi_data,
        doc_md="""
        **Collecte brute HDRO API**
        - Appelle l'API officielle du PNUD (HDRO)
        - Indicateur principal : HDI (ID 137506)
        - Pays : Tunisie (TUN) + Maroc (MAR)
        - Stocke le JSON brut dans MinIO : `01-raw/pnud/hdi/raw/`
        """,
    )

    t2 = PythonOperator(
        task_id="transform_and_store",
        python_callable=transform_and_store,
        doc_md="""
        **Transformation et stockage structuré**
        - Parse le JSON imbriqué de l'API HDRO
        - Aplatit en enregistrements : country_code, country_name, year, indicator, value
        - Stocke dans MinIO :
          - `01-raw/pnud/hdi/processed/` → fichier global
          - `01-raw/pnud/hdi/by_country/tun/` → Tunisie
          - `01-raw/pnud/hdi/by_country/mar/` → Maroc
        """,
    )

    t3 = PythonOperator(
        task_id="validate_data",
        python_callable=validate_data,
        doc_md="""
        **Validation qualité**
        - Vérifie la présence des données HDI post-2015
        - Logue les valeurs récentes pour chaque pays
        - Lève une exception si données manquantes
        """,
    )

    t1 >> t2 >> t3