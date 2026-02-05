import pandas as pd
import os
import sys
from minio import Minio
from datetime import datetime

# ===============================
# Configuration MinIO
# ===============================
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT")
MINIO_USER = os.getenv("MINIO_ROOT_USER")
MINIO_PASSWORD = os.getenv("MINIO_ROOT_PASSWORD")

minio_client = Minio(
    MINIO_ENDPOINT.replace("http://", "").replace("https://", ""),
    access_key=MINIO_USER,
    secret_key=MINIO_PASSWORD,
    secure=False
)

# ===============================
# Dossier local pour visualisation
# ===============================
DOSSIER_TEST_LOCAL = "/tmp/test_data"
os.makedirs(DOSSIER_TEST_LOCAL, exist_ok=True)

# ===============================
# Utils
# ===============================
def nettoyer_nombre(x):
    if pd.isna(x):
        return None
    x = str(x).replace(" ", "").replace(" ", "").replace(",", ".")
    try:
        return float(x)
    except:
        return x

# ===============================
# Traitement principal
# ===============================
def process_file(file_path):
    print(f"Traitement du fichier : {file_path}")

    df = pd.read_excel(file_path)

    # Colonnes années
    colonnes_annees = [c for c in df.columns if str(c).isdigit()]
    if not colonnes_annees:
        print("❌ Aucune colonne année détectée")
        return

    colonnes_meta = [c for c in df.columns if c not in colonnes_annees]

    # Passage en format long
    df_long = df.melt(
        id_vars=colonnes_meta,
        value_vars=colonnes_annees,
        var_name="annee",
        value_name="valeur"
    )

    # Gestion des indicateurs
    if len(colonnes_meta) == 1:
        df_long = df_long.rename(columns={colonnes_meta[0]: "indicateur"})
    else:
        df_long["indicateur"] = (
            df_long[colonnes_meta]
            .astype(str)
            .agg(" | ".join, axis=1)
        )
        df_long = df_long.drop(columns=colonnes_meta)

    # Nettoyage
    df_long["valeur"] = df_long["valeur"].apply(nettoyer_nombre)
    df_long["annee"] = df_long["annee"].astype(int)
    df_long["date_traitement"] = datetime.utcnow()

    df_long = df_long[
        ["indicateur", "valeur", "annee", "date_traitement"]
    ]

    # ===============================
    # Sauvegarde par année
    # ===============================
    for annee in df_long["annee"].unique():
        df_annee = df_long[df_long["annee"] == annee]

        # Noms normalisés
        nom_base = f"PAE_TRANSFORMED_{annee}"

        # --- Parquet ---
        chemin_parquet = f"/tmp/{nom_base}.parquet"
        df_annee.to_parquet(chemin_parquet, index=False)

        objet_minio = f"INS/agrégats/{annee}/{nom_base}.parquet"
        minio_client.fput_object(
            bucket_name="02-transformed",
            object_name=objet_minio,
            file_path=chemin_parquet
        )
        print(f"✔ Parquet MinIO : {objet_minio}")

        # --- CSV local ---
        chemin_csv = os.path.join(DOSSIER_TEST_LOCAL, f"{nom_base}.csv")
        df_annee.to_csv(chemin_csv, index=False)
        print(f"✔ CSV local : {chemin_csv}")

# ===============================
# Entrée script
# ===============================
if __name__ == "__main__":
    fichiers = sys.argv[1:]
    if not fichiers:
        print("❌ Aucun fichier fourni")
        sys.exit(1)

    for fichier in fichiers:
        process_file(fichier)
