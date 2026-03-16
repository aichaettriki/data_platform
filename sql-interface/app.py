from flask import Flask, request, jsonify, render_template, Response, stream_with_context
from flask_cors import CORS
import duckdb
import traceback
import json
from datetime import datetime
import threading
import os
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

# ─── Chargement des variables d'environnement ────────────────────────────────
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))

def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value

# ─── Config MinIO ─────────────────────────────────────────────────────────────
MINIO_ENDPOINT   = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

MINIO_CONFIG = {
    'endpoint':             MINIO_ENDPOINT,
    's3_access_key_id':     MINIO_ACCESS_KEY,
    's3_secret_access_key': MINIO_SECRET_KEY,
    's3_use_ssl':           os.getenv('MINIO_USE_SSL', 'false'),
    's3_url_style':         'path'
}

# ─── Limites de sécurité ──────────────────────────────────────────────────────
MAX_ROWS_PER_PAGE = 500     # Lignes max retournées par page
MAX_PAGE_SIZE     = 1000    # Plafond absolu que le client peut demander
QUERY_TIMEOUT_SEC = 120     # Timeout requête en secondes


# ─── Connexion DuckDB ─────────────────────────────────────────────────────────
def get_duckdb_connection():
    """Créer une connexion DuckDB configurée pour MinIO"""
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{MINIO_CONFIG['endpoint']}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_CONFIG['s3_access_key_id']}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_CONFIG['s3_secret_access_key']}';")
    conn.execute(f"SET s3_use_ssl={MINIO_CONFIG['s3_use_ssl']};")
    conn.execute(f"SET s3_url_style='{MINIO_CONFIG['s3_url_style']}';")
    return conn


def run_with_timeout(fn, timeout_sec):
    """Exécuter fn() dans un thread avec timeout. Retourne (result, error)."""
    result_holder = [None, None]  # [result, exception]

    def target():
        try:
            result_holder[0] = fn()
        except Exception as e:
            result_holder[1] = e

    t = threading.Thread(target=target, daemon=True)
    t.start()
    t.join(timeout=timeout_sec)

    if t.is_alive():
        return None, TimeoutError(f"La requête a dépassé le délai de {timeout_sec}s. "
                                   "Ajoutez un LIMIT ou affinez vos filtres WHERE.")
    return result_holder[0], result_holder[1]


def serialize_value(value):
    """Convertir les valeurs non-JSON en types sérialisables."""
    if isinstance(value, datetime):
        return str(value)
    if hasattr(value, 'item'):          # numpy scalars
        return value.item()
    return value


# ─── Routes ───────────────────────────────────────────────────────────────────
@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
def execute_query():
    """
    Exécuter une requête SQL avec pagination côté serveur.

    Body JSON :
        query     : str   – requête SQL
        page      : int   – numéro de page (défaut 1)
        pageSize  : int   – lignes par page (défaut 100, max 1000)
        countTotal: bool  – calculer le total (peut être lent, défaut false)
    """
    try:
        body = request.json or {}
        query = body.get('query', '').strip()

        if not query:
            return jsonify({'error': 'La requête est vide'}), 400

        # ── Paramètres de pagination ──────────────────────────────────────────
        page        = max(1, int(body.get('page', 1)))
        page_size   = min(max(1, int(body.get('pageSize', 100))), MAX_PAGE_SIZE)
        offset      = (page - 1) * page_size
        want_count  = bool(body.get('countTotal', False))

        query_clean = query.rstrip(';')
        query_upper = query_clean.upper()

        # Si la requête n'a pas de LIMIT, on enveloppe avec pagination
        if 'LIMIT' not in query_upper:
            paginated_query = (
                f"SELECT * FROM ({query_clean}) __paged "
                f"LIMIT {page_size} OFFSET {offset}"
            )
            count_query = (
                f"SELECT COUNT(*) FROM ({query_clean}) __count"
                if want_count else None
            )
        else:
            # L'utilisateur a mis son propre LIMIT → on ne touche pas
            paginated_query = query_clean
            count_query     = None

        # ── Exécution avec timeout ────────────────────────────────────────────
        start_time = datetime.now()

        def run():
            conn = get_duckdb_connection()
            res  = conn.execute(paginated_query)
            cols = [desc[0] for desc in res.description]
            rows = res.fetchall()

            total = None
            if count_query:
                total = conn.execute(count_query).fetchone()[0]

            conn.close()
            return cols, rows, total

        result, err = run_with_timeout(run, QUERY_TIMEOUT_SEC)

        if err:
            raise err

        columns, rows, total_count = result
        execution_time = round((datetime.now() - start_time).total_seconds(), 3)

        # ── Sérialisation ─────────────────────────────────────────────────────
        data_rows = [
            {col: serialize_value(row[i]) for i, col in enumerate(columns)}
            for row in rows
        ]

        response = {
            'success':       True,
            'columns':       columns,
            'data':          data_rows,
            'rowCount':      len(data_rows),
            'page':          page,
            'pageSize':      page_size,
            'executionTime': execution_time,
            'hasMore':       len(data_rows) == page_size,   # indique s'il y a potentiellement une page suivante
        }
        if total_count is not None:
            response['totalCount'] = total_count
            response['totalPages'] = -(-total_count // page_size)  # ceiling division

        return jsonify(response)

    except TimeoutError as e:
        return jsonify({'success': False, 'error': str(e)}), 408

    except Exception as e:
        return jsonify({
            'success':    False,
            'error':      str(e),
            'stackTrace': traceback.format_exc()
        }), 400


@app.route('/api/query/stream', methods=['POST'])
def stream_query():
    """
    Variante streaming : envoie les lignes au fur et à mesure (NDJSON).
    Utile pour des exports volumineux sans bloquer le navigateur.

    Body JSON identique à /api/query sauf que pageSize peut aller jusqu'à 10 000.
    """
    body       = request.json or {}
    query      = body.get('query', '').strip().rstrip(';')
    page_size  = min(int(body.get('pageSize', 1000)), 10_000)
    page       = max(1, int(body.get('page', 1)))
    offset     = (page - 1) * page_size

    query_upper = query.upper()
    if 'LIMIT' not in query_upper:
        final_query = f"SELECT * FROM ({query}) __s LIMIT {page_size} OFFSET {offset}"
    else:
        final_query = query

    def generate():
        try:
            conn   = get_duckdb_connection()
            res    = conn.execute(final_query)
            cols   = [desc[0] for desc in res.description]
            # Émet les colonnes en premier
            yield json.dumps({'type': 'columns', 'columns': cols}) + '\n'

            batch_size = 200
            while True:
                batch = res.fetchmany(batch_size)
                if not batch:
                    break
                rows = [
                    {col: serialize_value(row[i]) for i, col in enumerate(cols)}
                    for row in batch
                ]
                yield json.dumps({'type': 'rows', 'rows': rows}) + '\n'

            conn.close()
            yield json.dumps({'type': 'done'}) + '\n'

        except Exception as e:
            yield json.dumps({'type': 'error', 'error': str(e)}) + '\n'

    return Response(
        stream_with_context(generate()),
        mimetype='application/x-ndjson'
    )


@app.route('/api/schema', methods=['POST'])
def get_schema():
    """
    Retourner le schéma d'un fichier/dossier S3 sans charger les données.
    Body JSON : { "path": "s3://bucket/file.parquet" }
    """
    try:
        body = request.json or {}
        path = body.get('path', '').strip()
        if not path:
            return jsonify({'error': 'path manquant'}), 400

        def run():
            conn = get_duckdb_connection()
            # DESCRIBE retourne le schéma sans lire toutes les lignes
            res  = conn.execute(f"DESCRIBE SELECT * FROM read_parquet('{path}') LIMIT 0")
            cols = res.fetchall()
            conn.close()
            return cols

        result, err = run_with_timeout(run, 30)
        if err:
            raise err

        schema = [{'column_name': r[0], 'column_type': r[1]} for r in result]
        return jsonify({'success': True, 'schema': schema})

    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/examples', methods=['GET'])
def get_examples():
    examples = [
        {
            'name': 'Aperçu rapide (100 premières lignes)',
            'query': "SELECT * FROM read_parquet('s3://mybucket/data.parquet') LIMIT 100;"
        },
        {
            'name': 'Compter les lignes sans tout charger',
            'query': "SELECT COUNT(*) FROM read_parquet('s3://mybucket/data.parquet');"
        },
        {
            'name': 'Schéma / colonnes disponibles',
            'query': "DESCRIBE SELECT * FROM read_parquet('s3://mybucket/data.parquet') LIMIT 0;"
        },
        {
            'name': 'Lire tous les fichiers Parquet d\'un dossier',
            'query': "SELECT * FROM read_parquet('s3://mybucket/folder/*.parquet') LIMIT 100;"
        },
        {
            'name': 'Lire récursivement tous les sous-dossiers',
            'query': "SELECT * FROM read_parquet('s3://mybucket/**/*.parquet') LIMIT 100;"
        },
        {
            'name': 'Statistiques agrégées (sans ramener toutes les lignes)',
            'query': """SELECT
    date_trunc('month', event_date) AS mois,
    COUNT(*)                        AS nb,
    SUM(montant)                    AS total
FROM read_parquet('s3://mybucket/sales/*.parquet')
GROUP BY 1
ORDER BY 1;"""
        },
        {
            'name': 'Compter le nombre de fichiers dans un dossier',
            'query': "SELECT count(*) AS nb_fichiers FROM glob('s3://mybucket/folder/*.parquet');"
        },
        {
            'name': 'Lister les fichiers d\'un dossier',
            'query': "SELECT file FROM glob('s3://mybucket/folder/**/*');"
        },
        {
            'name': 'Inclure le nom du fichier dans les résultats',
            'query': "SELECT * FROM read_parquet('s3://mybucket/folder/*.parquet', filename=true) LIMIT 100;"
        },
        {
            'name': 'Filtrer avec WHERE (évite de scanner tout le fichier)',
            'query': """SELECT *
FROM read_parquet('s3://mybucket/sales.parquet')
WHERE date >= '2024-01-01'
LIMIT 500;"""
        },
        {
            'name': 'Lire un fichier CSV',
            'query': "SELECT * FROM read_csv('s3://mybucket/data.csv') LIMIT 100;"
        },
        {
            'name': 'Lire un fichier JSON',
            'query': "SELECT * FROM read_json('s3://mybucket/data.json') LIMIT 100;"
        },
    ]
    return jsonify({'examples': examples})


@app.route('/api/list-buckets', methods=['GET'])
def list_buckets():
    return jsonify({
        'success': True,
        'message': 'Utilisez boto3 ou minio-py pour lister les buckets'
    })


@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        'endpoint':   MINIO_CONFIG['endpoint'],
        's3_use_ssl': MINIO_CONFIG['s3_use_ssl']
    })


@app.route('/api/config', methods=['POST'])
def update_config():
    try:
        data = request.json or {}
        for key in ('endpoint', 's3_access_key_id', 's3_secret_access_key', 's3_use_ssl'):
            if key in data:
                MINIO_CONFIG[key] = data[key]
        return jsonify({'success': True, 'message': 'Configuration mise à jour'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


# ─── Démarrage ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    print("=" * 60)
    print("Interface SQL pour MinIO - DuckDB  (Big Data Edition)")
    print("=" * 60)
    print(f"Endpoint MinIO : {MINIO_CONFIG['endpoint']}")
    print(f"SSL            : {MINIO_CONFIG['s3_use_ssl']}")
    print(f"Timeout requête: {QUERY_TIMEOUT_SEC}s")
    print(f"Lignes/page max: {MAX_ROWS_PER_PAGE}")
    print("=" * 60)
    print("\nL'application démarre sur http://localhost:5000")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)