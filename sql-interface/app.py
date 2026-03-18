from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import duckdb
import traceback
from datetime import datetime
import os
from dotenv import load_dotenv

app = Flask(__name__)
CORS(app)

load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))


def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

MINIO_CONFIG = {
    'endpoint': MINIO_ENDPOINT,
    's3_access_key_id': MINIO_ACCESS_KEY,
    's3_secret_access_key': MINIO_SECRET_KEY,
    's3_use_ssl': os.getenv('MINIO_USE_SSL', 'false'),
    's3_url_style': 'path'
}


def get_duckdb_connection():
    conn = duckdb.connect(':memory:')
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    conn.execute(f"SET s3_endpoint='{MINIO_CONFIG['endpoint']}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_CONFIG['s3_access_key_id']}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_CONFIG['s3_secret_access_key']}';")
    conn.execute(f"SET s3_use_ssl={MINIO_CONFIG['s3_use_ssl']};")
    conn.execute(f"SET s3_url_style='{MINIO_CONFIG['s3_url_style']}';")
    return conn


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/api/query', methods=['POST'])
def execute_query():
    try:
        data = request.json
        query = data.get('query', '').strip()

        if not query:
            return jsonify({'error': 'La requete est vide'}), 400

        conn = get_duckdb_connection()
        start_time = datetime.now()

        result = conn.execute(query)
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()

        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()

        data_rows = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                if isinstance(value, datetime):
                    value = str(value)
                row_dict[col] = value
            data_rows.append(row_dict)

        conn.close()

        return jsonify({
            'success': True,
            'columns': columns,
            'data': data_rows,
            'rowCount': len(data_rows),
            'executionTime': round(execution_time, 3)
        })

    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e),
            'stackTrace': traceback.format_exc()
        }), 400


@app.route('/api/list-buckets', methods=['GET'])
def list_buckets():
    try:
        return jsonify({
            'success': True,
            'message': 'Utilisez boto3 ou minio-py pour lister les buckets'
        })
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


@app.route('/api/examples', methods=['GET'])
def get_examples():
    examples = [
        {
            'name': 'Lire un fichier Parquet',
            'query': "SELECT * FROM read_parquet('s3://mybucket/data.parquet');"
        },
        {
            'name': 'Lire tous les fichiers Parquet d un dossier',
            'query': "SELECT *\nFROM read_parquet('s3://mybucket/folder/*.parquet');"
        },
        {
            'name': 'Lire recursivement tous les sous-dossiers',
            'query': "SELECT *\nFROM read_parquet('s3://mybucket/**/*.parquet');"
        },
        {
            'name': 'Compter le nombre de fichiers dans un dossier',
            'query': "SELECT count(*) as nb_fichiers\nFROM glob('s3://mybucket/folder/*.parquet');"
        },
        {
            'name': 'Lister les fichiers d un dossier',
            'query': "SELECT file\nFROM glob('s3://mybucket/folder/**/*');"
        },
        {
            'name': 'Inclure le nom du fichier dans les resultats',
            'query': "SELECT * FROM read_parquet('s3://mybucket/folder/*.parquet', filename=true);"
        },
        {
            'name': 'Lire un fichier CSV',
            'query': "SELECT * FROM read_csv('s3://mybucket/data.csv') LIMIT 10;"
        },
        {
            'name': 'Lire un fichier JSON',
            'query': "SELECT * FROM read_json('s3://mybucket/data.json') LIMIT 10;"
        },
        {
            'name': 'Lire plusieurs annees (Parquet)',
            'query': "SELECT *\nFROM read_parquet('s3://mybucket/folder/202[0-2]/*.parquet');"
        },
        {
            'name': 'Filtrer avec WHERE',
            'query': "SELECT *\nFROM read_parquet('s3://mybucket/sales.parquet')\nWHERE date >= '2024-01-01';"
        }
    ]
    return jsonify({'examples': examples})


@app.route('/api/config', methods=['GET'])
def get_config():
    return jsonify({
        'endpoint': MINIO_CONFIG['endpoint'],
        's3_use_ssl': MINIO_CONFIG['s3_use_ssl']
    })


@app.route('/api/config', methods=['POST'])
def update_config():
    try:
        data = request.json
        if 'endpoint' in data:
            MINIO_CONFIG['endpoint'] = data['endpoint']
        if 's3_access_key_id' in data:
            MINIO_CONFIG['s3_access_key_id'] = data['s3_access_key_id']
        if 's3_secret_access_key' in data:
            MINIO_CONFIG['s3_secret_access_key'] = data['s3_secret_access_key']
        if 's3_use_ssl' in data:
            MINIO_CONFIG['s3_use_ssl'] = data['s3_use_ssl']
        return jsonify({'success': True, 'message': 'Configuration mise a jour'})
    except Exception as e:
        return jsonify({'success': False, 'error': str(e)}), 400


if __name__ == '__main__':
    print("=" * 60)
    print("Interface SQL pour MinIO - DuckDB")
    print("=" * 60)
    print(f"Endpoint MinIO: {MINIO_CONFIG['endpoint']}")
    print(f"SSL: {MINIO_CONFIG['s3_use_ssl']}")
    print("=" * 60)
    app.run(debug=True, host='0.0.0.0', port=5000)