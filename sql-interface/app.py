from flask import Flask, request, jsonify, render_template
from flask_cors import CORS
import duckdb
import traceback
import json
from datetime import datetime

app = Flask(__name__)
CORS(app)

# Configuration MinIO (à adapter selon votre environnement)
import os
from dotenv import load_dotenv

# Charger les variables d'environnement
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '../../.env'))


def get_env_var(name, default=None, required=False):
    value = os.getenv(name, default)
    if required and value is None:
        raise ValueError(f"Missing required environment variable: {name}")
    return value


# 🔧 Config
MINIO_ENDPOINT = get_env_var("MINIO_ENDPOINT", required=True)
MINIO_ACCESS_KEY = "minio"
MINIO_SECRET_KEY = "minio123"

MINIO_CONFIG = {
    'endpoint': MINIO_ENDPOINT,
    's3_access_key_id': MINIO_ACCESS_KEY,
    's3_secret_access_key': MINIO_SECRET_KEY,
    's3_use_ssl': os.getenv('MINIO_USE_SSL', 'false'),  # 'true' si vous utilisez HTTPS
    's3_url_style': 'path'  # MinIO utilise le style path
}

def get_duckdb_connection():
    """Créer une connexion DuckDB configurée pour MinIO"""
    conn = duckdb.connect(':memory:')
    
    # Installer et charger l'extension httpfs pour S3
    conn.execute("INSTALL httpfs;")
    conn.execute("LOAD httpfs;")
    
    # Configuration pour MinIO
    conn.execute(f"SET s3_endpoint='{MINIO_CONFIG['endpoint']}';")
    conn.execute(f"SET s3_access_key_id='{MINIO_CONFIG['s3_access_key_id']}';")
    conn.execute(f"SET s3_secret_access_key='{MINIO_CONFIG['s3_secret_access_key']}';")
    conn.execute(f"SET s3_use_ssl={MINIO_CONFIG['s3_use_ssl']};")
    conn.execute(f"SET s3_url_style='{MINIO_CONFIG['s3_url_style']}';")
    
    return conn

@app.route('/')
def index():
    """Page d'accueil avec l'interface web"""
    return render_template('index.html')

@app.route('/api/query', methods=['POST'])
def execute_query():
    """Exécuter une requête SQL"""
    try:
        data = request.json
        query = data.get('query', '').strip()
        
        if not query:
            return jsonify({'error': 'La requête est vide'}), 400
        
        # Créer une connexion DuckDB
        conn = get_duckdb_connection()
        
        # Mesurer le temps d'exécution
        start_time = datetime.now()
        
        # Exécuter la requête
        result = conn.execute(query)
        
        # Récupérer les résultats
        columns = [desc[0] for desc in result.description]
        rows = result.fetchall()
        
        end_time = datetime.now()
        execution_time = (end_time - start_time).total_seconds()
        
        # Convertir en format JSON-friendly
        data_rows = []
        for row in rows:
            row_dict = {}
            for i, col in enumerate(columns):
                value = row[i]
                # Convertir les types non-JSON en string
                if isinstance(value, (datetime,)):
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
        error_msg = str(e)
        stack_trace = traceback.format_exc()
        return jsonify({
            'success': False,
            'error': error_msg,
            'stackTrace': stack_trace
        }), 400

@app.route('/api/list-buckets', methods=['GET'])
def list_buckets():
    """Lister les buckets disponibles (exemple)"""
    try:
        conn = get_duckdb_connection()
        
        # Note: Cette requête nécessite que vous ayez accès aux buckets
        # Vous devrez peut-être utiliser boto3 ou minio-py pour lister les buckets
        return jsonify({
            'success': True,
            'message': 'Utilisez boto3 ou minio-py pour lister les buckets'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

@app.route('/api/examples', methods=['GET'])
def get_examples():
    """Retourner des exemples de requêtes"""
    examples = [
        {
            'name': 'Lire un fichier Parquet',
            'query': "SELECT * FROM read_parquet('s3://mybucket/data.parquet') LIMIT 10;"
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
            'name': 'Agrégation sur Parquet',
            'query': """SELECT 
    category, 
    COUNT(*) as count,
    AVG(price) as avg_price
FROM read_parquet('s3://mybucket/products.parquet')
GROUP BY category
ORDER BY count DESC;"""
        },
        {
            'name': 'Joindre plusieurs fichiers',
            'query': """SELECT 
    o.order_id,
    o.order_date,
    c.customer_name,
    o.total_amount
FROM read_parquet('s3://mybucket/orders.parquet') o
JOIN read_parquet('s3://mybucket/customers.parquet') c
    ON o.customer_id = c.customer_id
LIMIT 10;"""
        },
        {
            'name': 'Filtrer avec WHERE',
            'query': """SELECT * 
FROM read_parquet('s3://mybucket/sales.parquet')
WHERE date >= '2024-01-01' 
  AND amount > 100
LIMIT 20;"""
        }
    ]
    
    return jsonify({'examples': examples})

@app.route('/api/config', methods=['GET'])
def get_config():
    """Retourner la configuration MinIO (sans les secrets)"""
    safe_config = {
        'endpoint': MINIO_CONFIG['endpoint'],
        's3_use_ssl': MINIO_CONFIG['s3_use_ssl']
    }
    return jsonify(safe_config)

@app.route('/api/config', methods=['POST'])
def update_config():
    """Mettre à jour la configuration MinIO"""
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
        
        return jsonify({
            'success': True,
            'message': 'Configuration mise à jour'
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 400

if __name__ == '__main__':
    print("=" * 60)
    print("Interface SQL pour MinIO - DuckDB")
    print("=" * 60)
    print(f"Endpoint MinIO: {MINIO_CONFIG['endpoint']}")
    print(f"SSL: {MINIO_CONFIG['s3_use_ssl']}")
    print("=" * 60)
    print("\nL'application démarre sur http://localhost:5000")
    print("\nExemples de requêtes:")
    print("  - SELECT * FROM read_parquet('s3://bucket/file.parquet');")
    print("  - SELECT * FROM read_csv('s3://bucket/file.csv');")
    print("  - SELECT * FROM read_json('s3://bucket/file.json');")
    print("=" * 60)
    
    app.run(debug=True, host='0.0.0.0', port=5000)
