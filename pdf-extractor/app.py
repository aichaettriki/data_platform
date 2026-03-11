

# import os
# os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

# import uuid
# from flask import Flask, request, jsonify, render_template

# app = Flask(__name__)

# app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', 'uploads')
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# # ── Pre-load OCR engines at startup ───────────────────────────────────────────
# # ocr_extract_all is safe to import at startup (no API keys needed).
# # openai_interpreter is imported lazily inside the request handler because it
# # initialises the OpenAI client at module level and will crash if the key is
# # missing or if the openai/httpx versions are mismatched.
# print("[startup] Pre-loading PaddleOCR engines…")
# from ocr_extract_all import extract_all_from_pdf
# print("[startup] PaddleOCR ready.")


# def cleanup_file(filepath):
#     if filepath and os.path.exists(filepath):
#         os.remove(filepath)


# @app.route('/')
# def index():
#     return render_template('index.html')


# @app.route('/health')
# def health():
#     return jsonify({'status': 'ok'}), 200


# @app.route('/upload', methods=['POST'])
# # def upload_file():
# #     if 'file' not in request.files:
# #         return jsonify({'error': 'No file part'}), 400

# #     file = request.files['file']
# #     if file.filename == '':
# #         return jsonify({'error': 'No selected file'}), 400

# #     if not file.filename.lower().endswith('.pdf'):
# #         return jsonify({'error': 'Invalid file format. Please upload a PDF.'}), 400

# #     filename = f"{uuid.uuid4()}_{file.filename}"
# #     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

# #     try:
# #         file.save(filepath)
# #         print(f"[upload] Processing: {file.filename} → {filepath}")

# #         result = extract_all_from_pdf(filepath)

# #         charts = result['charts']
# #         if charts:
# #             api_key = os.environ.get('OPENAI_API_KEY', '')
# #             if api_key:
# #                 print(f"[upload] Interpreting {len(charts)} chart(s) with OpenAI…")
# #                 from openai_interpreter import interpret_all_charts
# #                 charts = interpret_all_charts(charts)
# #             else:
# #                 print("[upload] No OPENAI_API_KEY set — skipping chart interpretation.")

# #         cleanup_file(filepath)

# #         return jsonify({
# #             'success': True,
# #             'tables': result['tables'],
# #             'charts': charts
# #         })

# #     except Exception as e:
# #         print(f"[upload] ERROR: {e}")
# #         import traceback
# #         traceback.print_exc()
# #         cleanup_file(filepath)
# #         return jsonify({'error': str(e)}), 500
# def upload_file():
#     """
#     Single upload: extracts tables + charts, auto-interprets charts with OpenAI,
#     returns everything as JSON.
#     """
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file part'}), 400

#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No selected file'}), 400

#     if not file.filename.endswith('.pdf'):
#         return jsonify({'error': 'Invalid file format. Please upload a PDF.'}), 400

#     filename = f"{uuid.uuid4()}_{file.filename}"
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

#     try:
#         file.save(filepath)
#         print(f"Processing: {file.filename}...")

#         # Step 1: Extract tables + charts in one pass
#         from ocr_extract_all import extract_all_from_pdf
#         result = extract_all_from_pdf(filepath)

#         # Step 2: Auto-interpret charts with OpenAI
#         charts = result['charts']
#         if charts:
#             charts = charts[1:] 
#             print(f"Interpreting {len(charts)} chart(s) with OpenAI...")
#             from openai_interpreter import interpret_all_charts
#             charts = interpret_all_charts(charts)

#         cleanup_file(filepath)
#         return jsonify({
#             'success': True,
#             'tables': result['tables'],
#             'charts': charts
#         })

#     except Exception as e:
#         print(f"ERROR: {str(e)}")
#         import traceback
#         traceback.print_exc()
#         cleanup_file(filepath)
#         return jsonify({'error': str(e)}), 500


# if __name__ == '__main__':
#     port = int(os.environ.get('PORT', 5015))
#     debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
#     print(f"Starting PDF Extractor on http://0.0.0.0:{port}  (debug={debug})")
#     app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)


# import os
# os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

# import uuid
# from flask import Flask, request, jsonify, render_template

# app = Flask(__name__)

# app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', 'uploads')
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# print("[startup] Pre-loading PaddleOCR engines…")
# from ocr_extract_all import extract_tables_from_pdf
# print("[startup] PaddleOCR ready.")


# def cleanup_file(filepath):
#     if filepath and os.path.exists(filepath):
#         os.remove(filepath)


# @app.route('/')
# def index():
#     return render_template('index.html')


# @app.route('/health')
# def health():
#     return jsonify({'status': 'ok'}), 200


# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """
#     Single upload: extracts tables only, returns everything as JSON.
#     """
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file part'}), 400

#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No selected file'}), 400

#     if not file.filename.lower().endswith('.pdf'):
#         return jsonify({'error': 'Invalid file format. Please upload a PDF.'}), 400

#     filename = f"{uuid.uuid4()}_{file.filename}"
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

#     try:
#         file.save(filepath)
#         print(f"[upload] Processing: {file.filename} → {filepath}")

#         result = extract_tables_from_pdf(filepath)

#         cleanup_file(filepath)
#         return jsonify({
#             'success': True,
#             'tables': result['tables']
#         })

#     except Exception as e:
#         print(f"[upload] ERROR: {e}")
#         import traceback
#         traceback.print_exc()
#         cleanup_file(filepath)
#         return jsonify({'error': str(e)}), 500


# if __name__ == '__main__':
#     port = int(os.environ.get('PORT', 5015))
#     debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
#     print(f"Starting PDF Extractor on http://0.0.0.0:{port}  (debug={debug})")
#     app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)

# import os
# os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

# import uuid
# import traceback
# from flask import Flask, request, jsonify, render_template

# app = Flask(__name__)

# app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', 'uploads')
# os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

# print("[startup] Pre-loading PaddleOCR engines…")
# from ocr_extract_tables import extract_tables_from_pdf, _get_engines

# # Warm up all engines at startup so the first request isn't slow
# # and we catch any initialization errors immediately
# try:
#     _get_engines()
#     print("[startup] PaddleOCR engines ready.")
# except Exception as e:
#     print(f"[startup] ERROR initializing PaddleOCR engines: {e}")
#     traceback.print_exc()
#     raise  # Fail fast at startup rather than silently


# def cleanup_file(filepath):
#     try:
#         if filepath and os.path.exists(filepath):
#             os.remove(filepath)
#     except Exception as e:
#         print(f"[cleanup] Failed to remove {filepath}: {e}")


# @app.route('/')
# def index():
#     return render_template('index.html')


# @app.route('/health')
# def health():
#     return jsonify({'status': 'ok'}), 200


# @app.route('/upload', methods=['POST'])
# def upload_file():
#     """
#     Single upload: extracts tables only, returns everything as JSON.
#     Engines are reused across requests — no re-initialization overhead.
#     """
#     if 'file' not in request.files:
#         return jsonify({'error': 'No file part'}), 400

#     file = request.files['file']
#     if file.filename == '':
#         return jsonify({'error': 'No selected file'}), 400

#     if not file.filename.lower().endswith('.pdf'):
#         return jsonify({'error': 'Invalid file format. Please upload a PDF.'}), 400

#     filename = f"{uuid.uuid4()}_{file.filename}"
#     filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

#     try:
#         file.save(filepath)
#         print(f"[upload] Processing: {file.filename} → {filepath}")

#         result = extract_tables_from_pdf(filepath)

#         cleanup_file(filepath)
#         return jsonify({
#             'success': True,
#             'tables': result['tables']
#         })

#     except Exception as e:
#         print(f"[upload] ERROR: {e}")
#         traceback.print_exc()
#         cleanup_file(filepath)
#         return jsonify({'error': str(e)}), 500


# if __name__ == '__main__':
#     port  = int(os.environ.get('PORT', 5015))
#     debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
#     print(f"Starting PDF Extractor on http://0.0.0.0:{port}  (debug={debug})")
#     # use_reloader=False is critical — reloader forks the process which
#     # breaks PaddleOCR singleton state
#     app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)


import os
os.environ['PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION'] = 'python'

import uuid
import traceback
from flask import Flask, request, jsonify, render_template

app = Flask(__name__)

app.config['UPLOAD_FOLDER'] = os.environ.get('UPLOAD_FOLDER', 'uploads')
os.makedirs(app.config['UPLOAD_FOLDER'], exist_ok=True)

print("[startup] Pre-loading PaddleOCR engines…")
from ocr_extract_tables import extract_tables_from_pdf, _get_engines

# Warm up OCR engine at startup so the first request isn't slow
# and we catch any initialization errors immediately
try:
    _get_engines()
    print("[startup] PaddleOCR engine ready.")
except Exception as e:
    print(f"[startup] ERROR initializing PaddleOCR engines: {e}")
    traceback.print_exc()
    raise  # Fail fast at startup rather than silently


def cleanup_file(filepath):
    try:
        if filepath and os.path.exists(filepath):
            os.remove(filepath)
    except Exception as e:
        print(f"[cleanup] Failed to remove {filepath}: {e}")


@app.route('/')
def index():
    return render_template('index.html')


@app.route('/health')
def health():
    return jsonify({'status': 'ok'}), 200


@app.route('/upload', methods=['POST'])
def upload_file():
    """
    Single upload: extracts tables only, returns everything as JSON.
    Engines are reused across requests — no re-initialization overhead.

    Optional form fields:
        page_start (int): First page to process (1-indexed). Omit for first page.
        page_end   (int): Last page to process (1-indexed).  Omit for last page.
    """
    if 'file' not in request.files:
        return jsonify({'error': 'No file part'}), 400

    file = request.files['file']
    if file.filename == '':
        return jsonify({'error': 'No selected file'}), 400

    if not file.filename.lower().endswith('.pdf'):
        return jsonify({'error': 'Invalid file format. Please upload a PDF.'}), 400

    # ── Parse optional page range ─────────────────────────────────────────────
    page_start = request.form.get('page_start')
    page_end   = request.form.get('page_end')

    try:
        page_start = int(page_start) if page_start else None
        page_end   = int(page_end)   if page_end   else None
    except ValueError:
        page_start = None
        page_end   = None

    filename = f"{uuid.uuid4()}_{file.filename}"
    filepath = os.path.join(app.config['UPLOAD_FOLDER'], filename)

    try:
        file.save(filepath)
        print(f"[upload] Processing: {file.filename} → {filepath} "
              f"(pages: {page_start or 'start'}–{page_end or 'end'})")

        result = extract_tables_from_pdf(
            filepath,
            page_start=page_start,
            page_end=page_end,
        )

        cleanup_file(filepath)
        return jsonify({
            'success': True,
            'tables': result['tables']
        })

    except Exception as e:
        print(f"[upload] ERROR: {e}")
        traceback.print_exc()
        cleanup_file(filepath)
        return jsonify({'error': str(e)}), 500


if __name__ == '__main__':
    port  = int(os.environ.get('PORT', 5015))
    debug = os.environ.get('FLASK_DEBUG', 'false').lower() == 'true'
    print(f"Starting PDF Extractor on http://0.0.0.0:{port}  (debug={debug})")
    # use_reloader=False is critical — reloader forks the process which
    # breaks PaddleOCR singleton state
    app.run(debug=debug, host='0.0.0.0', port=port, use_reloader=False)