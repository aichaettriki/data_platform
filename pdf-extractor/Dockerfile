# FROM python:3.10-slim

# # ── System dependencies ────────────────────────────────────────────────────────
# # PaddleOCR needs OpenGL, pdf2image needs poppler, OpenCV needs libglib
# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libgl1 \
#     libglib2.0-0 \
#     libsm6 \
#     libxext6 \
#     libxrender-dev \
#     libgomp1 \
#     poppler-utils \
#     curl \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# # ── Python dependencies ────────────────────────────────────────────────────────
# # Copy requirements first to exploit Docker layer cache
# COPY requirements.txt .

# # pin protobuf BEFORE installing everything else to avoid the conflict
# # that app.py works around with os.environ at runtime
# RUN pip install --no-cache-dir "protobuf==3.20.3" && \
#     pip install --no-cache-dir -r requirements.txt

# # ── Application code ───────────────────────────────────────────────────────────
# COPY . .

# # uploads directory must exist inside the container
# RUN mkdir -p uploads

# # ── Pre-download PaddleOCR models at build time ────────────────────────────────
# # This avoids a slow first-request download in production.
# # The models are cached in /root/.paddleocr/
# RUN python -c "\
# from paddleocr import PPStructure, PaddleOCR; \
# PPStructure(layout=True,  table=False, ocr=False,  show_log=False, image_orientation=False); \
# PPStructure(layout=False, table=True,  ocr=True,   show_log=False, lang='en'); \
# PaddleOCR(use_angle_cls=False, lang='en', show_log=False); \
# print('Models cached OK')"

# EXPOSE 5001

# # ── Healthcheck (Traefik / Docker will use this) ───────────────────────────────
# HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
#     CMD curl -f http://localhost:5001/ || exit 1

# CMD ["python", "app.py"]


# FROM python:3.10-slim

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libgl1 \
#     libglib2.0-0 \
#     libsm6 \
#     libxext6 \
#     libxrender-dev \
#     libgomp1 \
#     poppler-utils \
#     curl \
#     wget \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# COPY requirements.txt .

# RUN pip install --no-cache-dir "protobuf==3.20.3" && \
#     pip install --no-cache-dir "openai==1.14.3" "httpx==0.27.0" && \
#     pip install --no-cache-dir -r requirements.txt

# COPY . .

# RUN mkdir -p uploads

# ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# # ── Download PaddleOCR models directly via wget (no Python execution at build) ─
# RUN mkdir -p /root/.paddleocr/whl/layout/picodet_lcnet_x1_0_fgd_layout_infer \
#              /root/.paddleocr/whl/table/en_ppstructure_mobile_v2.0_SLANet_infer \
#              /root/.paddleocr/whl/det/en/en_PP-OCRv3_det_infer \
#              /root/.paddleocr/whl/rec/en/en_PP-OCRv4_rec_infer \
#              /root/.paddleocr/whl/cls/ch_ppocr_mobile_v2.0_cls_infer && \
#     wget -q -O /tmp/layout.tar https://paddleocr.bj.bcebos.com/ppstructure/models/layout/picodet_lcnet_x1_0_fgd_layout_infer.tar && \
#     tar -xf /tmp/layout.tar -C /root/.paddleocr/whl/layout/ && \
#     wget -q -O /tmp/table.tar https://paddleocr.bj.bcebos.com/ppstructure/models/slanet/en_ppstructure_mobile_v2.0_SLANet_infer.tar && \
#     tar -xf /tmp/table.tar -C /root/.paddleocr/whl/table/ && \
#     wget -q -O /tmp/det.tar https://paddleocr.bj.bcebos.com/PP-OCRv3/english/en_PP-OCRv3_det_infer.tar && \
#     tar -xf /tmp/det.tar -C /root/.paddleocr/whl/det/en/ && \
#     wget -q -O /tmp/rec.tar https://paddleocr.bj.bcebos.com/PP-OCRv4/english/en_PP-OCRv4_rec_infer.tar && \
#     tar -xf /tmp/rec.tar -C /root/.paddleocr/whl/rec/en/ && \
#     wget -q -O /tmp/cls.tar https://paddleocr.bj.bcebos.com/dygraph_v2.0/ch/ch_ppocr_mobile_v2.0_cls_infer.tar && \
#     tar -xf /tmp/cls.tar -C /root/.paddleocr/whl/cls/ && \
#     rm -f /tmp/*.tar && \
#     echo "All models downloaded."

# EXPOSE 5015

# HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
#     CMD curl -f http://localhost:5015/health || exit 1

# CMD ["python", "app.py"]

# FROM python:3.10-slim

# RUN apt-get update && apt-get install -y --no-install-recommends \
#     libgl1 \
#     libglib2.0-0 \
#     libsm6 \
#     libxext6 \
#     libxrender-dev \
#     libgomp1 \
#     poppler-utils \
#     curl \
#     wget \
#     && rm -rf /var/lib/apt/lists/*

# WORKDIR /app

# COPY requirements.txt .

# RUN pip install --no-cache-dir "protobuf==3.20.3" && \
#     pip install --no-cache-dir "openai==1.14.3" "httpx==0.27.0" && \
#     pip install --no-cache-dir -r requirements.txt && \
#     pip install --no-cache-dir gunicorn

# COPY . .

# RUN mkdir -p uploads

# ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# # ── Download PaddleOCR models directly via wget (no Python execution at build) ─
# RUN mkdir -p /root/.paddleocr/whl/layout/picodet_lcnet_x1_0_fgd_layout_infer \
#              /root/.paddleocr/whl/table/en_ppstructure_mobile_v2.0_SLANet_infer \
#              /root/.paddleocr/whl/det/en/en_PP-OCRv3_det_infer \
#              /root/.paddleocr/whl/rec/en/en_PP-OCRv4_rec_infer \
#              /root/.paddleocr/whl/cls/ch_ppocr_mobile_v2.0_cls_infer && \
#     wget -q -O /tmp/layout.tar https://paddleocr.bj.bcebos.com/ppstructure/models/layout/picodet_lcnet_x1_0_fgd_layout_infer.tar && \
#     tar -xf /tmp/layout.tar -C /root/.paddleocr/whl/layout/ && \
#     wget -q -O /tmp/table.tar https://paddleocr.bj.bcebos.com/ppstructure/models/slanet/en_ppstructure_mobile_v2.0_SLANet_infer.tar && \
#     tar -xf /tmp/table.tar -C /root/.paddleocr/whl/table/ && \
#     wget -q -O /tmp/det.tar https://paddleocr.bj.bcebos.com/PP-OCRv3/english/en_PP-OCRv3_det_infer.tar && \
#     tar -xf /tmp/det.tar -C /root/.paddleocr/whl/det/en/ && \
#     wget -q -O /tmp/rec.tar https://paddleocr.bj.bcebos.com/PP-OCRv4/english/en_PP-OCRv4_rec_infer.tar && \
#     tar -xf /tmp/rec.tar -C /root/.paddleocr/whl/rec/en/ && \
#     wget -q -O /tmp/cls.tar https://paddleocr.bj.bcebos.com/dygraph_v2.0/ch/ch_ppocr_mobile_v2.0_cls_infer.tar && \
#     tar -xf /tmp/cls.tar -C /root/.paddleocr/whl/cls/ && \
#     rm -f /tmp/*.tar && \
#     echo "All models downloaded."

# EXPOSE 5015

# HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
#     CMD curl -f http://localhost:5015/health || exit 1

# # Single worker — PaddleOCR is NOT multi-process safe
# # --timeout 300  → allow up to 5 min for heavy PDFs
# # --preload      → warm up engines before accepting requests, catch errors early
# CMD ["gunicorn", "--workers=1", "--timeout=300", "--preload", \
#      "--bind=0.0.0.0:5015", "--access-logfile=-", "--error-logfile=-", \
#      "app:app"]


FROM python:3.10-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
    libgomp1 \
    poppler-utils \
    curl \
    wget \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY requirements.txt .

RUN pip install --no-cache-dir "protobuf==3.20.3" && \
    pip install --no-cache-dir "openai==1.14.3" "httpx==0.27.0" && \
    pip install --no-cache-dir -r requirements.txt && \
    pip install --no-cache-dir gunicorn

COPY . .

RUN mkdir -p uploads

ENV PROTOCOL_BUFFERS_PYTHON_IMPLEMENTATION=python

# ── Download PaddleOCR models at build time ───────────────────────────────────
RUN mkdir -p /root/.paddleocr/whl/layout/picodet_lcnet_x1_0_fgd_layout_infer \
             /root/.paddleocr/whl/table/en_ppstructure_mobile_v2.0_SLANet_infer \
             /root/.paddleocr/whl/det/en/en_PP-OCRv3_det_infer \
             /root/.paddleocr/whl/rec/en/en_PP-OCRv4_rec_infer \
             /root/.paddleocr/whl/cls/ch_ppocr_mobile_v2.0_cls_infer && \
    wget -q -O /tmp/layout.tar https://paddleocr.bj.bcebos.com/ppstructure/models/layout/picodet_lcnet_x1_0_fgd_layout_infer.tar && \
    tar -xf /tmp/layout.tar -C /root/.paddleocr/whl/layout/ && \
    wget -q -O /tmp/table.tar https://paddleocr.bj.bcebos.com/ppstructure/models/slanet/en_ppstructure_mobile_v2.0_SLANet_infer.tar && \
    tar -xf /tmp/table.tar -C /root/.paddleocr/whl/table/ && \
    wget -q -O /tmp/det.tar https://paddleocr.bj.bcebos.com/PP-OCRv3/english/en_PP-OCRv3_det_infer.tar && \
    tar -xf /tmp/det.tar -C /root/.paddleocr/whl/det/en/ && \
    wget -q -O /tmp/rec.tar https://paddleocr.bj.bcebos.com/PP-OCRv4/english/en_PP-OCRv4_rec_infer.tar && \
    tar -xf /tmp/rec.tar -C /root/.paddleocr/whl/rec/en/ && \
    wget -q -O /tmp/cls.tar https://paddleocr.bj.bcebos.com/dygraph_v2.0/ch/ch_ppocr_mobile_v2.0_cls_infer.tar && \
    tar -xf /tmp/cls.tar -C /root/.paddleocr/whl/cls/ && \
    rm -f /tmp/*.tar && \
    echo "All models downloaded."

EXPOSE 5015

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD curl -f http://localhost:5015/health || exit 1

# NO --preload: avoids double-loading paddle models in master+worker memory
# --workers 1:  mandatory, PaddleOCR is not multi-process safe
# --timeout 300: allow up to 5 min for heavy PDFs
CMD ["gunicorn", "--workers=1", "--timeout=300", \
     "--bind=0.0.0.0:5015", "--access-logfile=-", "--error-logfile=-", \
     "app:app"]