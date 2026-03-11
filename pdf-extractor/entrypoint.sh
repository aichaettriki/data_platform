# #!/bin/sh
# # Check for a specific model file, not just the directory
# if [ ! -f "/root/.paddleocr/whl/det/ch/ch_PP-OCRv4_det_infer/inference.pdmodel" ]; then
#   echo ">>> Downloading PaddleOCR models..."
#   python -c "
# from paddleocr import PPStructure, PaddleOCR
# PPStructure(layout=True,  table=False, ocr=False,  show_log=True, image_orientation=False)
# PPStructure(layout=False, table=True,  ocr=True,   show_log=True, lang='en')
# PaddleOCR(use_angle_cls=False, lang='en', show_log=True)
# print('>>> Models cached OK')
# "
# else
#   echo ">>> Models already present, skipping download."
# fi
# exec "$@"

#!/bin/sh
echo ">>> Models already present in image, starting app..."
exec "$@"