
 
# import gc
# import logging
# import re
# import numpy as np
# import pandas as pd
# from pdf2image import convert_from_path
 
# # ── Logger setup ──────────────────────────────────────────────────────────────
# logging.basicConfig(
#     level=logging.DEBUG,
#     format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
#     datefmt="%Y-%m-%dT%H:%M:%S",
# )
# logger = logging.getLogger("ocr_extraction")
# logger.info("ocr_extract_tables module loaded.")
 
 
# # ══════════════════════════════════════════════════════════════════════════════
# # HELPERS
# # ══════════════════════════════════════════════════════════════════════════════
 
# _STRICT_TITLE_TYPES = {'table_caption', 'title', 'figure_caption'}
 
# # Matches "TABLE B.1", "Table 3", "Tab. 2.1", "Tableau 5", etc.
# _CAPTION_RE = re.compile(
#     r'^\s*(table|tab\.?|tableau|tbl\.?)\s*[\d\-\.A-Z]+',
#     re.IGNORECASE
# )
 
 
# def _ocr_region_crop(ocr_engine, img_np, bbox, pad_y=10) -> str:
#     """
#     OCR a title region and return the full joined text (top→bottom).
 
#     FIX: Horizontal crop spans the FULL page width so long titles like
#          "TABLE B.1 • Contribution sectorielle à la croissance du PIB ..."
#          are never cut off on the right side.
#     All detected lines are joined in top→bottom order — no filtering —
#     so multi-word / multi-line captions are fully captured.
#     """
#     h, w = img_np.shape[:2]
#     x1, y1, x2, y2 = bbox
 
#     # Full page width: the detected bbox often clips before the title ends
#     crop = img_np[max(0, y1 - pad_y):min(h, y2 + pad_y), 0:w]
 
#     if crop.size == 0:
#         return ''
#     try:
#         result = ocr_engine.ocr(crop, cls=False)
#     except Exception as exc:
#         logger.warning("[OCR-CROP] failed on %s: %s", bbox, exc)
#         return ''
#     if not result or not result[0]:
#         return ''
 
#     lines = []
#     for line in result[0]:
#         try:
#             mean_y = float(np.mean([pt[1] for pt in line[0]]))
#             text   = line[1][0].strip()
#             if text:
#                 lines.append((mean_y, text))
#         except (IndexError, TypeError):
#             pass
 
#     lines.sort(key=lambda t: t[0])   # top → bottom
#     return ' '.join(t for _, t in lines).strip()
 
 
# def _looks_like_caption(text: str) -> bool:
#     """Return True if the text matches a table-caption pattern."""
#     return bool(_CAPTION_RE.match(text))
 
 
# def find_title_above_from_layout(layout_title_regions, bbox, img_np,
#                                   ocr_engine, margin_px=200):
#     """
#     Strategy A — two rounds:
 
#     Round 1 (strict): only 'table_caption' / 'title' / 'figure_caption' regions.
#     Round 2 (caption-regex fallback): also considers 'text' regions, but ONLY
#              if their OCR content matches _CAPTION_RE (e.g. "Table 3.1 ...").
 
#     This prevents body paragraphs from ever being returned as a title.
#     """
#     bx1, by1, bx2, by2 = bbox
#     table_width = bx2 - bx1
 
#     def _is_aligned(rx1, ry1, rx2, ry2):
#         if ry2 > by1:
#             return False, 0
#         gap = by1 - ry2
#         if gap > margin_px:
#             return False, gap
#         x_overlap = min(rx2, bx2) - max(rx1, bx1)
#         region_cx = (rx1 + rx2) / 2
#         h_ok = (table_width > 0 and x_overlap / table_width >= 0.20) \
#                or (bx1 <= region_cx <= bx2)
#         return h_ok, gap
 
#     candidates      = []
#     text_candidates = []
 
#     for r in layout_title_regions:
#         rx1, ry1, rx2, ry2 = r['bbox']
#         aligned, gap = _is_aligned(rx1, ry1, rx2, ry2)
#         if not aligned:
#             continue
#         rtype = r.get('type', 'text').lower()
#         if rtype in _STRICT_TITLE_TYPES:
#             candidates.append((gap, r))
#         elif rtype == 'text':
#             text_candidates.append((gap, r))
 
#     # ── Round 1: strict types ─────────────────────────────────────────────────
#     if candidates:
#         candidates.sort(key=lambda x: x[0])
#         best = candidates[0][1]
#         text = _ocr_region_crop(ocr_engine, img_np, best['bbox'])
#         if text:
#             logger.debug("[TITLE] A-Round1 '%s' gap=%dpx → '%s'",
#                          best.get('type'), candidates[0][0], text)
#             return text
#         logger.debug("[TITLE] A-Round1: region found but OCR empty.")
 
#     # ── Round 2: 'text' regions passing caption regex ─────────────────────────
#     if text_candidates:
#         text_candidates.sort(key=lambda x: x[0])
#         for gap, r in text_candidates:
#             ocr_text = _ocr_region_crop(ocr_engine, img_np, r['bbox'])
#             if ocr_text and _looks_like_caption(ocr_text):
#                 logger.debug("[TITLE] A-Round2 caption-regex match gap=%dpx → '%s'",
#                              gap, ocr_text)
#                 return ocr_text
 
#     logger.debug("[TITLE] Strategy A: no usable region found.")
#     return None
 
 
# def find_title_above_ocr(ocr_engine, img_np, bbox, margin_px=200):
#     """
#     Strategy B — OCR the full-width strip above the table, then filter lines.
 
#     1. Prefer any line matching _CAPTION_RE  (e.g. "TABLE B.1 — Results").
#     2. Fall back to the LAST non-empty line (bottom-most = closest to table).
#     """
#     bx1, by1, bx2, by2 = bbox
#     h, w = img_np.shape[:2]
 
#     # Full page width so the title is never clipped horizontally
#     strip_y1 = max(0, by1 - margin_px)
#     strip_y2 = max(0, by1 - 2)
 
#     if strip_y2 - strip_y1 < 8:
#         logger.debug("[TITLE] B: strip too thin, skipping.")
#         return None
 
#     strip = img_np[strip_y1:strip_y2, 0:w]   # ← full width
#     if strip.size == 0:
#         return None
 
#     try:
#         ocr_result = ocr_engine.ocr(strip, cls=False)
#     except Exception as exc:
#         logger.warning("[TITLE] B OCR failed: %s", exc)
#         return None
 
#     if not ocr_result or not ocr_result[0]:
#         return None
 
#     lines_with_y = []
#     for line in ocr_result[0]:
#         try:
#             mean_y = float(np.mean([pt[1] for pt in line[0]]))
#             text   = line[1][0].strip()
#             if text:
#                 lines_with_y.append((mean_y, text))
#         except (IndexError, TypeError):
#             pass
 
#     if not lines_with_y:
#         return None
 
#     lines_with_y.sort(key=lambda t: t[0])   # top → bottom
 
#     # Prefer a line matching the caption pattern
#     for _, text in lines_with_y:
#         if _looks_like_caption(text):
#             logger.debug("[TITLE] B caption-regex match → '%s'", text)
#             return text
 
#     # Take bottom-most line (closest to the table)
#     last_text = lines_with_y[-1][1]
#     logger.debug("[TITLE] B last-line fallback → '%s'", last_text)
#     return last_text
 
 
# def resolve_title(layout_title_regions, bbox, img_np, ocr_engine,
#                   margin_px=200, fallback_label=None):
#     """Strategy A → Strategy B → fallback label."""
#     title = find_title_above_from_layout(
#         layout_title_regions, bbox, img_np, ocr_engine, margin_px)
#     if title:
#         return title
 
#     logger.debug("[TITLE] A failed — trying B (OCR strip)...")
#     title = find_title_above_ocr(ocr_engine, img_np, bbox, margin_px)
#     if title:
#         return title
 
#     result = fallback_label or "(no title found)"
#     logger.debug("[TITLE] Both failed — fallback: '%s'", result)
#     return result
 
 
# def is_valid_table(df: pd.DataFrame) -> bool:
#     """
#     Strict validation — rejects figures, text blocks, and lists that
#     PPStructure mis-classifies as tables.
#     """
#     rows_count, cols = df.shape
 
#     # 1. Minimum 3 rows (was 2)
#     if rows_count < 3 or cols < 2:
#         logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
#         return False
 
#     str_df = df.astype(str).replace({'nan': '', 'None': ''})
 
#     # 2. Fill ratio >= 50% (was 30%)
#     fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
#     if fill_ratio < 0.50:
#         logger.debug("[Validate] Rejected: fill ratio %.2f", fill_ratio)
#         return False
 
#     # 3. At least 2 diverse columns (was 1)
#     diverse_cols = sum(
#         1 for col in str_df.columns
#         if str_df[col][str_df[col] != ''].nunique() >= 2
#     )
#     if diverse_cols < 2:
#         logger.debug("[Validate] Rejected: only %d diverse column(s)", diverse_cols)
#         return False
 
#     # 4. Row fill consistency (std/mean < 0.6)
#     row_fill = (str_df != '').sum(axis=1).astype(float)
#     if row_fill.mean() > 0:
#         if row_fill.std() / row_fill.mean() > 0.6:
#             logger.debug("[Validate] Rejected: inconsistent row fill")
#             return False
 
#     # 5. Header row must not be all-numeric
#     first_row = [v for v in str_df.iloc[0].tolist() if v]
#     if first_row and all(
#         v.replace('.', '', 1).replace('-', '', 1).isdigit() for v in first_row
#     ):
#         logger.debug("[Validate] Rejected: all-numeric header row")
#         return False
 
#     # 6. At least 2 columns present in >50% of rows
#     cols_present_majority = sum(
#         1 for col in str_df.columns
#         if (str_df[col] != '').sum() / rows_count > 0.50
#     )
#     if cols_present_majority < 2:
#         logger.debug("[Validate] Rejected: fewer than 2 columns in >50%% of rows")
#         return False
 
#     return True
 
 
# # ══════════════════════════════════════════════════════════════════════════════
# # ENGINE SINGLETON
# # ══════════════════════════════════════════════════════════════════════════════
 
# _engines = {}
 
 
# def _get_engines():
#     """Initialize and cache PaddleOCR engines (called once at startup)."""
#     if _engines:
#         return _engines
 
#     from paddleocr import PPStructure, PaddleOCR
 
#     logger.info("[engines] Initializing layout engine...")
#     _engines['layout'] = PPStructure(
#         layout=True, table=False, ocr=False,
#         show_log=False, image_orientation=False
#     )
 
#     logger.info("[engines] Initializing table engine...")
#     _engines['table'] = PPStructure(
#         layout=False, table=True, ocr=True,
#         show_log=False, lang='en'
#     )
 
#     logger.info("[engines] Initializing OCR engine...")
#     _engines['ocr'] = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)
 
#     logger.info("[engines] All engines ready.")
#     return _engines
 
 
# # ══════════════════════════════════════════════════════════════════════════════
# # MAIN EXTRACTION
# # ══════════════════════════════════════════════════════════════════════════════
 
# def extract_tables_from_pdf(pdf_path, page_start=None, page_end=None):
#     """
#     2-pass, memory-safe PDF table extractor.
 
#     Pass 1 — PPStructure(ocr=False): lightweight layout scan, bboxes only.
#     Pass 2 — PPStructure(table=True) + PaddleOCR: parse tables, OCR titles.
 
#     Args:
#         pdf_path:   Path to the PDF file.
#         page_start: First page to process (1-indexed, inclusive). None = first page.
#         page_end:   Last page to process (1-indexed, inclusive). None = last page.
#     """
#     engines = _get_engines()
#     layout_engine = engines['layout']
#     table_engine  = engines['table']
#     ocr_engine    = engines['ocr']
 
#     logger.info("[ocr] Converting PDF pages (dpi=150, pages=%s-%s)...",
#                 page_start or 'start', page_end or 'end')
 
#     images = convert_from_path(
#         pdf_path,
#         dpi=150,
#         first_page=page_start,
#         last_page=page_end,
#     )
#     pages = [np.array(img) for img in images]
#     del images
#     gc.collect()
#     logger.info("[ocr] %d page(s) loaded.", len(pages))
 
#     # Determine actual starting page number for accurate reporting
#     start_num = page_start if page_start else 1
 
#     # ── PASS 1 ────────────────────────────────────────────────────────────────
#     logger.info("[ocr] Pass 1: layout detection (ocr=False)...")
 
#     page_table_regions = []
#     page_layout_titles = []
 
#     for idx, img_np in enumerate(pages):
#         page_num = start_num + idx
#         result = layout_engine(img_np)
 
#         table_regions = [r for r in result if r.get('type', '').lower() == 'table']
#         title_regions = [
#             {'bbox': r['bbox'], 'type': r.get('type', 'text')}
#             for r in result
#             if r.get('type', '').lower() in
#                ('title', 'text', 'figure_caption', 'table_caption')
#         ]
 
#         page_table_regions.append(table_regions)
#         page_layout_titles.append(title_regions)
#         logger.info("[Layout] p%d: %d table(s), %d title-candidates",
#                     page_num, len(table_regions), len(title_regions))
 
#     logger.info("[ocr] Pass 1 done.")
 
#     # ── PASS 2 ────────────────────────────────────────────────────────────────
#     logger.info("[ocr] Pass 2: table extraction + title OCR...")
 
#     tables      = []
#     table_count = 0
 
#     for idx, img_np in enumerate(pages):
#         page_num = start_num + idx
 
#         for region in page_table_regions[idx]:
#             x1, y1, x2, y2 = region['bbox']
#             crop = img_np[y1:y2, x1:x2]
#             if crop.size == 0:
#                 continue
 
#             display_title = resolve_title(
#                 layout_title_regions=page_layout_titles[idx],
#                 bbox=(x1, y1, x2, y2),
#                 img_np=img_np,
#                 ocr_engine=ocr_engine,
#                 margin_px=200,
#                 fallback_label="(no title found)"
#             )
#             logger.info("[Table] p%d title: '%s'", page_num, display_title)
 
#             table_result = table_engine(crop)
#             del crop
 
#             for region2 in table_result:
#                 res          = region2.get('res', {})
#                 html_content = res.get('html', '') if isinstance(res, dict) else (
#                     res if isinstance(res, str) else ''
#                 )
#                 if not html_content:
#                     continue
 
#                 try:
#                     df_with_header = pd.read_html(html_content, header=0)[0]
#                 except Exception as exc:
#                     logger.warning("[Table] pd.read_html failed: %s", exc)
#                     continue
 
#                 # Re-insert header as the first data row so it appears in rows[]
#                 header_row = pd.DataFrame(
#                     [df_with_header.columns.tolist()],
#                     columns=df_with_header.columns
#                 )
#                 df = pd.concat([header_row, df_with_header], ignore_index=True)
 
#                 if not is_valid_table(df):
#                     logger.info("[Table] p%d: '%s' failed validation — skipped.",
#                                 page_num, display_title)
#                     continue
 
#                 table_count += 1
#                 str_df = df.astype(str).replace({'nan': '', 'None': ''})
#                 tables.append({
#                     "id":    f"table_{table_count}",
#                     "page":  page_num,
#                     "title": display_title,
#                     "rows":  [row.tolist() for _, row in str_df.iterrows()]
#                 })
#                 logger.info("[Table] Saved table_%d: '%s' (%dr x %dc)",
#                             table_count, display_title, *df.shape)
 
#         gc.collect()
 
#     logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
#     return {"tables": tables}
 


import gc
import logging
import re
import numpy as np
import pandas as pd
from pdf2image import convert_from_path

# ── Logger setup ──────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
    datefmt="%Y-%m-%dT%H:%M:%S",
)
logger = logging.getLogger("ocr_extraction")
logger.info("ocr_extract_tables module loaded.")


# ══════════════════════════════════════════════════════════════════════════════
# HELPERS
# ══════════════════════════════════════════════════════════════════════════════

_STRICT_TITLE_TYPES = {'table_caption', 'title', 'figure_caption'}

# Matches "TABLE B.1", "Table 3", "Tab. 2.1", "Tableau 5", etc.
_CAPTION_RE = re.compile(
    r'^\s*(table|tab\.?|tableau|tbl\.?)\s*[\d\-\.A-Z]+',
    re.IGNORECASE
)


def _ocr_region_crop(ocr_engine, img_np, bbox, pad_y=10) -> str:
    """
    OCR a title region and return the full joined text (top→bottom).

    FIX: Horizontal crop spans the FULL page width so long titles like
         "TABLE B.1 • Contribution sectorielle à la croissance du PIB ..."
         are never cut off on the right side.
    All detected lines are joined in top→bottom order — no filtering —
    so multi-word / multi-line captions are fully captured.
    """
    h, w = img_np.shape[:2]
    x1, y1, x2, y2 = bbox

    # Full page width: the detected bbox often clips before the title ends
    crop = img_np[max(0, y1 - pad_y):min(h, y2 + pad_y), 0:w]

    if crop.size == 0:
        return ''
    try:
        result = ocr_engine.ocr(crop, cls=False)
    except Exception as exc:
        logger.warning("[OCR-CROP] failed on %s: %s", bbox, exc)
        return ''
    if not result or not result[0]:
        return ''

    lines = []
    for line in result[0]:
        try:
            mean_y = float(np.mean([pt[1] for pt in line[0]]))
            text   = line[1][0].strip()
            if text:
                lines.append((mean_y, text))
        except (IndexError, TypeError):
            pass

    lines.sort(key=lambda t: t[0])   # top → bottom
    return ' '.join(t for _, t in lines).strip()


def _looks_like_caption(text: str) -> bool:
    """Return True if the text matches a table-caption pattern."""
    return bool(_CAPTION_RE.match(text))


def find_title_above_from_layout(layout_title_regions, bbox, img_np,
                                  ocr_engine, margin_px=200):
    """
    Strategy A — two rounds:

    Round 1 (strict): only 'table_caption' / 'title' / 'figure_caption' regions.
    Round 2 (caption-regex fallback): also considers 'text' regions, but ONLY
             if their OCR content matches _CAPTION_RE (e.g. "Table 3.1 ...").

    This prevents body paragraphs from ever being returned as a title.
    """
    bx1, by1, bx2, by2 = bbox
    table_width = bx2 - bx1

    def _is_aligned(rx1, ry1, rx2, ry2):
        if ry2 > by1:
            return False, 0
        gap = by1 - ry2
        if gap > margin_px:
            return False, gap
        x_overlap = min(rx2, bx2) - max(rx1, bx1)
        region_cx = (rx1 + rx2) / 2
        h_ok = (table_width > 0 and x_overlap / table_width >= 0.20) \
               or (bx1 <= region_cx <= bx2)
        return h_ok, gap

    candidates      = []
    text_candidates = []

    for r in layout_title_regions:
        rx1, ry1, rx2, ry2 = r['bbox']
        aligned, gap = _is_aligned(rx1, ry1, rx2, ry2)
        if not aligned:
            continue
        rtype = r.get('type', 'text').lower()
        if rtype in _STRICT_TITLE_TYPES:
            candidates.append((gap, r))
        elif rtype == 'text':
            text_candidates.append((gap, r))

    # ── Round 1: strict types ─────────────────────────────────────────────────
    if candidates:
        candidates.sort(key=lambda x: x[0])
        best = candidates[0][1]
        text = _ocr_region_crop(ocr_engine, img_np, best['bbox'])
        if text:
            logger.debug("[TITLE] A-Round1 '%s' gap=%dpx → '%s'",
                         best.get('type'), candidates[0][0], text)
            return text
        logger.debug("[TITLE] A-Round1: region found but OCR empty.")

    # ── Round 2: 'text' regions passing caption regex ─────────────────────────
    if text_candidates:
        text_candidates.sort(key=lambda x: x[0])
        for gap, r in text_candidates:
            ocr_text = _ocr_region_crop(ocr_engine, img_np, r['bbox'])
            if ocr_text and _looks_like_caption(ocr_text):
                logger.debug("[TITLE] A-Round2 caption-regex match gap=%dpx → '%s'",
                             gap, ocr_text)
                return ocr_text

    logger.debug("[TITLE] Strategy A: no usable region found.")
    return None


def find_title_above_ocr(ocr_engine, img_np, bbox, margin_px=200):
    """
    Strategy B — OCR the full-width strip above the table, then filter lines.

    1. Prefer any line matching _CAPTION_RE  (e.g. "TABLE B.1 — Results").
    2. Fall back to the LAST non-empty line (bottom-most = closest to table).
    """
    bx1, by1, bx2, by2 = bbox
    h, w = img_np.shape[:2]

    # Full page width so the title is never clipped horizontally
    strip_y1 = max(0, by1 - margin_px)
    strip_y2 = max(0, by1 - 2)

    if strip_y2 - strip_y1 < 8:
        logger.debug("[TITLE] B: strip too thin, skipping.")
        return None

    strip = img_np[strip_y1:strip_y2, 0:w]   # ← full width
    if strip.size == 0:
        return None

    try:
        ocr_result = ocr_engine.ocr(strip, cls=False)
    except Exception as exc:
        logger.warning("[TITLE] B OCR failed: %s", exc)
        return None

    if not ocr_result or not ocr_result[0]:
        return None

    lines_with_y = []
    for line in ocr_result[0]:
        try:
            mean_y = float(np.mean([pt[1] for pt in line[0]]))
            text   = line[1][0].strip()
            if text:
                lines_with_y.append((mean_y, text))
        except (IndexError, TypeError):
            pass

    if not lines_with_y:
        return None

    lines_with_y.sort(key=lambda t: t[0])   # top → bottom

    # Prefer a line matching the caption pattern
    for _, text in lines_with_y:
        if _looks_like_caption(text):
            logger.debug("[TITLE] B caption-regex match → '%s'", text)
            return text

    # Take bottom-most line (closest to the table)
    last_text = lines_with_y[-1][1]
    logger.debug("[TITLE] B last-line fallback → '%s'", last_text)
    return last_text


def resolve_title(layout_title_regions, bbox, img_np, ocr_engine,
                  margin_px=200, fallback_label=None):
    """Strategy A → Strategy B → fallback label."""
    title = find_title_above_from_layout(
        layout_title_regions, bbox, img_np, ocr_engine, margin_px)
    if title:
        return title

    logger.debug("[TITLE] A failed — trying B (OCR strip)...")
    title = find_title_above_ocr(ocr_engine, img_np, bbox, margin_px)
    if title:
        return title

    result = fallback_label or "(no title found)"
    logger.debug("[TITLE] Both failed — fallback: '%s'", result)
    return result


def is_valid_table(df: pd.DataFrame) -> bool:
    """
    Strict validation — rejects figures, text blocks, and lists that
    PPStructure mis-classifies as tables.
    """
    rows_count, cols = df.shape

    # 1. Minimum 3 rows (was 2)
    if rows_count < 3 or cols < 2:
        logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
        return False

    str_df = df.astype(str).replace({'nan': '', 'None': ''})

    # 2. Fill ratio >= 50% (was 30%)
    fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
    if fill_ratio < 0.50:
        logger.debug("[Validate] Rejected: fill ratio %.2f", fill_ratio)
        return False

    # 3. At least 2 diverse columns (was 1)
    diverse_cols = sum(
        1 for col in str_df.columns
        if str_df[col][str_df[col] != ''].nunique() >= 2
    )
    if diverse_cols < 2:
        logger.debug("[Validate] Rejected: only %d diverse column(s)", diverse_cols)
        return False

    # 4. Row fill consistency (std/mean < 0.6)
    row_fill = (str_df != '').sum(axis=1).astype(float)
    if row_fill.mean() > 0:
        if row_fill.std() / row_fill.mean() > 0.6:
            logger.debug("[Validate] Rejected: inconsistent row fill")
            return False

    # 5. Header row must not be all-numeric
    first_row = [v for v in str_df.iloc[0].tolist() if v]
    if first_row and all(
        v.replace('.', '', 1).replace('-', '', 1).isdigit() for v in first_row
    ):
        logger.debug("[Validate] Rejected: all-numeric header row")
        return False

    # 6. At least 2 columns present in >50% of rows
    cols_present_majority = sum(
        1 for col in str_df.columns
        if (str_df[col] != '').sum() / rows_count > 0.50
    )
    if cols_present_majority < 2:
        logger.debug("[Validate] Rejected: fewer than 2 columns in >50%% of rows")
        return False

    return True


# ══════════════════════════════════════════════════════════════════════════════
# ENGINE SINGLETON
# ══════════════════════════════════════════════════════════════════════════════

_engines = {}


def _get_engines():
    """Initialize and cache PaddleOCR engines (called once at startup)."""
    if _engines:
        return _engines

    from paddleocr import PPStructure, PaddleOCR

    logger.info("[engines] Initializing layout engine...")
    _engines['layout'] = PPStructure(
        layout=True, table=False, ocr=False,
        show_log=False, image_orientation=False
    )

    logger.info("[engines] Initializing table engine...")
    _engines['table'] = PPStructure(
        layout=False, table=True, ocr=True,
        show_log=False, lang='en'
    )

    logger.info("[engines] Initializing OCR engine...")
    _engines['ocr'] = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)

    logger.info("[engines] All engines ready.")
    return _engines


# ══════════════════════════════════════════════════════════════════════════════
# MAIN EXTRACTION
# ══════════════════════════════════════════════════════════════════════════════

def extract_tables_from_pdf(pdf_path, pages_list=None):
    """
    2-pass, memory-safe PDF table extractor.

    Pass 1 — PPStructure(ocr=False): lightweight layout scan, bboxes only.
    Pass 2 — PPStructure(table=True) + PaddleOCR: parse tables, OCR titles.

    Args:
        pdf_path:   Path to the PDF file.
        pages_list: List of specific 1-indexed pages to process (e.g., [1, 2, 3, 8, 11, 12, 13]).
                    If None, processes all pages.
    """
    engines = _get_engines()
    layout_engine = engines['layout']
    table_engine  = engines['table']
    ocr_engine    = engines['ocr']

    logger.info("[ocr] Converting PDF pages (pages=%s)...",
                pages_list or 'all')

    # Convert only the requested pages (or all if pages_list is None)
    # pdf2image converts everything if first/last aren't specified.
    # If pages_list has gaps, we fetch them individually or in requested chunks to save memory.
    pages = []
    page_numbers = []
    
    if not pages_list:
        images = convert_from_path(pdf_path, dpi=150)
        pages = [np.array(img) for img in images]
        page_numbers = list(range(1, len(pages) + 1))
        del images
    else:
        # If the user specified specific pages, load ONLY those.
        # convert_from_path's first_page/last_page fetches a contiguous block. 
        # For non-contiguous like 1, 8, 13 we should fetch them block by block.
        # But for simplicity and safety, we fetch page by page if it's intermittent, or we just fetch the whole bounding box and filter.
        # Since memory is constrained, doing it specifically is better.
        for page_num in pages_list:
            try:
                images = convert_from_path(pdf_path, dpi=150, first_page=page_num, last_page=page_num)
                if images:
                    pages.append(np.array(images[0]))
                    page_numbers.append(page_num)
                del images
            except Exception as e:
                logger.warning(f"Could not read page {page_num}: {e}")

    gc.collect()
    logger.info("[ocr] %d page(s) loaded.", len(pages))

    # ── PASS 1 ────────────────────────────────────────────────────────────────
    logger.info("[ocr] Pass 1: layout detection (ocr=False)...")

    page_table_regions = []
    page_layout_titles = []

    for idx, img_np in enumerate(pages):
        page_num = page_numbers[idx]
        result = layout_engine(img_np)

        table_regions = [r for r in result if r.get('type', '').lower() == 'table']
        title_regions = [
            {'bbox': r['bbox'], 'type': r.get('type', 'text')}
            for r in result
            if r.get('type', '').lower() in
               ('title', 'text', 'figure_caption', 'table_caption')
        ]

        page_table_regions.append(table_regions)
        page_layout_titles.append(title_regions)
        logger.info("[Layout] p%d: %d table(s), %d title-candidates",
                    page_num, len(table_regions), len(title_regions))

    logger.info("[ocr] Pass 1 done.")

    # ── PASS 2 ────────────────────────────────────────────────────────────────
    logger.info("[ocr] Pass 2: table extraction + title OCR...")

    tables      = []
    table_count = 0

    for idx, img_np in enumerate(pages):
        page_num = page_numbers[idx]

        for region in page_table_regions[idx]:
            x1, y1, x2, y2 = region['bbox']
            crop = img_np[y1:y2, x1:x2]
            if crop.size == 0:
                continue

            display_title = resolve_title(
                layout_title_regions=page_layout_titles[idx],
                bbox=(x1, y1, x2, y2),
                img_np=img_np,
                ocr_engine=ocr_engine,
                margin_px=200,
                fallback_label="(no title found)"
            )
            logger.info("[Table] p%d title: '%s'", page_num, display_title)

            table_result = table_engine(crop)
            del crop

            for region2 in table_result:
                res          = region2.get('res', {})
                html_content = res.get('html', '') if isinstance(res, dict) else (
                    res if isinstance(res, str) else ''
                )
                if not html_content:
                    continue

                try:
                    df_with_header = pd.read_html(html_content, header=0)[0]
                except Exception as exc:
                    logger.warning("[Table] pd.read_html failed: %s", exc)
                    continue

                # Re-insert header as the first data row so it appears in rows[]
                header_row = pd.DataFrame(
                    [df_with_header.columns.tolist()],
                    columns=df_with_header.columns
                )
                df = pd.concat([header_row, df_with_header], ignore_index=True)

                if not is_valid_table(df):
                    logger.info("[Table] p%d: '%s' failed validation — skipped.",
                                page_num, display_title)
                    continue

                table_count += 1
                str_df = df.astype(str).replace({'nan': '', 'None': ''})
                tables.append({
                    "id":    f"table_{table_count}",
                    "page":  page_num,
                    "title": display_title,
                    "rows":  [row.tolist() for _, row in str_df.iterrows()]
                })
                logger.info("[Table] Saved table_%d: '%s' (%dr x %dc)",
                            table_count, display_title, *df.shape)

        gc.collect()

    logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
    return {"tables": tables}