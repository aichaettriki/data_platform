


# import gc
# import logging
# import numpy as np
# import pandas as pd
# from PIL import Image
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

# def find_title_above_from_layout(layout_title_regions, bbox, margin_px=100):
#     """
#     Strategy A: look for a title/text region sitting just above the given bbox
#     using already-detected layout regions (no engine needed).
#     Returns extracted text string or None.
#     """
#     bx1, by1, bx2, by2 = bbox

#     candidates = []
#     for r in layout_title_regions:
#         rx1, ry1, rx2, ry2 = r['bbox']
#         vertical_ok = ry2 <= by1 and ry2 >= by1 - margin_px
#         x_overlap   = min(rx2, bx2) - max(rx1, bx1)
#         if vertical_ok and x_overlap > 0:
#             candidates.append((by1 - ry2, r))

#     if not candidates:
#         logger.debug("[TITLE] Strategy A: no matching layout regions found.")
#         return None

#     candidates.sort(key=lambda x: x[0])
#     best_region = candidates[0][1]
#     res = best_region.get('res', [])

#     logger.debug(
#         "[TITLE] Strategy A best region: type='%s' bbox=%s res=%s",
#         best_region.get('type'), best_region['bbox'], res
#     )

#     if isinstance(res, list) and res:
#         try:
#             title_text = ' '.join(
#                 item[1][0] for item in res if isinstance(item, (list, tuple))
#             ).strip()
#             if title_text:
#                 logger.debug("[TITLE] Strategy A result: '%s'", title_text)
#                 return title_text
#         except Exception as exc:
#             logger.warning("[TITLE] Strategy A parse error: %s", exc)

#     if isinstance(res, str) and res.strip():
#         logger.debug("[TITLE] Strategy A (plain str) result: '%s'", res.strip())
#         return res.strip()

#     logger.debug("[TITLE] Strategy A: region found but text empty.")
#     return None


# def find_title_above_ocr(ocr_engine, img_np, bbox, margin_px=100):
#     """
#     Strategy B: when layout title detection fails, run a lightweight OCR
#     on the pixel strip directly above the bounding box.
#     ocr_engine must be a live PaddleOCR instance (passed in, NOT created here).
#     Returns extracted text string or None.
#     """
#     bx1, by1, bx2, by2 = bbox

#     strip_y1 = max(0, by1 - margin_px)
#     strip_y2 = by1
#     strip_x1 = max(0, bx1 - 20)
#     strip_x2 = min(img_np.shape[1], bx2 + 20)

#     logger.debug(
#         "[TITLE] Strategy B: OCR strip y=[%d:%d] x=[%d:%d]",
#         strip_y1, strip_y2, strip_x1, strip_x2
#     )

#     if strip_y2 - strip_y1 < 5:
#         logger.debug("[TITLE] Strategy B: strip too thin, skipping.")
#         return None

#     strip = img_np[strip_y1:strip_y2, strip_x1:strip_x2]
#     if strip.size == 0:
#         logger.debug("[TITLE] Strategy B: strip empty, skipping.")
#         return None

#     try:
#         ocr_result = ocr_engine.ocr(strip, cls=False)
#     except Exception as exc:
#         logger.warning("[TITLE] Strategy B OCR failed: %s", exc)
#         return None

#     logger.debug("[TITLE] Strategy B raw result: %s", ocr_result)

#     if not ocr_result or not ocr_result[0]:
#         logger.debug("[TITLE] Strategy B: OCR returned no results.")
#         return None

#     lines = []
#     for line in ocr_result[0]:
#         try:
#             lines.append(line[1][0])
#         except (IndexError, TypeError):
#             pass

#     title = ' '.join(lines).strip() if lines else None
#     logger.debug("[TITLE] Strategy B result: '%s'", title)
#     return title


# def resolve_title(layout_title_regions, bbox, img_np, ocr_engine,
#                   margin_px=100, fallback_label=None):
#     """
#     Try Strategy A (layout regions) first, then Strategy B (OCR strip).
#     Returns a non-empty string always.
#     """
#     title = find_title_above_from_layout(layout_title_regions, bbox, margin_px)
#     if title:
#         return title

#     logger.debug("[TITLE] Strategy A failed — trying Strategy B (OCR strip)...")
#     title = find_title_above_ocr(ocr_engine, img_np, bbox, margin_px)
#     if title:
#         return title

#     result = fallback_label or "(no title found)"
#     logger.debug("[TITLE] Both strategies failed — using fallback: '%s'", result)
#     return result


# def is_valid_table(df: pd.DataFrame) -> bool:
#     """
#     Validate that a parsed DataFrame represents a real, meaningful table.

#     Checks:
#       1. Minimum 2 rows and 2 columns.
#       2. Fill ratio >= 30% (not mostly empty).
#       3. At least one column has 2+ unique non-empty values (not all identical).
#       4. First row looks like a header (has text values, not all numeric).
#     """
#     rows_count, cols = df.shape

#     # 1. Size check
#     if rows_count < 2 or cols < 2:
#         logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
#         return False

#     str_df = df.astype(str).replace({'nan': '', 'None': ''})

#     # 2. Fill ratio check
#     fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
#     if fill_ratio < 0.3:
#         logger.debug("[Validate] Rejected: fill ratio too low (%.2f)", fill_ratio)
#         return False

#     # 3. Diversity check — at least one column with 2+ unique non-empty values
#     has_diverse_col = False
#     for col in str_df.columns:
#         non_empty = str_df[col][str_df[col] != '']
#         if non_empty.nunique() >= 2:
#             has_diverse_col = True
#             break
#     if not has_diverse_col:
#         logger.debug("[Validate] Rejected: no column with diverse values")
#         return False

#     # 4. Header row check — first row should not be all numeric
#     first_row_values = str_df.iloc[0].tolist()
#     non_empty_header = [v for v in first_row_values if v]
#     if non_empty_header:
#         all_numeric = all(v.replace('.', '', 1).replace('-', '', 1).isdigit() for v in non_empty_header)
#         if all_numeric:
#             logger.debug("[Validate] Rejected: header row appears to be all numeric (likely no header)")
#             return False

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# # MAIN EXTRACTION  (2-pass, memory-efficient, tables only)
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_tables_from_pdf(pdf_path):
#     """
#     Extract ALL tables from a PDF using a 2-pass approach so only one
#     heavy engine is in memory at a time.

#     Pass 1 — layout engine  : find WHERE tables are on each page
#     Pass 2 — table engine   : read WHAT is inside each table crop,
#                               with OCR fallback for title detection

#     Returns:
#         {
#             "tables": [
#                 {
#                     "id":    "table_1",
#                     "page":  1,
#                     "title": "...",
#                     "rows":  [["col1", "col2", ...], ["val1", "val2", ...], ...]
#                 },
#                 ...
#             ]
#         }
#     """

#     # ── Convert PDF ───────────────────────────────────────────────────────────
#     logger.info("[ocr] Converting PDF pages (dpi=150)...")
#     images = convert_from_path(pdf_path, dpi=150)
#     total_pages = len(images)
#     pages = [np.array(img) for img in images]
#     del images
#     gc.collect()
#     logger.info("[ocr] %d page(s) loaded.", total_pages)

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 1 — Layout detection (tables only)
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 1: Layout detection (tables only)...")
#     from paddleocr import PPStructure

#     layout_engine = PPStructure(
#         layout=True, table=False, ocr=False,
#         show_log=False, image_orientation=False
#     )

#     page_table_regions = []   # list[list[region_dict]]
#     page_layout_titles = []   # list[list[region_dict]]  ← title/text/caption regions

#     for page_num, img_np in enumerate(pages):
#         result = layout_engine(img_np)

#         table_regions = [r for r in result if r.get('type', '').lower() == 'table']
#         title_regions = [r for r in result if r.get('type', '').lower()
#                          in ('title', 'text', 'figure_caption', 'table_caption')]

#         page_table_regions.append(table_regions)
#         page_layout_titles.append(title_regions)

#         logger.info(
#             "[Layout] p%d: %d table(s), %d title-candidate(s)",
#             page_num + 1, len(table_regions), len(title_regions)
#         )

#     del layout_engine
#     gc.collect()
#     logger.info("[ocr] Pass 1 done — layout engine released.")

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 2 — Table extraction + validation + title resolution
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 2: Table extraction + validation...")
#     from paddleocr import PPStructure, PaddleOCR

#     table_engine = PPStructure(layout=False, table=True, ocr=True, show_log=False, lang='en')
#     ocr_engine   = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)

#     tables      = []
#     table_count = 0

#     for page_num, img_np in enumerate(pages):
#         for region in page_table_regions[page_num]:
#             x1, y1, x2, y2 = region['bbox']
#             crop = img_np[y1:y2, x1:x2]
#             if crop.size == 0:
#                 logger.warning("[Table] p%d: empty crop, skipping.", page_num + 1)
#                 continue

#             # ── Title resolution: A (layout) → B (OCR strip fallback) ────────
#             display_title = resolve_title(
#                 layout_title_regions=page_layout_titles[page_num],
#                 bbox=(x1, y1, x2, y2),
#                 img_np=img_np,
#                 ocr_engine=ocr_engine,
#                 margin_px=120,
#                 fallback_label="(no title found)"
#             )
#             logger.info("[Table] p%d title: '%s'", page_num + 1, display_title)

#             # ── Parse table HTML → DataFrame ──────────────────────────────────
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
#                     # header=0 ensures pandas reads <thead> as column names.
#                     # We then immediately reset with header=None so the header
#                     # row is preserved as a regular data row inside the DataFrame.
#                     df_with_header = pd.read_html(html_content, header=0)[0]
#                 except Exception as exc:
#                     logger.warning("[Table] pd.read_html failed: %s", exc)
#                     continue

#                 # ── Preserve header as first data row ─────────────────────────
#                 # pd.read_html puts <thead> cells into df.columns — they would
#                 # be lost when we call iterrows(). Re-insert them explicitly.
#                 header_row = pd.DataFrame(
#                     [df_with_header.columns.tolist()],
#                     columns=df_with_header.columns
#                 )
#                 df = pd.concat([header_row, df_with_header], ignore_index=True)

#                 # ── Validate table structure ──────────────────────────────────
#                 if not is_valid_table(df):
#                     logger.info(
#                         "[Table] p%d: table under '%s' failed validation — skipped.",
#                         page_num + 1, display_title
#                     )
#                     continue

#                 table_count += 1
#                 str_df = df.astype(str).replace({'nan': '', 'None': ''})

#                 tables.append({
#                     "id":    f"table_{table_count}",
#                     "page":  page_num + 1,
#                     "title": display_title,
#                     "rows":  [row.tolist() for _, row in str_df.iterrows()]
#                 })
#                 logger.info(
#                     "[Table] Saved table_%d: '%s' (%dr x %dc)",
#                     table_count, display_title, *df.shape
#                 )

#         gc.collect()

#     del table_engine
#     del ocr_engine
#     del pages
#     gc.collect()

#     # ── Summary ───────────────────────────────────────────────────────────────
#     logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
#     return {"tables": tables}


# import gc
# import logging
# import numpy as np
# import pandas as pd
# from PIL import Image
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

# # Priority order when multiple region types are found above a table
# _TITLE_TYPE_PRIORITY = {
#     'table_caption': 0,
#     'title':         1,
#     'text':          2,
#     'figure_caption': 3,
# }


# def _extract_text_from_region(region) -> str:
#     """Pull plain text out of a layout region regardless of PaddleOCR version."""
#     res = region.get('res', [])

#     if isinstance(res, list) and res:
#         try:
#             parts = [
#                 item[1][0] for item in res
#                 if isinstance(item, (list, tuple)) and len(item) >= 2
#             ]
#             text = ' '.join(parts).strip()
#             if text:
#                 return text
#         except Exception as exc:
#             logger.warning("[TITLE] _extract_text_from_region list parse error: %s", exc)

#     if isinstance(res, str) and res.strip():
#         return res.strip()

#     return ''


# def find_title_above_from_layout(layout_title_regions, bbox, margin_px=100):
#     """
#     Strategy A: find the title region directly above the table bbox.

#     Stricter rules vs the original:
#       - Vertical gap  : region bottom must be within `margin_px` px above bbox top.
#       - X alignment   : the region must horizontally overlap at least 40% of the
#                         table width (avoids grabbing text from an adjacent column).
#       - Type priority : table_caption > title > text > figure_caption so we don't
#                         accidentally grab a random paragraph of body text.
#       - Closest wins  : among equally-typed candidates pick the one nearest the table.
#     """
#     bx1, by1, bx2, by2 = bbox
#     table_width = bx2 - bx1

#     candidates = []
#     for r in layout_title_regions:
#         rx1, ry1, rx2, ry2 = r['bbox']

#         # Must be above the table
#         if ry2 > by1:
#             continue

#         # Must be within vertical margin
#         vertical_gap = by1 - ry2
#         if vertical_gap > margin_px:
#             continue

#         # Must overlap horizontally with at least 40% of the table width
#         x_overlap = min(rx2, bx2) - max(rx1, bx1)
#         if table_width > 0 and x_overlap / table_width < 0.4:
#             continue

#         rtype    = r.get('type', 'text').lower()
#         priority = _TITLE_TYPE_PRIORITY.get(rtype, 99)
#         candidates.append((priority, vertical_gap, r))

#     if not candidates:
#         logger.debug("[TITLE] Strategy A: no matching layout regions found.")
#         return None

#     # Sort: best type first, then closest to table
#     candidates.sort(key=lambda x: (x[0], x[1]))
#     best_region = candidates[0][2]

#     logger.debug(
#         "[TITLE] Strategy A best region: type='%s' gap=%dpx bbox=%s",
#         best_region.get('type'), candidates[0][1], best_region['bbox']
#     )

#     text = _extract_text_from_region(best_region)
#     if text:
#         logger.debug("[TITLE] Strategy A result: '%s'", text)
#         return text

#     logger.debug("[TITLE] Strategy A: region found but text empty.")
#     return None


# def find_title_above_ocr(ocr_engine, img_np, bbox, margin_px=100):
#     """
#     Strategy B: when layout title detection fails, run a lightweight OCR
#     on the pixel strip directly above the bounding box.
#     ocr_engine must be a live PaddleOCR instance (passed in, NOT created here).
#     Returns extracted text string or None.
#     """
#     bx1, by1, bx2, by2 = bbox

#     strip_y1 = max(0, by1 - margin_px)
#     strip_y2 = by1
#     strip_x1 = max(0, bx1 - 20)
#     strip_x2 = min(img_np.shape[1], bx2 + 20)

#     logger.debug(
#         "[TITLE] Strategy B: OCR strip y=[%d:%d] x=[%d:%d]",
#         strip_y1, strip_y2, strip_x1, strip_x2
#     )

#     if strip_y2 - strip_y1 < 5:
#         logger.debug("[TITLE] Strategy B: strip too thin, skipping.")
#         return None

#     strip = img_np[strip_y1:strip_y2, strip_x1:strip_x2]
#     if strip.size == 0:
#         logger.debug("[TITLE] Strategy B: strip empty, skipping.")
#         return None

#     try:
#         ocr_result = ocr_engine.ocr(strip, cls=False)
#     except Exception as exc:
#         logger.warning("[TITLE] Strategy B OCR failed: %s", exc)
#         return None

#     logger.debug("[TITLE] Strategy B raw result: %s", ocr_result)

#     if not ocr_result or not ocr_result[0]:
#         logger.debug("[TITLE] Strategy B: OCR returned no results.")
#         return None

#     lines = []
#     for line in ocr_result[0]:
#         try:
#             lines.append(line[1][0])
#         except (IndexError, TypeError):
#             pass

#     title = ' '.join(lines).strip() if lines else None
#     logger.debug("[TITLE] Strategy B result: '%s'", title)
#     return title


# def resolve_title(layout_title_regions, bbox, img_np, ocr_engine,
#                   margin_px=100, fallback_label=None):
#     """
#     Try Strategy A (layout regions) first, then Strategy B (OCR strip).
#     Returns a non-empty string always.
#     """
#     title = find_title_above_from_layout(layout_title_regions, bbox, margin_px)
#     if title:
#         return title

#     logger.debug("[TITLE] Strategy A failed — trying Strategy B (OCR strip)...")
#     title = find_title_above_ocr(ocr_engine, img_np, bbox, margin_px)
#     if title:
#         return title

#     result = fallback_label or "(no title found)"
#     logger.debug("[TITLE] Both strategies failed — using fallback: '%s'", result)
#     return result


# def is_valid_table(df: pd.DataFrame) -> bool:
#     """
#     Validate that a parsed DataFrame represents a real, meaningful table.

#     Checks:
#       1. Minimum 2 rows and 2 columns.
#       2. Fill ratio >= 30% (not mostly empty).
#       3. At least one column has 2+ unique non-empty values (not all identical).
#       4. First row looks like a header (has text values, not all numeric).
#     """
#     rows_count, cols = df.shape

#     # 1. Size check
#     if rows_count < 2 or cols < 2:
#         logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
#         return False

#     str_df = df.astype(str).replace({'nan': '', 'None': ''})

#     # 2. Fill ratio check
#     fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
#     if fill_ratio < 0.3:
#         logger.debug("[Validate] Rejected: fill ratio too low (%.2f)", fill_ratio)
#         return False

#     # 3. Diversity check — at least one column with 2+ unique non-empty values
#     has_diverse_col = False
#     for col in str_df.columns:
#         non_empty = str_df[col][str_df[col] != '']
#         if non_empty.nunique() >= 2:
#             has_diverse_col = True
#             break
#     if not has_diverse_col:
#         logger.debug("[Validate] Rejected: no column with diverse values")
#         return False

#     # 4. Header row check — first row should not be all numeric
#     first_row_values = str_df.iloc[0].tolist()
#     non_empty_header = [v for v in first_row_values if v]
#     if non_empty_header:
#         all_numeric = all(v.replace('.', '', 1).replace('-', '', 1).isdigit() for v in non_empty_header)
#         if all_numeric:
#             logger.debug("[Validate] Rejected: header row appears to be all numeric (likely no header)")
#             return False

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# # MAIN EXTRACTION  (2-pass, memory-efficient, tables only)
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_tables_from_pdf(pdf_path):
#     """
#     Extract ALL tables from a PDF using a 2-pass approach so only one
#     heavy engine is in memory at a time.

#     Pass 1 — layout engine  : find WHERE tables are on each page
#     Pass 2 — table engine   : read WHAT is inside each table crop,
#                               with OCR fallback for title detection

#     Returns:
#         {
#             "tables": [
#                 {
#                     "id":    "table_1",
#                     "page":  1,
#                     "title": "...",
#                     "rows":  [["col1", "col2", ...], ["val1", "val2", ...], ...]
#                 },
#                 ...
#             ]
#         }
#     """

#     # ── Convert PDF ───────────────────────────────────────────────────────────
#     logger.info("[ocr] Converting PDF pages (dpi=150)...")
#     images = convert_from_path(pdf_path, dpi=150)
#     total_pages = len(images)
#     pages = [np.array(img) for img in images]
#     del images
#     gc.collect()
#     logger.info("[ocr] %d page(s) loaded.", total_pages)

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 1 — Layout detection (tables only)
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 1: Layout detection (tables only)...")
#     from paddleocr import PPStructure

#     layout_engine = PPStructure(
#         layout=True, table=False, ocr=False,
#         show_log=False, image_orientation=False
#     )

#     page_table_regions = []   # list[list[region_dict]]
#     page_layout_titles = []   # list[list[region_dict]]  ← title/text/caption regions

#     for page_num, img_np in enumerate(pages):
#         result = layout_engine(img_np)

#         table_regions = [r for r in result if r.get('type', '').lower() == 'table']
#         title_regions = [r for r in result if r.get('type', '').lower()
#                          in ('title', 'text', 'figure_caption', 'table_caption')]

#         page_table_regions.append(table_regions)
#         page_layout_titles.append(title_regions)

#         logger.info(
#             "[Layout] p%d: %d table(s), %d title-candidate(s)",
#             page_num + 1, len(table_regions), len(title_regions)
#         )

#     del layout_engine
#     gc.collect()
#     logger.info("[ocr] Pass 1 done — layout engine released.")

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 2 — Table extraction + validation + title resolution
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 2: Table extraction + validation...")
#     from paddleocr import PPStructure, PaddleOCR

#     table_engine = PPStructure(layout=False, table=True, ocr=True, show_log=False, lang='en')
#     ocr_engine   = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)

#     tables      = []
#     table_count = 0

#     for page_num, img_np in enumerate(pages):
#         for region in page_table_regions[page_num]:
#             x1, y1, x2, y2 = region['bbox']
#             crop = img_np[y1:y2, x1:x2]
#             if crop.size == 0:
#                 logger.warning("[Table] p%d: empty crop, skipping.", page_num + 1)
#                 continue

#             # ── Title resolution: A (layout) → B (OCR strip fallback) ────────
#             display_title = resolve_title(
#                 layout_title_regions=page_layout_titles[page_num],
#                 bbox=(x1, y1, x2, y2),
#                 img_np=img_np,
#                 ocr_engine=ocr_engine,
#                 margin_px=120,
#                 fallback_label="(no title found)"
#             )
#             logger.info("[Table] p%d title: '%s'", page_num + 1, display_title)

#             # ── Parse table HTML → DataFrame ──────────────────────────────────
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
#                     # header=0 ensures pandas reads <thead> as column names.
#                     # We then immediately reset with header=None so the header
#                     # row is preserved as a regular data row inside the DataFrame.
#                     df_with_header = pd.read_html(html_content, header=0)[0]
#                 except Exception as exc:
#                     logger.warning("[Table] pd.read_html failed: %s", exc)
#                     continue

#                 # ── Preserve header as first data row ─────────────────────────
#                 # pd.read_html puts <thead> cells into df.columns — they would
#                 # be lost when we call iterrows(). Re-insert them explicitly.
#                 header_row = pd.DataFrame(
#                     [df_with_header.columns.tolist()],
#                     columns=df_with_header.columns
#                 )
#                 df = pd.concat([header_row, df_with_header], ignore_index=True)

#                 # ── Validate table structure ──────────────────────────────────
#                 if not is_valid_table(df):
#                     logger.info(
#                         "[Table] p%d: table under '%s' failed validation — skipped.",
#                         page_num + 1, display_title
#                     )
#                     continue

#                 table_count += 1
#                 str_df = df.astype(str).replace({'nan': '', 'None': ''})

#                 tables.append({
#                     "id":    f"table_{table_count}",
#                     "page":  page_num + 1,
#                     "title": display_title,
#                     "rows":  [row.tolist() for _, row in str_df.iterrows()]
#                 })
#                 logger.info(
#                     "[Table] Saved table_%d: '%s' (%dr x %dc)",
#                     table_count, display_title, *df.shape
#                 )

#         gc.collect()

#     del table_engine
#     del ocr_engine
#     del pages
#     gc.collect()

#     # ── Summary ───────────────────────────────────────────────────────────────
#     logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
#     return {"tables": tables}


# import gc
# import logging
# import numpy as np
# import pandas as pd
# from PIL import Image
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

# _TITLE_TYPE_PRIORITY = {
#     'table_caption': 0,
#     'title':         1,
#     'text':          2,
#     'figure_caption': 3,
# }


# def _ocr_region_crop(ocr_engine, img_np, bbox, pad=10) -> str:
#     """
#     Crop the exact bounding box from img_np and run OCR on it.
#     Used in Pass 2 to extract text from title regions that were found
#     geometrically in Pass 1 (where ocr=False kept memory low).
#     Returns plain text string or ''.
#     """
#     h, w = img_np.shape[:2]
#     x1, y1, x2, y2 = bbox
#     cx1 = max(0, x1 - pad)
#     cy1 = max(0, y1 - pad)
#     cx2 = min(w, x2 + pad)
#     cy2 = min(h, y2 + pad)

#     crop = img_np[cy1:cy2, cx1:cx2]
#     if crop.size == 0:
#         return ''

#     try:
#         result = ocr_engine.ocr(crop, cls=False)
#     except Exception as exc:
#         logger.warning("[OCR-CROP] OCR on region %s failed: %s", bbox, exc)
#         return ''

#     if not result or not result[0]:
#         return ''

#     lines = []
#     for line in result[0]:
#         try:
#             lines.append((np.mean([pt[1] for pt in line[0]]), line[1][0]))
#         except (IndexError, TypeError):
#             pass

#     lines.sort(key=lambda t: t[0])
#     return ' '.join(t for _, t in lines).strip()


# def find_title_above_from_layout(layout_title_regions, bbox, img_np,
#                                   ocr_engine, margin_px=200):
#     """
#     Strategy A: find the geometrically best title region above the table,
#     then OCR just that crop to get its text.
#     """
#     bx1, by1, bx2, by2 = bbox
#     table_width = bx2 - bx1

#     candidates = []
#     for r in layout_title_regions:
#         rx1, ry1, rx2, ry2 = r['bbox']

#         if ry2 > by1:
#             continue
#         vertical_gap = by1 - ry2
#         if vertical_gap > margin_px:
#             continue

#         x_overlap     = min(rx2, bx2) - max(rx1, bx1)
#         region_cx     = (rx1 + rx2) / 2
#         centre_inside = bx1 <= region_cx <= bx2
#         overlap_ok    = (table_width > 0 and x_overlap / table_width >= 0.20)

#         if not (overlap_ok or centre_inside):
#             continue

#         rtype    = r.get('type', 'text').lower()
#         priority = _TITLE_TYPE_PRIORITY.get(rtype, 99)
#         candidates.append((priority, vertical_gap, r))

#     if not candidates:
#         logger.debug("[TITLE] Strategy A: no matching layout regions found.")
#         return None

#     candidates.sort(key=lambda x: (x[0], x[1]))
#     best_region = candidates[0][2]
#     logger.debug(
#         "[TITLE] Strategy A best region: type='%s' gap=%dpx bbox=%s",
#         best_region.get('type'), candidates[0][1], best_region['bbox']
#     )

#     text = _ocr_region_crop(ocr_engine, img_np, best_region['bbox'])
#     if text:
#         logger.debug("[TITLE] Strategy A result: '%s'", text)
#         return text

#     logger.debug("[TITLE] Strategy A: region found but OCR returned no text.")
#     return None


# def find_title_above_ocr(ocr_engine, img_np, bbox, margin_px=200):
#     """
#     Strategy B: OCR the full pixel strip above the table bounding box.
#     Used only when Strategy A finds no matching layout region.
#     """
#     bx1, by1, bx2, by2 = bbox
#     h, w = img_np.shape[:2]

#     strip_y1 = max(0, by1 - margin_px)
#     strip_y2 = max(0, by1 - 2)
#     strip_x1 = max(0, bx1 - 40)
#     strip_x2 = min(w, bx2 + 40)

#     logger.debug(
#         "[TITLE] Strategy B: OCR strip y=[%d:%d] x=[%d:%d]",
#         strip_y1, strip_y2, strip_x1, strip_x2
#     )

#     if strip_y2 - strip_y1 < 8:
#         logger.debug("[TITLE] Strategy B: strip too thin, skipping.")
#         return None

#     strip = img_np[strip_y1:strip_y2, strip_x1:strip_x2]
#     if strip.size == 0:
#         return None

#     try:
#         ocr_result = ocr_engine.ocr(strip, cls=False)
#     except Exception as exc:
#         logger.warning("[TITLE] Strategy B OCR failed: %s", exc)
#         return None

#     if not ocr_result or not ocr_result[0]:
#         logger.debug("[TITLE] Strategy B: OCR returned no results.")
#         return None

#     lines_with_y = []
#     for line in ocr_result[0]:
#         try:
#             box    = line[0]
#             text   = line[1][0]
#             mean_y = np.mean([pt[1] for pt in box])
#             lines_with_y.append((mean_y, text))
#         except (IndexError, TypeError):
#             pass

#     if not lines_with_y:
#         return None

#     lines_with_y.sort(key=lambda t: t[0])
#     title = ' '.join(t for _, t in lines_with_y).strip() or None
#     logger.debug("[TITLE] Strategy B result: '%s'", title)
#     return title


# def resolve_title(layout_title_regions, bbox, img_np, ocr_engine,
#                   margin_px=200, fallback_label=None):
#     """
#     Try Strategy A (layout bbox -> OCR crop) first,
#     then Strategy B (OCR full strip above table).
#     Always returns a non-empty string.
#     """
#     title = find_title_above_from_layout(
#         layout_title_regions, bbox, img_np, ocr_engine, margin_px
#     )
#     if title:
#         return title

#     logger.debug("[TITLE] Strategy A failed — trying Strategy B (OCR strip)...")
#     title = find_title_above_ocr(ocr_engine, img_np, bbox, margin_px)
#     if title:
#         return title

#     result = fallback_label or "(no title found)"
#     logger.debug("[TITLE] Both strategies failed — using fallback: '%s'", result)
#     return result


# def is_valid_table(df: pd.DataFrame) -> bool:
#     """Validate that a parsed DataFrame represents a real, meaningful table."""
#     rows_count, cols = df.shape

#     if rows_count < 2 or cols < 2:
#         logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
#         return False

#     str_df = df.astype(str).replace({'nan': '', 'None': ''})

#     fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
#     if fill_ratio < 0.3:
#         logger.debug("[Validate] Rejected: fill ratio too low (%.2f)", fill_ratio)
#         return False

#     has_diverse_col = False
#     for col in str_df.columns:
#         non_empty = str_df[col][str_df[col] != '']
#         if non_empty.nunique() >= 2:
#             has_diverse_col = True
#             break
#     if not has_diverse_col:
#         logger.debug("[Validate] Rejected: no column with diverse values")
#         return False

#     first_row_values = str_df.iloc[0].tolist()
#     non_empty_header = [v for v in first_row_values if v]
#     if non_empty_header:
#         all_numeric = all(
#             v.replace('.', '', 1).replace('-', '', 1).isdigit()
#             for v in non_empty_header
#         )
#         if all_numeric:
#             logger.debug("[Validate] Rejected: header row appears all numeric")
#             return False

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# # MAIN EXTRACTION  (2-pass, memory-efficient, tables only)
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_tables_from_pdf(pdf_path):
#     """
#     Extract ALL tables from a PDF using a memory-safe 2-pass approach.

#     Pass 1 — layout engine (ocr=False, lightweight):
#               Detect WHERE tables and title regions are on each page.
#               Only bounding boxes are stored — no OCR text, low RAM.

#     Pass 2 — table engine + PaddleOCR (one pair, sequential):
#               * Parse each table crop -> HTML -> DataFrame.
#               * OCR just the title region crop (Strategy A) or a narrow
#                 pixel strip above the table (Strategy B) to get title text.
#               The layout engine is fully released before Pass 2 begins.
#     """

#     # ── Convert PDF ───────────────────────────────────────────────────────────
#     logger.info("[ocr] Converting PDF pages (dpi=150)...")
#     images = convert_from_path(pdf_path, dpi=150)
#     total_pages = len(images)
#     pages = [np.array(img) for img in images]
#     del images
#     gc.collect()
#     logger.info("[ocr] %d page(s) loaded.", total_pages)

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 1 — Lightweight layout detection (bboxes only, ocr=False)
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 1: Layout detection (bboxes only, ocr=False)...")
#     from paddleocr import PPStructure

#     layout_engine = PPStructure(
#         layout=True, table=False, ocr=False,   # lightweight — no OCR
#         show_log=False, image_orientation=False
#     )

#     page_table_regions = []
#     page_layout_titles = []

#     for page_num, img_np in enumerate(pages):
#         result = layout_engine(img_np)

#         table_regions = [r for r in result if r.get('type', '').lower() == 'table']

#         # Store only bbox + type — text will be OCR'd in Pass 2
#         title_regions = [
#             {'bbox': r['bbox'], 'type': r.get('type', 'text')}
#             for r in result
#             if r.get('type', '').lower() in
#                ('title', 'text', 'figure_caption', 'table_caption')
#         ]

#         page_table_regions.append(table_regions)
#         page_layout_titles.append(title_regions)

#         logger.info(
#             "[Layout] p%d: %d table(s), %d title-candidate(s)",
#             page_num + 1, len(table_regions), len(title_regions)
#         )

#     del layout_engine
#     gc.collect()
#     logger.info("[ocr] Pass 1 done — layout engine released.")

#     # ══════════════════════════════════════════════════════════════════════════
#     # PASS 2 — Table extraction + title OCR
#     # ══════════════════════════════════════════════════════════════════════════
#     logger.info("[ocr] Pass 2: Table extraction + title OCR...")
#     from paddleocr import PPStructure, PaddleOCR

#     table_engine = PPStructure(layout=False, table=True, ocr=True,
#                                show_log=False, lang='en')
#     # Lightweight OCR engine — used only for title crop / strip
#     ocr_engine   = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)

#     tables      = []
#     table_count = 0

#     for page_num, img_np in enumerate(pages):
#         for region in page_table_regions[page_num]:
#             x1, y1, x2, y2 = region['bbox']
#             crop = img_np[y1:y2, x1:x2]
#             if crop.size == 0:
#                 logger.warning("[Table] p%d: empty crop, skipping.", page_num + 1)
#                 continue

#             # ── Title: Strategy A (OCR bbox crop) -> B (OCR strip) ───────────
#             display_title = resolve_title(
#                 layout_title_regions=page_layout_titles[page_num],
#                 bbox=(x1, y1, x2, y2),
#                 img_np=img_np,
#                 ocr_engine=ocr_engine,
#                 margin_px=200,
#                 fallback_label="(no title found)"
#             )
#             logger.info("[Table] p%d title: '%s'", page_num + 1, display_title)

#             # ── Parse table HTML -> DataFrame ─────────────────────────────────
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

#                 header_row = pd.DataFrame(
#                     [df_with_header.columns.tolist()],
#                     columns=df_with_header.columns
#                 )
#                 df = pd.concat([header_row, df_with_header], ignore_index=True)

#                 if not is_valid_table(df):
#                     logger.info(
#                         "[Table] p%d: table under '%s' failed validation — skipped.",
#                         page_num + 1, display_title
#                     )
#                     continue

#                 table_count += 1
#                 str_df = df.astype(str).replace({'nan': '', 'None': ''})

#                 tables.append({
#                     "id":    f"table_{table_count}",
#                     "page":  page_num + 1,
#                     "title": display_title,
#                     "rows":  [row.tolist() for _, row in str_df.iterrows()]
#                 })
#                 logger.info(
#                     "[Table] Saved table_%d: '%s' (%dr x %dc)",
#                     table_count, display_title, *df.shape
#                 )

#         gc.collect()

#     del table_engine
#     del ocr_engine
#     del pages
#     gc.collect()

#     logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
#     return {"tables": tables}


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

# # FIX 1 — 'text' removed from candidate types entirely.
# # Plain body paragraphs are classified as 'text' by PPStructure and were
# # the main source of wrong titles.  We now only accept real caption/title
# # region types.  'text' is kept as a last-resort fallback ONLY inside
# # _find_caption_text_fallback(), which applies a strict regex gate first.
# _STRICT_TITLE_TYPES = {'table_caption', 'title', 'figure_caption'}

# # Regex: a line that starts with a table-label pattern is almost certainly
# # a real caption regardless of its region type.
# _CAPTION_RE = re.compile(
#     r'^\s*(table|tab\.?|tableau|tbl\.?)\s*[\d\-\.]+',
#     re.IGNORECASE
# )


# def _ocr_region_crop(ocr_engine, img_np, bbox, pad=10) -> str:
#     """OCR a single bounding-box crop and return joined text (top→bottom)."""
#     h, w = img_np.shape[:2]
#     x1, y1, x2, y2 = bbox
#     crop = img_np[max(0, y1-pad):min(h, y2+pad),
#                   max(0, x1-pad):min(w, x2+pad)]
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
#             lines.append((float(np.mean([pt[1] for pt in line[0]])), line[1][0]))
#         except (IndexError, TypeError):
#             pass
#     lines.sort(key=lambda t: t[0])
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
#         """True if the region is above and horizontally related to the table."""
#         if ry2 > by1:
#             return False, 0
#         gap = by1 - ry2
#         if gap > margin_px:
#             return False, gap
#         x_overlap  = min(rx2, bx2) - max(rx1, bx1)
#         region_cx  = (rx1 + rx2) / 2
#         h_ok = (table_width > 0 and x_overlap / table_width >= 0.20) \
#                or (bx1 <= region_cx <= bx2)
#         return h_ok, gap

#     # ── Round 1: strict types only ────────────────────────────────────────────
#     candidates = []
#     text_candidates = []   # 'text' regions saved for Round 2

#     for r in layout_title_regions:
#         rx1, ry1, rx2, ry2 = r['bbox']
#         aligned, gap = _is_aligned(rx1, ry1, rx2, ry2)
#         if not aligned:
#             continue
#         rtype = r.get('type', 'text').lower()
#         if rtype in _STRICT_TITLE_TYPES:
#             candidates.append((gap, r))
#         elif rtype == 'text':
#             text_candidates.append((gap, r))   # held for Round 2

#     if candidates:
#         candidates.sort(key=lambda x: x[0])    # closest first
#         best = candidates[0][1]
#         text = _ocr_region_crop(ocr_engine, img_np, best['bbox'])
#         if text:
#             logger.debug("[TITLE] A-Round1 '%s' gap=%dpx → '%s'",
#                          best.get('type'), candidates[0][0], text)
#             return text
#         logger.debug("[TITLE] A-Round1: region found but OCR empty.")

#     # ── Round 2: 'text' regions that pass the caption regex ───────────────────
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
#     Strategy B — OCR the full strip above the table, then filter lines.

#     FIX 2: After getting all OCR lines from the strip, we:
#       1. Prefer any line that matches _CAPTION_RE  (e.g. "Table 2 — Results").
#       2. Fall back to the LAST non-empty line (closest to the table top),
#          which is most likely the caption rather than body text further up.
#     This prevents returning unrelated paragraph text that happens to sit
#     anywhere in the strip.
#     """
#     bx1, by1, bx2, by2 = bbox
#     h, w = img_np.shape[:2]

#     strip_y1 = max(0, by1 - margin_px)
#     strip_y2 = max(0, by1 - 2)
#     strip_x1 = max(0, bx1 - 40)
#     strip_x2 = min(w, bx2 + 40)

#     if strip_y2 - strip_y1 < 8:
#         logger.debug("[TITLE] B: strip too thin, skipping.")
#         return None

#     strip = img_np[strip_y1:strip_y2, strip_x1:strip_x2]
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

#     # FIX 2b: take the LAST line (bottom-most = closest to table),
#     # which is far more likely to be the caption than earlier body text.
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
#     """Validate that a parsed DataFrame represents a real, meaningful table."""
#     rows_count, cols = df.shape
#     if rows_count < 2 or cols < 2:
#         logger.debug("[Validate] Rejected: too small (%dr x %dc)", rows_count, cols)
#         return False

#     str_df = df.astype(str).replace({'nan': '', 'None': ''})

#     fill_ratio = (str_df != '').values.sum() / (rows_count * cols)
#     if fill_ratio < 0.3:
#         logger.debug("[Validate] Rejected: fill ratio %.2f", fill_ratio)
#         return False

#     has_diverse_col = any(
#         str_df[col][str_df[col] != ''].nunique() >= 2
#         for col in str_df.columns
#     )
#     if not has_diverse_col:
#         logger.debug("[Validate] Rejected: no diverse column")
#         return False

#     first_row = [v for v in str_df.iloc[0].tolist() if v]
#     if first_row and all(
#         v.replace('.', '', 1).replace('-', '', 1).isdigit() for v in first_row
#     ):
#         logger.debug("[Validate] Rejected: all-numeric header")
#         return False

#     return True


# # ══════════════════════════════════════════════════════════════════════════════
# # MAIN EXTRACTION
# # ══════════════════════════════════════════════════════════════════════════════

# def extract_tables_from_pdf(pdf_path):
#     """
#     2-pass, memory-safe PDF table extractor.

#     Pass 1 — PPStructure(ocr=False): lightweight layout scan, bboxes only.
#     Pass 2 — PPStructure(table=True) + PaddleOCR: parse tables, OCR titles.
#     """

#     logger.info("[ocr] Converting PDF pages (dpi=150)...")
#     images     = convert_from_path(pdf_path, dpi=150)
#     pages      = [np.array(img) for img in images]
#     del images
#     gc.collect()
#     logger.info("[ocr] %d page(s) loaded.", len(pages))

#     # ── PASS 1 ────────────────────────────────────────────────────────────────
#     logger.info("[ocr] Pass 1: layout detection (ocr=False)...")
#     from paddleocr import PPStructure

#     layout_engine = PPStructure(
#         layout=True, table=False, ocr=False,
#         show_log=False, image_orientation=False
#     )

#     page_table_regions = []
#     page_layout_titles = []

#     for page_num, img_np in enumerate(pages):
#         result = layout_engine(img_np)

#         table_regions = [r for r in result if r.get('type', '').lower() == 'table']

#         # FIX 1: include 'text' in stored regions so Round 2 can inspect them,
#         # but they are NOT promoted to title unless they pass _CAPTION_RE.
#         title_regions = [
#             {'bbox': r['bbox'], 'type': r.get('type', 'text')}
#             for r in result
#             if r.get('type', '').lower() in
#                ('title', 'text', 'figure_caption', 'table_caption')
#         ]

#         page_table_regions.append(table_regions)
#         page_layout_titles.append(title_regions)
#         logger.info("[Layout] p%d: %d table(s), %d title-candidates",
#                     page_num + 1, len(table_regions), len(title_regions))

#     del layout_engine
#     gc.collect()
#     logger.info("[ocr] Pass 1 done.")

#     # ── PASS 2 ────────────────────────────────────────────────────────────────
#     logger.info("[ocr] Pass 2: table extraction + title OCR...")
#     from paddleocr import PPStructure, PaddleOCR

#     table_engine = PPStructure(layout=False, table=True, ocr=True,
#                                show_log=False, lang='en')
#     ocr_engine   = PaddleOCR(use_angle_cls=False, lang='en', show_log=False)

#     tables      = []
#     table_count = 0

#     for page_num, img_np in enumerate(pages):
#         for region in page_table_regions[page_num]:
#             x1, y1, x2, y2 = region['bbox']
#             crop = img_np[y1:y2, x1:x2]
#             if crop.size == 0:
#                 continue

#             display_title = resolve_title(
#                 layout_title_regions=page_layout_titles[page_num],
#                 bbox=(x1, y1, x2, y2),
#                 img_np=img_np,
#                 ocr_engine=ocr_engine,
#                 margin_px=200,
#                 fallback_label="(no title found)"
#             )
#             logger.info("[Table] p%d title: '%s'", page_num + 1, display_title)

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

#                 header_row = pd.DataFrame(
#                     [df_with_header.columns.tolist()],
#                     columns=df_with_header.columns
#                 )
#                 df = pd.concat([header_row, df_with_header], ignore_index=True)

#                 if not is_valid_table(df):
#                     logger.info("[Table] p%d: '%s' failed validation — skipped.",
#                                 page_num + 1, display_title)
#                     continue

#                 table_count += 1
#                 str_df = df.astype(str).replace({'nan': '', 'None': ''})
#                 tables.append({
#                     "id":    f"table_{table_count}",
#                     "page":  page_num + 1,
#                     "title": display_title,
#                     "rows":  [row.tolist() for _, row in str_df.iterrows()]
#                 })
#                 logger.info("[Table] Saved table_%d: '%s' (%dr x %dc)",
#                             table_count, display_title, *df.shape)


#         gc.collect()

#     del table_engine, ocr_engine, pages
#     gc.collect()

#     logger.info("EXTRACTION COMPLETE — %d valid table(s)", table_count)
#     return {"tables": tables}

