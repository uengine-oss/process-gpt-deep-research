"""
소스 파일 파싱 + 청크 분할 + LLM 요약 모듈.

사용자가 프로세스 소스에 올린 참고자료 파일(pdf, hwp, hwpx, docx, txt, md)을
파싱하고 청크로 분할한 뒤, 각 청크를 LLM으로 요약하여 반환한다.

결과물(SourceChunk 리스트)은 메모리에 보관하며,
hwpx-mcp에서 템플릿 청크별 참고자료 선택에 사용된다.
"""

import asyncio
import json
import logging
import os
import re
import struct
import tempfile
import zipfile
import xml.etree.ElementTree as ET
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from io import BytesIO

import requests

logger = logging.getLogger("source-parser")

CHUNK_SIZE = 2000
CHUNK_OVERLAP = 200
SUMMARY_MAX_CONCURRENCY = 10


# ─── 데이터 모델 ─────────────────────────────────────────────────────

@dataclass
class SourceChunk:
    file_name: str
    chunk_index: int
    original_text: str
    summary: str

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


# ─── 텍스트 청크 분할 ────────────────────────────────────────────────

def _split_text(text: str, chunk_size: int = CHUNK_SIZE, overlap: int = CHUNK_OVERLAP) -> List[str]:
    """텍스트를 청크로 분할한다. RecursiveCharacterTextSplitter와 유사한 로직."""
    if not text or not text.strip():
        return []
    separators = ["\n\n", "\n", ". ", "! ", "? ", " ", ""]
    return _recursive_split(text, separators, chunk_size, overlap)


def _recursive_split(text: str, separators: List[str], chunk_size: int, overlap: int) -> List[str]:
    final_chunks: List[str] = []
    separator = separators[-1]
    new_separators: List[str] = []
    for i, sep in enumerate(separators):
        if sep == "":
            separator = sep
            break
        if sep in text:
            separator = sep
            new_separators = separators[i + 1:]
            break

    if separator:
        splits = text.split(separator)
    else:
        splits = list(text)

    good_splits: List[str] = []
    for s in splits:
        if len(s) < chunk_size:
            good_splits.append(s)
        else:
            if good_splits:
                merged = _merge_splits(good_splits, separator, chunk_size, overlap)
                final_chunks.extend(merged)
                good_splits = []
            if not new_separators:
                final_chunks.append(s)
            else:
                sub = _recursive_split(s, new_separators, chunk_size, overlap)
                final_chunks.extend(sub)

    if good_splits:
        merged = _merge_splits(good_splits, separator, chunk_size, overlap)
        final_chunks.extend(merged)

    return final_chunks


def _merge_splits(splits: List[str], separator: str, chunk_size: int, overlap: int) -> List[str]:
    docs: List[str] = []
    current: List[str] = []
    total = 0
    for s in splits:
        s_len = len(s)
        if total + s_len + (len(separator) if current else 0) > chunk_size and current:
            doc = separator.join(current).strip()
            if doc:
                docs.append(doc)
            # overlap: 뒤에서부터 overlap 크기만큼 유지
            while total > overlap and current:
                removed = current.pop(0)
                total -= len(removed) + len(separator)
        current.append(s)
        total += s_len + (len(separator) if len(current) > 1 else 0)
    if current:
        doc = separator.join(current).strip()
        if doc:
            docs.append(doc)
    return docs


# ─── 파일 다운로드 ───────────────────────────────────────────────────

def _download_file(url: str, suffix: str = "") -> Path:
    resp = requests.get(url, timeout=60)
    resp.raise_for_status()
    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=suffix)
    tmp.write(resp.content)
    tmp.flush()
    tmp.close()
    return Path(tmp.name)


# ─── PDF 파싱 ────────────────────────────────────────────────────────

def _parse_pdf(file_path: str) -> str:
    try:
        import pdfplumber
    except ImportError:
        logger.warning("pdfplumber 미설치 — PDF 파싱 불가")
        return ""

    parts: List[str] = []
    with pdfplumber.open(file_path) as pdf:
        for page in pdf.pages:
            elements: List[Dict[str, Any]] = []

            # 표 추출
            tables = page.find_tables()
            table_bboxes = [t.bbox for t in tables]
            for table in tables:
                try:
                    extracted = table.extract()
                    if extracted:
                        md = _table_to_markdown(extracted)
                        if md:
                            elements.append({"y": table.bbox[1], "content": md})
                except Exception:
                    continue

            # 표 외 텍스트 추출
            try:
                all_words = page.extract_words()
            except Exception:
                all_words = []
            non_table_words = [
                w for w in all_words
                if not any(_word_in_bbox(w, bbox) for bbox in table_bboxes)
            ]
            if non_table_words:
                lines: Dict[float, List[Tuple[float, str]]] = {}
                for w in non_table_words:
                    y = round(w["top"], 1)
                    lines.setdefault(y, []).append((w["x0"], w.get("text", "")))
                current_block: List[str] = []
                prev_y = None
                for y in sorted(lines.keys()):
                    line_text = " ".join(t for _, t in sorted(lines[y], key=lambda x: x[0]))
                    if prev_y is not None and (y - prev_y) > 20:
                        if current_block:
                            elements.append({"y": prev_y, "content": "\n".join(current_block)})
                        current_block = []
                    current_block.append(line_text)
                    prev_y = y
                if current_block and prev_y is not None:
                    elements.append({"y": prev_y, "content": "\n".join(current_block)})

            elements.sort(key=lambda x: x["y"])
            page_text = "\n\n".join(e["content"] for e in elements if e.get("content"))
            if page_text.strip():
                parts.append(page_text)

    return "\n\n".join(parts)


def _word_in_bbox(word: Dict, bbox: tuple) -> bool:
    x0, top, x1, bottom = bbox
    h_mid = (word["x0"] + word["x1"]) / 2
    v_mid = (word["top"] + word["bottom"]) / 2
    return x0 <= h_mid <= x1 and top <= v_mid <= bottom


def _table_to_markdown(table: List[List[Any]]) -> str:
    if not table or len(table) == 0:
        return ""
    lines = []
    header = table[0]
    lines.append("| " + " | ".join(str(c) if c else "" for c in header) + " |")
    lines.append("| " + " | ".join(["---"] * len(header)) + " |")
    for row in table[1:]:
        lines.append("| " + " | ".join(str(c) if c else "" for c in row) + " |")
    return "\n".join(lines)


# ─── HWPX 파싱 (ZIP+XML, 표→마크다운) ───────────────────────────────

def _parse_hwpx(file_path: str) -> str:
    def _ltag(elem):
        t = elem.tag
        return t.split('}', 1)[1] if '}' in t else t

    def _collect_t(elem):
        parts = []
        for n in elem.iter():
            if _ltag(n) == 'tbl':
                continue
            if _ltag(n) == 't' and n.text:
                parts.append(n.text)
        return "".join(parts)

    def _tbl_to_md(tbl):
        cells = []
        for tr in tbl:
            if _ltag(tr) != 'tr':
                continue
            for tc in tr:
                if _ltag(tc) != 'tc':
                    continue
                row = col = 0
                col_span = row_span = 1
                for cc in tc:
                    tag = _ltag(cc)
                    if tag == 'cellAddr':
                        for k, v in cc.attrib.items():
                            if 'colAddr' in k: col = int(v)
                            if 'rowAddr' in k: row = int(v)
                    elif tag == 'cellSpan':
                        for k, v in cc.attrib.items():
                            if 'colSpan' in k:
                                try: col_span = int(v)
                                except ValueError: pass
                            if 'rowSpan' in k:
                                try: row_span = int(v)
                                except ValueError: pass
                tp = []
                for sub in tc.iter():
                    if _ltag(sub) == 't' and sub.text:
                        tp.append(sub.text)
                cells.append((row, col, " ".join("".join(tp).split())))
        if not cells:
            return ""
        mr = max(r + 1 for r, c, t in cells)
        mc = max(c + 1 for r, c, t in cells)
        grid = [[""] * mc for _ in range(mr)]
        for r, c, t in cells:
            grid[r][c] = t
        cw = [max(3, *(len(grid[r][c]) for r in range(mr))) for c in range(mc)]
        lines = []
        for r in range(mr):
            lines.append("| " + " | ".join(grid[r][c].ljust(cw[c]) for c in range(mc)) + " |")
            if r == 0:
                lines.append("| " + " | ".join("-" * cw[c] for c in range(mc)) + " |")
        return "\n".join(lines)

    def _walk(elem, results):
        tag = _ltag(elem)
        if tag == 'tbl':
            md = _tbl_to_md(elem)
            if md:
                results.append(md)
            return
        if tag == 'p':
            has_tbl = any(_ltag(d) == 'tbl' for d in elem.iter() if d is not elem)
            if has_tbl:
                for ch in elem:
                    _walk(ch, results)
            else:
                text = _collect_t(elem)
                if text.strip():
                    results.append(text)
            return
        for ch in elem:
            _walk(ch, results)

    try:
        with zipfile.ZipFile(file_path, "r") as z:
            names = z.namelist()
            section_files = sorted(n for n in names if "Contents/section" in n and n.endswith(".xml"))
            if not section_files:
                content_name = next((n for n in names if "contents" in n.lower() and n.endswith(".xml")), None)
                if not content_name:
                    return ""
                with z.open(content_name) as f:
                    raw = f.read().decode("utf-8", errors="replace")
                text = re.sub(r"<[^>]+>", " ", raw)
                return re.sub(r"\s+", " ", text).strip()
            sections = []
            for sf in section_files:
                with z.open(sf) as f:
                    root = ET.fromstring(f.read())
                parts: List[str] = []
                _walk(root, parts)
                if parts:
                    sections.append("\n\n".join(parts))
            return "\n\n".join(sections)
    except Exception as e:
        logger.warning("HWPX 파싱 실패: %s", e)
        return ""


# ─── HWP5 (OLE) 파싱 ────────────────────────────────────────────────

TAG_PARA_TEXT = 67
TAG_LIST_HEADER = 72
TAG_TABLE = 77


def _parse_hwp(file_path: str) -> str:
    try:
        import olefile
    except ImportError:
        logger.warning("olefile 미설치 — HWP 파싱 불가")
        return ""

    if not olefile.isOleFile(file_path):
        logger.warning("OLE 형식이 아닙니다: %s", file_path)
        return ""

    ole = olefile.OleFileIO(file_path)
    try:
        if not ole.exists("FileHeader"):
            return ""
        # 암호 검사
        header_data = ole.openstream("FileHeader").read(256)
        if len(header_data) >= 40:
            props = struct.unpack_from('<I', header_data, 36)[0]
            if (props & 0x02) != 0:
                logger.warning("암호로 보호된 HWP 파일")
                return ""

        sections = []
        idx = 0
        while True:
            stream = f"BodyText/Section{idx}"
            if not ole.exists(stream):
                break
            import zlib
            compressed = ole.openstream(stream).read()
            try:
                data = zlib.decompress(compressed, 15)
            except zlib.error:
                try:
                    data = zlib.decompress(compressed, -15)
                except zlib.error:
                    data = compressed
            text = _process_hwp5_section(data)
            if text.strip():
                sections.append(text)
            idx += 1
        return "\n\n".join(sections)
    finally:
        ole.close()


def _parse_hwp5_records(data: bytes):
    records = []
    offset = 0
    while offset + 4 <= len(data):
        header_value = struct.unpack_from('<I', data, offset)[0]
        tag_id = header_value & 0x3FF
        level = (header_value >> 10) & 0x3FF
        size = (header_value >> 20) & 0xFFF
        offset += 4
        if size == 0xFFF:
            if offset + 4 > len(data):
                break
            size = struct.unpack_from('<I', data, offset)[0]
            offset += 4
        if size == 0 or offset + size > len(data):
            offset += size
            continue
        records.append((tag_id, level, data[offset:offset + size]))
        offset += size
    return records


def _extract_para_text(record_data: bytes) -> str:
    parts = []
    off = 0
    while off + 2 <= len(record_data):
        char_code = struct.unpack_from('<H', record_data, off)[0]
        off += 2
        if char_code < 32:
            if char_code in (10, 13):
                parts.append('\n')
            elif char_code == 9:
                parts.append('\t')
        else:
            if (0x0020 <= char_code <= 0x007E or
                0xAC00 <= char_code <= 0xD7AF or
                0x3130 <= char_code <= 0x318F or
                0xFF00 <= char_code <= 0xFFEF or
                0x2000 <= char_code <= 0x206F):
                parts.append(chr(char_code))
    return "".join(parts)


def _build_hwp5_table_markdown(records, start_idx):
    _, table_level, table_data = records[start_idx]
    if len(table_data) >= 8:
        n_rows = struct.unpack_from('<H', table_data, 4)[0]
        n_cols = struct.unpack_from('<H', table_data, 6)[0]
    else:
        return None, start_idx + 1
    if n_rows == 0 or n_cols == 0:
        return None, start_idx + 1

    cells = {}
    current_cell = None
    cell_texts: List[str] = []
    cell_seq = 0
    i = start_idx + 1
    while i < len(records):
        tag_id, level, data = records[i]
        if level < table_level:
            break
        if tag_id == TAG_LIST_HEADER and level == table_level:
            if current_cell is not None:
                cells[current_cell] = " ".join(cell_texts).strip()
            cell_texts = []
            if len(data) >= 16:
                col_addr = struct.unpack_from('<H', data, 8)[0]
                row_addr = struct.unpack_from('<H', data, 10)[0]
                current_cell = (row_addr, col_addr)
            else:
                current_cell = (cell_seq // n_cols, cell_seq % n_cols)
            cell_seq += 1
        elif tag_id == TAG_PARA_TEXT and current_cell is not None:
            text = _extract_para_text(data)
            if text.strip():
                cell_texts.append(text.strip())
        i += 1

    if current_cell is not None:
        cells[current_cell] = " ".join(cell_texts).strip()
    if not cells:
        return None, i

    actual_rows = max(max(r for r, c in cells) + 1, n_rows)
    actual_cols = max(max(c for r, c in cells) + 1, n_cols)
    grid = [["" for _ in range(actual_cols)] for _ in range(actual_rows)]
    for (r, c), text in cells.items():
        if 0 <= r < actual_rows and 0 <= c < actual_cols:
            grid[r][c] = text

    cw = [max(3, *(len(grid[r][c]) for r in range(actual_rows))) for c in range(actual_cols)]
    lines = []
    for r in range(actual_rows):
        lines.append("| " + " | ".join(grid[r][c].ljust(cw[c]) for c in range(actual_cols)) + " |")
        if r == 0:
            lines.append("| " + " | ".join("-" * cw[c] for c in range(actual_cols)) + " |")
    return "\n".join(lines), i


def _process_hwp5_section(data: bytes) -> str:
    records = _parse_hwp5_records(data)
    blocks = []
    i = 0
    table_end_idx = -1
    while i < len(records):
        tag_id, level, rec_data = records[i]
        if tag_id == TAG_TABLE:
            md_table, next_i = _build_hwp5_table_markdown(records, i)
            if md_table:
                blocks.append(('table', md_table))
            table_end_idx = next_i
            i = next_i
            continue
        if tag_id == TAG_PARA_TEXT and i >= table_end_idx:
            text = _extract_para_text(rec_data).strip()
            if text:
                blocks.append(('text', text))
        i += 1

    parts = []
    for idx, (btype, content) in enumerate(blocks):
        if idx == 0:
            parts.append(content)
            continue
        prev_type = blocks[idx - 1][0]
        if prev_type != btype or btype == 'table':
            parts.append('\n\n')
        else:
            parts.append('\n')
        parts.append(content)
    return "".join(parts)


# ─── DOCX 파싱 (표→마크다운) ────────────────────────────────────────

def _parse_docx(file_path: str) -> str:
    try:
        import docx
    except ImportError:
        logger.warning("python-docx 미설치 — DOCX 파싱 불가")
        return ""

    doc = docx.Document(file_path)
    parts: List[str] = []

    for element in doc.element.body:
        tag = element.tag.split('}')[-1] if '}' in element.tag else element.tag
        if tag == 'tbl':
            # 표 → 마크다운
            table = None
            for t in doc.tables:
                if t._element is element:
                    table = t
                    break
            if table:
                rows_data = []
                for row in table.rows:
                    rows_data.append([cell.text.strip() for cell in row.cells])
                if rows_data:
                    md = _table_to_markdown(rows_data)
                    if md:
                        parts.append(md)
        elif tag == 'p':
            # 단락
            from docx.oxml.ns import qn
            texts = []
            for r in element.findall(qn('w:r')):
                t = r.find(qn('w:t'))
                if t is not None and t.text:
                    texts.append(t.text)
            line = "".join(texts).strip()
            if line:
                parts.append(line)

    return "\n\n".join(parts)


# ─── TXT / MD 파싱 ──────────────────────────────────────────────────

def _parse_text(file_path: str) -> str:
    encodings = ["utf-8", "cp949", "euc-kr", "latin-1"]
    for enc in encodings:
        try:
            return Path(file_path).read_text(encoding=enc)
        except (UnicodeDecodeError, UnicodeError):
            continue
    return ""


# ─── 통합 파서 디스패처 ─────────────────────────────────────────────

PARSERS = {
    ".pdf": _parse_pdf,
    ".hwpx": _parse_hwpx,
    ".hwp": _parse_hwp,
    ".docx": _parse_docx,
    ".doc": _parse_docx,
    ".txt": _parse_text,
    ".md": _parse_text,
    ".csv": _parse_text,
}


def parse_file(file_path: str) -> str:
    ext = Path(file_path).suffix.lower()
    parser = PARSERS.get(ext)
    if not parser:
        logger.warning("지원하지 않는 파일 형식: %s", ext)
        return ""
    try:
        return parser(file_path)
    except Exception as e:
        logger.error("파일 파싱 실패 (%s): %s", file_path, e)
        return ""


# ─── LLM 요약 ───────────────────────────────────────────────────────

def _summarize_chunk_sync(text: str, file_name: str) -> str:
    """단일 청크를 경량 LLM으로 요약한다. (동기 함수)"""
    from .llm import chat_text_with

    system_prompt = (
        "당신은 문서 청크 요약 전문가입니다. "
        "아래 문서 청크의 내용을 2-3문장으로 요약하세요. "
        "이 요약은 나중에 검색에 사용되므로, "
        "이 청크에 어떤 종류의 정보가 있는지 구체적으로 명시하세요. "
        "숫자, 기관명, 기술명 등 핵심 키워드를 반드시 포함하세요."
    )
    user_prompt = f"파일명: {file_name}\n\n---\n\n{text[:3000]}"

    try:
        return chat_text_with(system_prompt, user_prompt, temperature=0.2, max_tokens=200).strip()
    except Exception as e:
        logger.warning("청크 요약 실패: %s", e)
        return text[:200]


async def _summarize_chunk(text: str, file_name: str, sem: asyncio.Semaphore) -> str:
    async with sem:
        return await asyncio.to_thread(_summarize_chunk_sync, text, file_name)


# ─── 메인 파이프라인 ────────────────────────────────────────────────

async def parse_and_chunk_sources(
    source_items: List[Dict[str, Any]],
    max_concurrent_summary: int = SUMMARY_MAX_CONCURRENCY,
    on_progress: Optional[Any] = None,
) -> List[SourceChunk]:
    """
    소스 파일 목록을 파싱 + 청크 분할 + LLM 요약하여 SourceChunk 리스트를 반환한다.

    Args:
        source_items: proc_inst_source 레코드 리스트 [{file_name, file_path, ...}]
        max_concurrent_summary: LLM 요약 동시 호출 수 제한
        on_progress: 콜백 함수 (stage: str, detail: str) → 진행 상태 보고

    Returns:
        SourceChunk 리스트 (메모리에 보관하여 hwpx-mcp에 전달)
    """
    def _emit(stage: str, detail: str) -> None:
        if on_progress:
            on_progress(stage, detail)

    if not source_items:
        return []

    total_files = len(source_items)
    parse_failed: List[str] = []

    # 1단계: 파일 다운로드 + 파싱
    all_chunks: List[Tuple[str, int, str]] = []  # (file_name, chunk_idx, text)

    for file_idx, item in enumerate(source_items, start=1):
        file_name = item.get("file_name") or ""
        file_url = item.get("file_path") or ""
        if not file_url:
            continue

        ext = Path(file_name).suffix.lower()
        if ext not in PARSERS:
            logger.info("지원하지 않는 파일 형식 건너뜀: %s (%s)", file_name, ext)
            parse_failed.append(f"{file_name} (미지원 형식)")
            continue

        _emit("참고자료 파싱 중", f"{file_idx}/{total_files} 파일 — {file_name}")
        logger.info("[소스파싱] 파일 다운로드 시작: %s", file_name)
        try:
            tmp_path = _download_file(file_url, suffix=ext)
        except Exception as e:
            logger.warning("[소스파싱] 다운로드 실패: %s — %s", file_name, e)
            parse_failed.append(f"{file_name} (다운로드 실패)")
            continue

        try:
            text = parse_file(str(tmp_path))
            if not text or not text.strip():
                logger.info("[소스파싱] 파싱 결과 비어있음: %s", file_name)
                parse_failed.append(f"{file_name} (파싱 실패)")
                continue
            logger.info("[소스파싱] 파싱 완료: %s (길이=%d)", file_name, len(text))

            chunks = _split_text(text)
            logger.info("[소스파싱] 청크 분할: %s → %d개", file_name, len(chunks))
            for i, chunk_text in enumerate(chunks):
                all_chunks.append((file_name, i, chunk_text))
        finally:
            try:
                os.unlink(str(tmp_path))
            except OSError:
                pass

    if parse_failed:
        _emit("참고자료 파싱 경고", f"{len(parse_failed)}개 파일 파싱 실패: {', '.join(parse_failed)}")

    if not all_chunks:
        logger.info("[소스파싱] 파싱된 청크 없음")
        return []

    # 대용량 경고
    total_chunks = len(all_chunks)
    if total_chunks > 200:
        logger.warning("[소스파싱] ⚠ 청크 수 %d개 — 대용량 소스. LLM 비용 주의.", total_chunks)
        _emit("참고자료 경고", f"대용량 소스: {total_chunks}개 청크 (요약에 시간이 걸릴 수 있습니다)")

    # 2단계: LLM 요약 (동시성 제어)
    _emit("참고자료 요약 중", f"0/{total_chunks} 청크")
    logger.info("[소스파싱] LLM 요약 시작: %d개 청크", total_chunks)
    sem = asyncio.Semaphore(max_concurrent_summary)
    done_count = 0
    done_lock = asyncio.Lock()

    async def _process_one(file_name: str, chunk_idx: int, text: str) -> SourceChunk:
        nonlocal done_count
        summary = await _summarize_chunk(text, file_name, sem)
        async with done_lock:
            done_count += 1
            if done_count % 5 == 0 or done_count == total_chunks:
                _emit("참고자료 요약 중", f"{done_count}/{total_chunks} 청크")
        return SourceChunk(
            file_name=file_name,
            chunk_index=chunk_idx,
            original_text=text,
            summary=summary,
        )

    tasks = [_process_one(fn, ci, txt) for fn, ci, txt in all_chunks]
    results = await asyncio.gather(*tasks)
    _emit("참고자료 준비 완료", f"{len(results)}개 청크 준비됨")
    logger.info("[소스파싱] LLM 요약 완료: %d개 SourceChunk 생성", len(results))
    return list(results)


def source_chunks_to_json(chunks: List[SourceChunk]) -> str:
    """SourceChunk 리스트를 JSON 문자열로 직렬화한다."""
    return json.dumps([c.to_dict() for c in chunks], ensure_ascii=False)


def source_chunks_from_json(json_str: str) -> List[Dict[str, Any]]:
    """JSON 문자열에서 소스 청크 리스트를 파싱한다."""
    try:
        return json.loads(json_str)
    except (json.JSONDecodeError, TypeError):
        return []
