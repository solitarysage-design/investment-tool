"""
楽天証券「保有商品一覧」PDF 解析モジュール

楽天証券のPDFフォーマット（国内株式セクション）を解析して
保有銘柄情報をDataFrameに変換する。

列構成（楽天証券標準）:
  銘柄コード | 銘柄名 | 口座 | 保有株数 | 平均取得単価 | 現在値 | 評価額 | 評価損益 | 評価損益率(%)

※ PDFのバージョンや表示設定によって列順が変わる場合があります。
  その場合は parse_rakuten_pdf() 内の COLUMN_PATTERNS を調整してください。
"""

import re
import logging
from pathlib import Path

import pdfplumber
import pandas as pd

logger = logging.getLogger(__name__)

# ▲ や △ はマイナス（含み損）を意味する
_MINUS_PREFIXES = ("▲", "△", "－", "−")

# 楽天証券の列名パターン（部分一致・複数バージョン対応）
_COL_PATTERNS = {
    "code":            ["銘柄コード", "コード", "証券コード"],
    "name":            ["銘柄名", "銘柄", "銘柄・ファンド名"],
    "account_type":    ["口座区分", "口座", "口座種別"],
    "quantity":        ["保有株数", "保有数量", "数量", "保有口数"],
    "avg_cost":        ["平均取得単価", "取得単価", "取得価格", "平均取得価格"],
    "current_price":   ["現在値", "株価", "基準価額"],
    "assessed_value":  ["評価額", "時価評価額"],
    "unrealized_pl":   ["評価損益(円)", "評価損益額", "評価損益", "損益(円)", "損益額"],
    "unrealized_pct":  ["評価損益率", "損益率", "損益(%)"],
}


# ---------------------------------------------------------------------------
# 公開関数
# ---------------------------------------------------------------------------

def parse_rakuten_pdf(pdf_path: str | Path) -> pd.DataFrame:
    """
    楽天証券「保有商品一覧」PDFを解析して国内株式保有銘柄を返す。

    Returns
    -------
    pd.DataFrame
        columns: code, name, account_type, quantity, avg_cost,
                 current_price, assessed_value, unrealized_pl, unrealized_pct
    """
    path = Path(pdf_path)
    if not path.exists():
        raise FileNotFoundError(f"PDFが見つかりません: {path.resolve()}")

    logger.info(f"PDF解析開始: {path.name}")

    with pdfplumber.open(path) as pdf:
        records = _try_table_extraction(pdf)
        if not records:
            logger.warning("テーブル抽出失敗 → テキスト解析にフォールバック")
            records = _try_text_extraction(pdf)

    if not records:
        raise ValueError(
            "PDFから保有銘柄を抽出できませんでした。\n"
            "  ・楽天証券の「保有商品一覧」PDFか確認してください\n"
            "  ・PDFがパスワード保護されていないか確認してください\n"
            "  ・問題が続く場合は pdf_parser.py の解析ロジックを調整してください"
        )

    df = pd.DataFrame(records)
    df = _clean_dataframe(df)
    logger.info(f"  → {len(df)} 銘柄を取得")
    return df


def save_to_csv(df: pd.DataFrame, output_path: str | Path) -> None:
    """保有銘柄DataFrameをCSVに保存"""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(output_path, index=False, encoding="utf-8-sig")
    logger.info(f"CSVに保存: {output_path}")


# ---------------------------------------------------------------------------
# 内部関数: テーブル抽出
# ---------------------------------------------------------------------------

def _try_table_extraction(pdf: pdfplumber.PDF) -> list[dict]:
    """
    pdfplumber でテーブル抽出。
    楽天証券PDFは罫線のないレイアウトが多いため、4つの戦略を順に試みる。
    """
    # 戦略リスト（strict → 緩い順）
    STRATEGIES = [
        {"vertical_strategy": "lines_strict", "horizontal_strategy": "lines_strict",
         "intersection_tolerance": 5},
        {"vertical_strategy": "lines",        "horizontal_strategy": "lines",
         "intersection_tolerance": 5},
        {"vertical_strategy": "text",         "horizontal_strategy": "lines",
         "text_tolerance": 5},
        {"vertical_strategy": "text",         "horizontal_strategy": "text",
         "text_tolerance": 5, "text_x_tolerance": 3},
    ]

    records = []
    for page_num, page in enumerate(pdf.pages):
        page_text = page.extract_text() or ""

        for strategy in STRATEGIES:
            try:
                tables = page.extract_tables(table_settings=strategy)
            except Exception:
                tables = []

            found_in_page = []
            for table in tables:
                if not table or len(table) < 2:
                    continue
                header_idx, col_map = _find_header(table)
                if header_idx is None or not col_map:
                    continue
                for row in table[header_idx + 1:]:
                    if not row:
                        continue
                    rec = _row_to_record(row, col_map)
                    if rec:
                        found_in_page.append(rec)

            if found_in_page:
                records.extend(found_in_page)
                break  # このページは成功したので次の戦略は不要

    return records


def _find_header(table: list[list]) -> tuple[int | None, dict]:
    """
    テーブルのヘッダー行を特定し、列インデックスマッピングを返す。
    Returns (header_row_index, {field_name: col_index})
    """
    for i, row in enumerate(table[:5]):  # 先頭5行以内にヘッダーがあるはず
        if not row:
            continue
        cells = [str(c).strip() if c else "" for c in row]
        col_map = {}
        for field, patterns in _COL_PATTERNS.items():
            for j, cell in enumerate(cells):
                if any(pat in cell for pat in patterns):
                    col_map[field] = j
                    break
        # "code" か "name" が見つかればヘッダーと判断
        if "code" in col_map or "name" in col_map:
            return i, col_map
    return None, {}


def _row_to_record(row: list, col_map: dict) -> dict | None:
    """テーブル行を辞書に変換。銘柄コードがなければNoneを返す"""
    def cell(field):
        idx = col_map.get(field)
        if idx is None or idx >= len(row):
            return None
        return row[idx]

    # 銘柄コード: 4桁数字
    code_raw = str(cell("code") or "").strip()
    if not re.match(r"^\d{4}$", code_raw):
        # code列が特定できていない場合、行全体から4桁コードを探す
        for c in row:
            if c and re.match(r"^\d{4}$", str(c).strip()):
                code_raw = str(c).strip()
                break
        else:
            return None

    name_raw = str(cell("name") or "").strip()
    if not name_raw or name_raw == code_raw:
        return None

    return {
        "code":           code_raw,
        "name":           name_raw,
        "account_type":   str(cell("account_type") or "").strip(),
        "quantity":       _to_float(cell("quantity")),
        "avg_cost":       _to_float(cell("avg_cost")),
        "current_price":  _to_float(cell("current_price")),
        "assessed_value": _to_float(cell("assessed_value")),
        "unrealized_pl":  _to_float(cell("unrealized_pl")),
        "unrealized_pct": _to_float(cell("unrealized_pct")),
    }


# ---------------------------------------------------------------------------
# 内部関数: テキスト抽出（フォールバック）
# ---------------------------------------------------------------------------

def _try_text_extraction(pdf: pdfplumber.PDF) -> list[dict]:
    """
    PDFテキストから銘柄情報を抽出するフォールバック。
    楽天証券のPDFは値が改行で区切られていることがあるため、
    「4桁コード行」を起点に前後の行から数値を収集する。
    """
    records = []
    full_text = ""
    for page in pdf.pages:
        t = page.extract_text()
        if t:
            full_text += t + "\n"

    lines = full_text.split("\n")

    # --- パターン1: 1行にすべてまとまっている場合 ---
    pattern_oneline = re.compile(
        r"(\d{4})\s+"
        r"([^\d▲△\n]+?)\s+"
        r"(?:(特定|一般|NISA|つみたてNISA|成長投資枠|特定口座|一般口座)\s+)?"
        r"([\d,]+)\s+"
        r"([\d,]+(?:\.\d+)?)\s+"
        r"([\d,]+(?:\.\d+)?)\s+"
        r"([\d,]+)\s+"
        r"([▲△]?[\d,]+(?:\.\d+)?)\s+"
        r"([▲△]?[\d.]+)"
    )
    for m in pattern_oneline.finditer(full_text):
        records.append({
            "code":           m.group(1),
            "name":           m.group(2).strip(),
            "account_type":   m.group(3) or "",
            "quantity":       _to_float(m.group(4)),
            "avg_cost":       _to_float(m.group(5)),
            "current_price":  _to_float(m.group(6)),
            "assessed_value": _to_float(m.group(7)),
            "unrealized_pl":  _to_float(m.group(8)),
            "unrealized_pct": _to_float(m.group(9)),
        })

    if records:
        return records

    # --- パターン2: 4桁コード行を起点に周辺行から数値を収集 ---
    # 楽天証券の一部PDFは各値が別行になっている
    i = 0
    while i < len(lines):
        line = lines[i].strip()
        m = re.match(r"^(\d{4})$", line)  # 4桁コードだけの行
        if not m:
            # コードが行頭にある場合も対応
            m = re.match(r"^(\d{4})\s+(\S.*)$", line)
        if m:
            code = m.group(1)
            # 前後10行から銘柄名と数値を収集
            window = lines[max(0, i-2):min(len(lines), i+12)]
            name = _extract_name_from_window(window, code)
            numbers = _extract_numbers_from_window(window)

            if name and len(numbers) >= 4:
                records.append({
                    "code":           code,
                    "name":           name,
                    "account_type":   _extract_account_type(window),
                    "quantity":       numbers[0] if len(numbers) > 0 else None,
                    "avg_cost":       numbers[1] if len(numbers) > 1 else None,
                    "current_price":  numbers[2] if len(numbers) > 2 else None,
                    "assessed_value": numbers[3] if len(numbers) > 3 else None,
                    "unrealized_pl":  numbers[4] if len(numbers) > 4 else None,
                    "unrealized_pct": numbers[5] if len(numbers) > 5 else None,
                })
        i += 1

    return records


def _extract_name_from_window(lines: list[str], code: str) -> str:
    """周辺行から銘柄名を探す"""
    for line in lines:
        line = line.strip()
        if re.match(r"^\d{4}", line):
            # コード行の残り部分
            rest = re.sub(r"^\d{4}\s*", "", line).strip()
            # 先頭の日本語/英字部分を銘柄名と見なす
            m = re.match(r"^([^\d▲△,]{2,40})", rest)
            if m:
                return m.group(1).strip()
        # 日本語が多い行を銘柄名候補と見なす
        jp_count = sum(1 for c in line if '\u3000' <= c <= '\u9fff' or '\uff00' <= c <= '\uffef')
        if jp_count >= 2 and not re.search(r"[\d,]{4,}", line):
            return line[:40].strip()
    return ""


def _extract_numbers_from_window(lines: list[str]) -> list[float | None]:
    """周辺行から数値を順に収集する"""
    numbers = []
    for line in lines:
        line = line.strip()
        # ▲/△ をマイナスに変換して数値抽出
        is_neg = line.startswith(("▲", "△"))
        cleaned = re.sub(r"[▲△,\s円%]", "", line)
        try:
            val = float(cleaned)
            numbers.append(-val if is_neg else val)
        except ValueError:
            # 数値と非数値が混在する行（例: "1,000 特定"）からも抽出
            for part in re.findall(r"[▲△]?[\d,]+(?:\.\d+)?", line):
                neg = part.startswith(("▲", "△"))
                try:
                    numbers.append(-float(re.sub(r"[▲△,]", "", part)) if neg
                                   else float(re.sub(r",", "", part)))
                except ValueError:
                    pass
    return numbers


def _extract_account_type(lines: list[str]) -> str:
    """口座区分を周辺行から探す"""
    for line in lines:
        for kw in ("特定", "一般", "NISA", "つみたて", "成長投資"):
            if kw in line:
                return kw
    return ""


# ---------------------------------------------------------------------------
# ユーティリティ
# ---------------------------------------------------------------------------

def _to_float(value) -> float | None:
    """数値文字列を float に変換。▲/△ はマイナス"""
    if value is None:
        return None
    text = str(value).strip()
    if text in ("", "-", "―", "−"):
        return None
    is_negative = text.startswith(_MINUS_PREFIXES)
    cleaned = re.sub(r"[,円¥%\s▲△－−]", "", text)
    try:
        return -float(cleaned) if is_negative else float(cleaned)
    except ValueError:
        return None


def _clean_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """DataFrameのクリーニングと型変換"""
    # 重複除去（同コードが複数ページにまたがって抽出された場合）
    df = df.drop_duplicates(subset=["code"])

    # 数値列の型変換
    numeric_cols = ["quantity", "avg_cost", "current_price",
                    "assessed_value", "unrealized_pl", "unrealized_pct"]
    for col in numeric_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # assessed_value が空の場合は計算で補完
    mask = df["assessed_value"].isna() & df["quantity"].notna() & df["current_price"].notna()
    df.loc[mask, "assessed_value"] = df.loc[mask, "quantity"] * df.loc[mask, "current_price"]

    return df.reset_index(drop=True)
