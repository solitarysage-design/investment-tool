"""設定ファイル: .env から読み込む"""
from pathlib import Path
from dotenv import load_dotenv
import os

# .env ファイルを読み込む
env_path = Path(__file__).parent / ".env"
load_dotenv(env_path)

# J-Quants API 認証情報
JQUANTS_EMAIL: str = os.getenv("JQUANTS_EMAIL", "")
JQUANTS_PASSWORD: str = os.getenv("JQUANTS_PASSWORD", "")

# ファイルパス（このファイルからの相対パス or 絶対パス）
_base = Path(__file__).parent
PDF_PATH: Path = Path(os.getenv("PDF_PATH", str(_base / "data" / "holdings.pdf")))
OUTPUT_DIR: Path = Path(os.getenv("OUTPUT_DIR", str(_base / "data" / "output")))

# スクリーニング条件
SCREEN_PBR_MAX: float = float(os.getenv("SCREEN_PBR_MAX", "1.5"))
SCREEN_YIELD_MIN: float = float(os.getenv("SCREEN_YIELD_MIN", "2.5"))
SCREEN_MARKET_CAP_MIN: float = float(os.getenv("SCREEN_MARKET_CAP_MIN", "10000000000"))
SCREEN_DIVIDEND_CUT_YEARS: int = int(os.getenv("SCREEN_DIVIDEND_CUT_YEARS", "3"))

def validate():
    """設定値の基本チェック"""
    errors = []
    if not JQUANTS_EMAIL:
        errors.append("JQUANTS_EMAIL が未設定です")
    if not JQUANTS_PASSWORD:
        errors.append("JQUANTS_PASSWORD が未設定です")
    if errors:
        raise ValueError("設定エラー:\n" + "\n".join(f"  - {e}" for e in errors))
