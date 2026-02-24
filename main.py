"""
増配バリュー株 週次投資判断ツール - メインスクリプト

使い方:
  1. .env ファイルに J-Quants 認証情報と PDF_PATH を設定
  2. 楽天証券の「保有商品一覧」PDFを PDF_PATH に置く
  3. python main.py を実行
  4. data/output/ に Excel ファイルが生成される
"""

import logging
import sys
from datetime import datetime
from pathlib import Path

import config
from pdf_parser import parse_rakuten_pdf, parse_rakuten_excel, save_to_csv
from jquants_api import JQuantsClient, JQuantsScreener, enrich_holdings
from excel_generator import create_investment_excel


# ---------------------------------------------------------------------------
# ログ設定
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler(
            Path(__file__).parent / "data" / "output" / "run.log",
            encoding="utf-8",
        ),
    ],
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# メイン処理
# ---------------------------------------------------------------------------

def main():
    print("=" * 60)
    print("  増配バリュー株 週次投資判断ツール")
    print(f"  実行日時: {datetime.now().strftime('%Y年%m月%d日 %H:%M')}")
    print("=" * 60)

    # --- 設定チェック ---
    try:
        config.validate()
    except ValueError as e:
        print(f"\n[エラー] {e}")
        print("\n.env ファイルを確認してください（.env.example を参考に）")
        sys.exit(1)

    output_dir = config.OUTPUT_DIR
    output_dir.mkdir(parents=True, exist_ok=True)

    # ================================================================
    # STEP 1: PDF解析
    # ================================================================
    print(f"\n[STEP 1] PDF解析 → {config.PDF_PATH}")

    holdings_df = None
    holdings_codes = []

    if config.PDF_PATH.exists():
        try:
            suffix = config.PDF_PATH.suffix.lower()
            if suffix == ".pdf":
                holdings_df = parse_rakuten_pdf(config.PDF_PATH)
            else:
                holdings_df = parse_rakuten_excel(config.PDF_PATH)
            print(f"  保有銘柄: {len(holdings_df)} 銘柄")
            holdings_codes = holdings_df["code"].tolist()

            # CSV保存
            csv_path = output_dir / "holdings.csv"
            save_to_csv(holdings_df, csv_path)
            print(f"  CSVに保存: {csv_path.name}")

        except Exception as e:
            print(f"  [警告] PDF解析に失敗しました: {e}")
            print("  保有銘柄なしで続行します（スクリーニングのみ実行）")
    else:
        print(f"  [警告] PDFが見つかりません: {config.PDF_PATH}")
        print("  スクリーニングのみ実行します")

    import pandas as pd
    if holdings_df is None:
        holdings_df = pd.DataFrame()

    # ================================================================
    # STEP 2: J-Quants スクリーニング
    # ================================================================
    print(f"\n[STEP 2] J-Quants スクリーニング")
    print(f"  条件: PBR≦{config.SCREEN_PBR_MAX}倍 / 配当利回り≧{config.SCREEN_YIELD_MIN}%"
          f" / 時価総額≧{config.SCREEN_MARKET_CAP_MIN / 1e8:.0f}億 / 減配なし（{config.SCREEN_DIVIDEND_CUT_YEARS}年）")

    try:
        client = JQuantsClient(config.JQUANTS_EMAIL, config.JQUANTS_PASSWORD)
        screener = JQuantsScreener(
            client=client,
            pbr_max=config.SCREEN_PBR_MAX,
            yield_min=config.SCREEN_YIELD_MIN,
            market_cap_min=config.SCREEN_MARKET_CAP_MIN,
            div_cut_years=config.SCREEN_DIVIDEND_CUT_YEARS,
        )

        jq_holdings_raw, candidates_df = screener.run(holdings_codes=holdings_codes)

        if client.subscription_end:
            days_old = (datetime.now() - client.subscription_end).days
            print(f"\n  ⚠️  注意: Lightプランのため {client.subscription_end.strftime('%Y-%m-%d')} 時点の"
                  f"データを使用しています（約{days_old}日前）")

        # 保有銘柄に J-Quants データを付加
        if not holdings_df.empty and not jq_holdings_raw.empty:
            holdings_df = enrich_holdings(holdings_df, jq_holdings_raw)

        print(f"  候補銘柄: {len(candidates_df)} 銘柄")

    except Exception as e:
        logger.exception("J-Quants APIエラー")
        print(f"\n[エラー] J-Quants API呼び出しに失敗しました: {e}")
        print("  API認証情報・ネットワーク接続を確認してください")
        candidates_df = pd.DataFrame()

    # ================================================================
    # STEP 3: Excel 生成
    # ================================================================
    today_str = datetime.now().strftime("%Y%m%d")
    excel_filename = f"投資判断シート_{today_str}.xlsx"
    excel_path = output_dir / excel_filename

    print(f"\n[STEP 3] Excel生成 → {excel_path}")

    try:
        saved_path = create_investment_excel(
            holdings_df=holdings_df,
            candidates_df=candidates_df,
            output_path=excel_path,
        )
        print(f"  完了: {saved_path.resolve()}")
    except Exception as e:
        logger.exception("Excel生成エラー")
        print(f"\n[エラー] Excel生成に失敗しました: {e}")
        sys.exit(1)

    # ================================================================
    # 完了
    # ================================================================
    print("\n" + "=" * 60)
    print(f"  完了！  {excel_path.resolve()}")
    print("=" * 60)

    # サマリー表示
    if not candidates_df.empty and "div_yield" in candidates_df.columns:
        print(f"\n【候補銘柄トップ5（配当利回り順）】")
        top5 = candidates_df.nlargest(5, "div_yield")[
            ["code", "name", "div_yield", "pbr"]
        ]
        for _, r in top5.iterrows():
            yield_pct = r["div_yield"]
            if yield_pct is not None and yield_pct <= 1:
                yield_pct *= 100
            print(f"  {r['code']} {r.get('name', '')[:15]:15s}"
                  f"  利回り {yield_pct:.2f}%  PBR {r.get('pbr', 0):.2f}倍")


if __name__ == "__main__":
    main()
