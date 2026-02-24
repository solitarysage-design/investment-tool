"""
J-Quants API クライアント & スクリーニングモジュール

スクリーニング条件:
  - PBR 1.5倍以下
  - 配当利回り 2.5%以上
  - 時価総額 100億円以上
  - 直近3年間で減配なし

J-Quants APIドキュメント: https://jpx-jquants.com/
無料のLightプランで利用可能（一部データに制限あり）
"""

import logging
import time
from datetime import datetime, timedelta

import pandas as pd
import requests

logger = logging.getLogger(__name__)

_BASE_URL = "https://api.jquants.com/v1"

# TypeOfCurrentPeriod の優先順位（年次 > 半期 > 四半期）
_PERIOD_PRIORITY = {"FY": 0, "2Q": 1, "Q3": 2, "Q1": 3}


class JQuantsClient:
    """J-Quants API クライアント"""

    def __init__(self, email: str, password: str):
        self.email = email
        self.password = password
        self.id_token: str = ""
        self.subscription_end: datetime | None = None  # プランのデータ上限日
        self._authenticate()

    # ------------------------------------------------------------------
    # 認証
    # ------------------------------------------------------------------

    def _authenticate(self):
        """リフレッシュトークン → IDトークン の2段階認証"""
        # Step 1: リフレッシュトークン取得
        resp = requests.post(
            f"{_BASE_URL}/token/auth_user",
            json={"mailaddress": self.email, "password": self.password},
            timeout=30,
        )
        resp.raise_for_status()
        refresh_token = resp.json()["refreshToken"]

        # Step 2: IDトークン取得
        resp = requests.post(
            f"{_BASE_URL}/token/auth_refresh",
            params={"refreshtoken": refresh_token},
            timeout=30,
        )
        resp.raise_for_status()
        self.id_token = resp.json()["idToken"]
        logger.info("J-Quants 認証成功")

    def _get(self, endpoint: str, params: dict | None = None) -> dict:
        """認証付きGETリクエスト（リトライ付き）"""
        headers = {"Authorization": f"Bearer {self.id_token}"}
        url = f"{_BASE_URL}/{endpoint}"
        last_exc: Exception | None = None

        for attempt in range(4):
            try:
                resp = requests.get(url, headers=headers, params=params, timeout=60)
                if resp.status_code == 429:
                    wait = 60 * (attempt + 1)
                    logger.warning(f"レート制限 → {wait}秒待機")
                    time.sleep(wait)
                    continue
                # 4xx/5xx を例外化する前にレスポンスボディをログ
                if not resp.ok:
                    logger.warning(
                        f"HTTP {resp.status_code} [{endpoint}] body={resp.text[:200]}"
                    )
                resp.raise_for_status()
                return resp.json()
            except requests.exceptions.HTTPError as e:
                last_exc = e
                # 4xx は基本リトライしない（5xx のみリトライ）
                if resp.status_code < 500:
                    raise
                logger.warning(f"サーバーエラー {resp.status_code}、リトライ ({attempt+1}/4)")
                time.sleep(10 * (attempt + 1))
            except (requests.exceptions.Timeout, requests.exceptions.ConnectionError) as e:
                last_exc = e
                logger.warning(f"通信エラー (試行 {attempt+1}/4): {e}")
                time.sleep(10)

        raise RuntimeError(f"APIリクエスト失敗 [{endpoint}]: {last_exc}")

    def _get_paginated(self, endpoint: str, key: str, params: dict | None = None) -> list:
        """ページネーション対応のGETリクエスト"""
        params = params or {}
        results = []
        while True:
            data = self._get(endpoint, params)
            results.extend(data.get(key, []))
            pagination_key = data.get("pagination_key")
            if not pagination_key:
                break
            params = {**params, "pagination_key": pagination_key}
            time.sleep(0.2)
        return results

    # ------------------------------------------------------------------
    # データ取得
    # ------------------------------------------------------------------

    def get_listed_info(self) -> pd.DataFrame:
        """全上場銘柄情報を取得"""
        items = self._get_paginated("listed/info", "info")
        df = pd.DataFrame(items)
        logger.info(f"上場銘柄数: {len(df)}")
        return df

    def get_daily_quotes(self, date: str) -> pd.DataFrame:
        """指定日の全銘柄株価を取得 (YYYYMMDD)"""
        items = self._get_paginated("prices/daily_quotes", "daily_quotes", {"date": date})
        return pd.DataFrame(items)

    def get_statements_for_date(self, date: str) -> list[dict]:
        """指定開示日の財務諸表を全銘柄分取得"""
        return self._get_paginated("fins/statements", "statements", {"date": date})

    def get_statements_for_code(self, code: str) -> pd.DataFrame:
        """特定銘柄の財務諸表を全期間取得（減配チェック用）"""
        items = self._get_paginated("fins/statements", "statements", {"code": code})
        return pd.DataFrame(items)

    def get_latest_trading_date(self) -> str:
        """
        最新の取引日を取得。
        Lightプランなどでデータ上限日が存在する場合は、
        エラーメッセージから上限日を自動検出してその日付から検索する。
        """
        import re as _re
        from datetime import timezone
        JST = timezone(timedelta(hours=9))
        today = datetime.now(JST).replace(tzinfo=None)

        # まず今日の日付で試して、サブスクリプション上限日を検出する
        search_from = today
        try:
            data = self._get("prices/daily_quotes", {"date": today.strftime("%Y%m%d")})
            if data.get("daily_quotes"):
                return today.strftime("%Y%m%d")
        except requests.exceptions.HTTPError as e:
            if e.response is not None and e.response.status_code == 400:
                # "covers the following dates: 2023-12-02 ~ 2025-12-02" から上限日を取得
                found = _re.findall(r"\d{4}-\d{2}-\d{2}", e.response.text)
                if len(found) >= 2:
                    sub_end = datetime.strptime(found[-1], "%Y-%m-%d")
                    logger.warning(
                        f"プランのデータ対象期間: {found[0]} 〜 {found[-1]}\n"
                        f"  → {found[-1]} 時点のデータを使用します（注意: 約{(today - sub_end).days}日前のデータ）"
                    )
                    self.subscription_end = sub_end
                    search_from = sub_end
        except Exception:
            pass

        # search_from から最大14日遡って最新の取引日を探す
        errors: list[str] = []
        for i in range(14):
            candidate = (search_from - timedelta(days=i)).strftime("%Y%m%d")
            try:
                data = self._get("prices/daily_quotes", {"date": candidate})
                if data.get("daily_quotes"):
                    logger.info(f"最新取引日: {candidate}")
                    return candidate
                logger.debug(f"{candidate}: daily_quotes が空（休場日）")
            except Exception as e:
                errors.append(f"{candidate}: {e}")
                logger.warning(f"取引日チェック失敗 {candidate}: {e}")
            time.sleep(0.3)

        detail = "\n".join(errors[-5:]) if errors else "なし"
        raise RuntimeError(
            f"最新取引日の取得に失敗しました（{search_from.strftime('%Y-%m-%d')} から14日間）\n"
            f"直近エラー:\n{detail}"
        )

    # ------------------------------------------------------------------
    # バルク財務データ収集
    # ------------------------------------------------------------------

    def collect_recent_statements(self, days_back: int = 120) -> pd.DataFrame:
        """
        直近 days_back 日間の財務諸表を収集し、
        各銘柄の最新データのみを返す。

        NOTE: fins/statements の date パラメータは「開示日」を指定する。
              決算発表が集中する期間（5月・8月・11月・2月）をカバーするため
              一定期間をスキャンする。
        """
        # subscription_end があればそこから、なければ今日から起算
        today = self.subscription_end or datetime.now()
        all_statements: list[dict] = []

        # 週次でサンプリング（APIコール数を抑える）
        scan_dates = []
        for i in range(0, days_back, 3):
            d = (today - timedelta(days=i))
            if d.weekday() < 5:  # 平日のみ
                scan_dates.append(d.strftime("%Y%m%d"))

        total = len(scan_dates)
        for idx, date in enumerate(scan_dates):
            if idx % 10 == 0:
                logger.info(f"  財務諸表スキャン: {idx}/{total} 日付処理済み")
            try:
                day_data = self.get_statements_for_date(date)
                all_statements.extend(day_data)
                time.sleep(0.3)
            except Exception as e:
                logger.debug(f"  {date}: {e}")

        if not all_statements:
            raise RuntimeError("財務諸表データを取得できませんでした")

        df = pd.DataFrame(all_statements)
        df = self._keep_latest_statements(df)
        logger.info(f"財務諸表取得完了: {len(df)} 銘柄")
        return df

    def _keep_latest_statements(self, df: pd.DataFrame) -> pd.DataFrame:
        """各銘柄につき最新の年次決算を優先して1行に絞る"""
        if df.empty:
            return df

        # TypeOfCurrentPeriod で優先度付け
        df["_period_priority"] = df.get("TypeOfCurrentPeriod", pd.Series()).map(
            lambda x: _PERIOD_PRIORITY.get(str(x), 99)
        )
        df["DisclosedDate"] = pd.to_datetime(df.get("DisclosedDate", pd.NaT), errors="coerce")

        df = df.sort_values(
            ["_period_priority", "DisclosedDate"],
            ascending=[True, False],
        )
        code_col = "Code" if "Code" in df.columns else "LocalCode"
        df = df.drop_duplicates(subset=[code_col], keep="first")
        df = df.drop(columns=["_period_priority"], errors="ignore")
        return df


# ------------------------------------------------------------------
# スクリーニング関数
# ------------------------------------------------------------------

class JQuantsScreener:
    """増配バリュー株スクリーナー"""

    def __init__(
        self,
        client: JQuantsClient,
        pbr_max: float = 1.5,
        yield_min: float = 2.5,
        market_cap_min: float = 10_000_000_000,
        div_cut_years: int = 3,
    ):
        self.client = client
        self.pbr_max = pbr_max
        self.yield_min = yield_min
        self.market_cap_min = market_cap_min
        self.div_cut_years = div_cut_years

    def run(self, holdings_codes: list[str] | None = None) -> tuple[pd.DataFrame, pd.DataFrame]:
        """
        スクリーニングを実行する。

        Parameters
        ----------
        holdings_codes : list[str] | None
            保有銘柄コードリスト（シート1用の詳細データ取得に使用）

        Returns
        -------
        (holdings_df, candidates_df)
        """
        print("【1/4】上場銘柄一覧を取得中...")
        listed = self.client.get_listed_info()

        print("【2/4】最新株価を取得中...")
        latest_date = self.client.get_latest_trading_date()
        print(f"  取引日: {latest_date}")
        prices = self.client.get_daily_quotes(latest_date)

        print("【3/4】財務諸表データを収集中 (数分かかります)...")
        statements = self.client.collect_recent_statements(days_back=120)

        print("【4/4】スクリーニング処理中...")
        merged = self._merge_all(listed, prices, statements)
        candidates = self._apply_filters(merged)

        # 減配チェック（フィルタ通過銘柄のみ）
        candidates = self._check_no_dividend_cut(candidates)

        # 保有銘柄の詳細データ
        holdings_df = pd.DataFrame()
        if holdings_codes:
            holdings_df = merged[merged["code"].isin(holdings_codes)].copy()

        print(f"スクリーニング結果: {len(candidates)} 銘柄が条件を満たしました")
        return holdings_df, candidates

    # ------------------------------------------------------------------
    # 内部: データ結合
    # ------------------------------------------------------------------

    def _merge_all(
        self,
        listed: pd.DataFrame,
        prices: pd.DataFrame,
        statements: pd.DataFrame,
    ) -> pd.DataFrame:
        """上場情報・株価・財務を結合してスクリーニング用DataFrameを作成"""

        # --- 列名の正規化 ---
        listed = listed.rename(columns={
            "Code": "code",
            "CompanyName": "name",
            "Sector17CodeName": "sector17",
            "Sector33CodeName": "sector33",
            "MarketCodeName": "market",
        })
        if "code" not in listed.columns and "LocalCode" in listed.columns:
            listed = listed.rename(columns={"LocalCode": "code"})

        prices = prices.rename(columns={
            "Code": "code",
            "Close": "close",
            "AdjustmentClose": "adj_close",
            "Volume": "volume",
            "TurnoverValue": "turnover",
        })

        stmt_code_col = "Code" if "Code" in statements.columns else "LocalCode"
        statements = statements.rename(columns={
            stmt_code_col: "code",
            "BookValuePerShare": "bps",
            "ResultDividendPerShareAnnual": "dps_result",
            "ForecastDividendPerShareAnnual": "dps_forecast",
            "NumberOfIssuedAndOutstandingSharesAtTheEndOfFiscalYearIncludingTreasuryStock": "shares_outstanding",
            "TypeOfCurrentPeriod": "period_type",
            "DisclosedDate": "disclosed_date",
            "CurrentFiscalYearEndDate": "fy_end_date",
        })

        # 株価はAdjustmentCloseを優先、なければClose
        if "adj_close" in prices.columns:
            prices["price"] = prices["adj_close"].combine_first(prices.get("close"))
        else:
            prices["price"] = prices.get("close")

        # コード列の型統一
        # fins/statements の LocalCode は5桁（例: "14140"）
        # listed/info・prices は4桁（例: "1414"）なので末尾1桁を除いて統一
        for df in [listed, prices]:
            if "code" in df.columns:
                df["code"] = df["code"].astype(str).str.strip().str[:4]
        if "code" in statements.columns:
            statements["code"] = (
                statements["code"].astype(str).str.strip()
                .apply(lambda x: x[:4] if len(x) == 5 else x)
            )

        # マージ
        df = listed[["code", "name", "sector17", "sector33", "market"]].merge(
            prices[["code", "price", "volume"]],
            on="code", how="left"
        ).merge(
            statements[["code", "bps", "dps_result", "dps_forecast",
                         "shares_outstanding", "period_type", "disclosed_date"]],
            on="code", how="left"
        )

        # 数値変換
        for col in ["price", "bps", "dps_result", "dps_forecast", "shares_outstanding"]:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # DPS: 結果値を優先、なければ予測値
        df["dps"] = df["dps_result"].combine_first(df["dps_forecast"])

        # PBR, 配当利回り, 時価総額を計算
        df["pbr"] = df["price"] / df["bps"]
        df["div_yield"] = (df["dps"] / df["price"] * 100).where(df["price"] > 0)
        df["market_cap"] = df["shares_outstanding"] * df["price"]

        return df

    # ------------------------------------------------------------------
    # 内部: フィルタリング
    # ------------------------------------------------------------------

    def _apply_filters(self, df: pd.DataFrame) -> pd.DataFrame:
        """PBR・配当利回り・時価総額フィルタを適用"""
        before = len(df)

        df = df.dropna(subset=["price", "pbr", "div_yield", "market_cap"])
        df = df[df["pbr"] <= self.pbr_max]
        df = df[df["div_yield"] >= self.yield_min]
        df = df[df["market_cap"] >= self.market_cap_min]

        # ETFや外国株など除外（コードが4桁数字のもののみ）
        df = df[df["code"].str.match(r"^\d{4}$")]

        after = len(df)
        logger.info(f"基本フィルタ: {before} → {after} 銘柄")
        return df

    # ------------------------------------------------------------------
    # 内部: 減配チェック
    # ------------------------------------------------------------------

    def _check_no_dividend_cut(self, df: pd.DataFrame) -> pd.DataFrame:
        """直近 div_cut_years 年間で減配がないかチェック"""
        if df.empty:
            return df

        print(f"  減配チェック中 ({len(df)} 銘柄)... ", end="", flush=True)
        results = []

        for _, row in df.iterrows():
            code = row["code"]
            try:
                no_cut = self._has_no_dividend_cut(code)
                if no_cut:
                    results.append(True)
                else:
                    results.append(False)
                time.sleep(0.3)
            except Exception as e:
                logger.debug(f"  {code} 減配チェック失敗: {e}")
                results.append(True)  # エラー時はフィルタしない（保守的）

        df = df[results].copy()
        print(f"完了 → {len(df)} 銘柄が減配なし")
        return df

    def _has_no_dividend_cut(self, code: str) -> bool:
        """
        指定銘柄が直近 div_cut_years 年間で減配していないか確認。
        年次DPS（ResultDividendPerShareAnnual）を年度ごとに比較。
        """
        stmt_df = self.client.get_statements_for_code(code)
        if stmt_df.empty:
            return True  # データなし → 判定不可なのでパス

        # 年次決算のみ抽出
        annual_mask = stmt_df.get("TypeOfCurrentPeriod", pd.Series()) == "FY"
        annual = stmt_df[annual_mask].copy()
        if annual.empty:
            return True

        dps_col = "ResultDividendPerShareAnnual"
        if dps_col not in annual.columns:
            return True

        annual = annual.rename(columns={"DisclosedDate": "date"})
        annual["date"] = pd.to_datetime(annual.get("date"), errors="coerce")
        annual[dps_col] = pd.to_numeric(annual[dps_col], errors="coerce")
        annual = annual.sort_values("date", ascending=False)

        # 直近 div_cut_years 年分の年次DPSを取得
        recent = annual.head(self.div_cut_years)
        dps_values = recent[dps_col].dropna().tolist()

        if len(dps_values) < 2:
            return True  # データ不足 → 判定不可なのでパス

        # 前年比較: 減配（直近DPS < 前年DPS）があればFalse
        for i in range(len(dps_values) - 1):
            if dps_values[i] < dps_values[i + 1]:
                return False  # 減配あり

        return True


# ------------------------------------------------------------------
# 保有銘柄の株式情報を充実させる
# ------------------------------------------------------------------

def enrich_holdings(
    holdings_pdf: pd.DataFrame,
    screener_merged: pd.DataFrame,
) -> pd.DataFrame:
    """
    PDFから取得した保有銘柄に、J-Quantsから取得した
    PBR・配当利回りなどの情報を付加する。
    """
    if screener_merged.empty:
        return holdings_pdf

    jq_cols = ["code", "pbr", "div_yield", "market_cap", "sector17", "market", "dps"]
    jq_data = screener_merged[
        [c for c in jq_cols if c in screener_merged.columns]
    ].drop_duplicates("code")

    merged = holdings_pdf.merge(jq_data, on="code", how="left")
    return merged
