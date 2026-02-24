"""
増配バリュー株 週次投資判断ツール - Streamlit Web アプリ

スマホのブラウザから:
  1. 楽天証券の「保有商品一覧」PDFをアップロード
  2. 「分析開始」ボタンを押す
  3. 生成されたExcelをダウンロード
"""

import io
import tempfile
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st

# ページ設定（必ず最初に呼ぶ）
st.set_page_config(
    page_title="増配バリュー株ツール",
    page_icon="📈",
    layout="centered",
    initial_sidebar_state="expanded",
)

from pdf_parser import parse_rakuten_pdf
from jquants_api import JQuantsClient, JQuantsScreener, enrich_holdings
from excel_generator import create_investment_excel


# ---------------------------------------------------------------------------
# セッション状態の初期化
# ---------------------------------------------------------------------------
for key, default in [
    ("result_excel", None),
    ("result_summary", None),
]:
    if key not in st.session_state:
        st.session_state[key] = default


# ---------------------------------------------------------------------------
# 認証情報の取得（Streamlit Secrets → .env → サイドバー入力 の優先順）
# ---------------------------------------------------------------------------
def _load_credentials() -> tuple[str, str]:
    # Streamlit Community Cloud の secrets
    try:
        return st.secrets["JQUANTS_EMAIL"], st.secrets["JQUANTS_PASSWORD"]
    except Exception:
        pass
    # ローカル .env
    try:
        import config
        if config.JQUANTS_EMAIL and config.JQUANTS_PASSWORD:
            return config.JQUANTS_EMAIL, config.JQUANTS_PASSWORD
    except Exception:
        pass
    return "", ""


preset_email, preset_password = _load_credentials()


# ---------------------------------------------------------------------------
# サイドバー
# ---------------------------------------------------------------------------
with st.sidebar:
    st.header("⚙️ 設定")

    # J-Quants 認証情報
    st.subheader("🔑 J-Quants 認証情報")
    if preset_email:
        st.success("認証情報は設定済みです")
        email = preset_email
        password = preset_password
    else:
        st.caption("未設定の場合はこちらに入力")
        email    = st.text_input("メールアドレス", placeholder="example@email.com")
        password = st.text_input("パスワード", type="password")
        st.caption("📖 [J-Quants 無料登録はこちら](https://application.jpx-jquants.com/)")

    st.divider()

    # スクリーニング条件
    st.subheader("📊 スクリーニング条件")
    pbr_max           = st.slider("PBR 上限（倍）",         0.5,  3.0, 1.5, 0.1)
    yield_min         = st.slider("配当利回り 下限（%）",   1.0,  6.0, 2.5, 0.1)
    mktcap_min_oku    = st.slider("時価総額 下限（億円）",  50,   500, 100,  50)
    div_cut_years     = st.selectbox("減配チェック年数",    [2, 3, 5], index=1)

    st.divider()
    st.caption("v1.0 · 毎週PDFを差し替えるだけで再実行可能")


# ---------------------------------------------------------------------------
# メインエリア
# ---------------------------------------------------------------------------
st.title("📈 増配バリュー株\n週次投資判断ツール")
st.caption("楽天証券の保有商品一覧PDFをアップロードして、週次の投資判断Excelを自動生成します")

st.divider()

# --- PDF アップロード ---
uploaded_file = st.file_uploader(
    "📄 楽天証券「保有商品一覧」PDF",
    type=["pdf"],
    help="楽天証券アプリ → 保有商品一覧 → PDF出力 でダウンロードしたファイルを選択",
)

if uploaded_file:
    st.success(f"✅ {uploaded_file.name}")

    # PDF デバッグパネル（解析失敗時に確認用）
    with st.expander("🔍 PDF 生テキスト確認（解析がうまくいかない場合に展開）"):
        try:
            import pdfplumber, io as _io, tempfile
            with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as _tmp:
                _tmp.write(uploaded_file.getvalue())
                _tmp_path = _tmp.name
            with pdfplumber.open(_tmp_path) as _pdf:
                for _pn, _pg in enumerate(_pdf.pages[:3]):
                    st.caption(f"--- Page {_pn+1} ---")
                    _txt = _pg.extract_text() or "（テキスト抽出なし）"
                    st.text(_txt[:1500])
            Path(_tmp_path).unlink(missing_ok=True)
        except Exception as _e:
            st.warning(f"デバッグ表示エラー: {_e}")
else:
    st.info("PDFなしでも実行できます（スクリーニングのみ）")

# --- スクリーニング条件の確認 ---
with st.expander("スクリーニング条件を確認"):
    c1, c2 = st.columns(2)
    c1.metric("PBR 上限",      f"{pbr_max} 倍")
    c1.metric("時価総額 下限", f"{mktcap_min_oku} 億円")
    c2.metric("配当利回り 下限", f"{yield_min} %")
    c2.metric("減配チェック",   f"{div_cut_years} 年間")

st.divider()

# --- 実行ボタン ---
can_run = bool(email and password)
run_btn = st.button(
    "🚀 分析開始",
    type="primary",
    disabled=not can_run,
    use_container_width=True,
)
if not can_run:
    st.caption("サイドバーで J-Quants 認証情報を入力してください")


# ---------------------------------------------------------------------------
# 分析処理
# ---------------------------------------------------------------------------
if run_btn:
    st.session_state.result_excel   = None
    st.session_state.result_summary = None
    holdings_df = pd.DataFrame()
    candidates_df = pd.DataFrame()

    with st.status("分析中... 数分かかります", expanded=True) as status:

        # STEP 1: PDF 解析
        if uploaded_file:
            st.write("📄 PDF を解析中...")
            try:
                with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
                    tmp.write(uploaded_file.getvalue())
                    tmp_path = tmp.name
                holdings_df = parse_rakuten_pdf(tmp_path)
                Path(tmp_path).unlink(missing_ok=True)
                st.write(f"　→ {len(holdings_df)} 銘柄の保有情報を取得")
            except Exception as e:
                st.warning(f"　PDF解析に失敗: {e}\n　スクリーニングのみ実行します")
        else:
            st.write("📄 PDF なし → スクリーニングのみ実行します")

        # STEP 2: J-Quants スクリーニング
        st.write("🔍 J-Quants API でスクリーニング中...")
        try:
            client = JQuantsClient(email, password)
            screener = JQuantsScreener(
                client=client,
                pbr_max=pbr_max,
                yield_min=yield_min,
                market_cap_min=mktcap_min_oku * 1e8,
                div_cut_years=div_cut_years,
            )
            holdings_codes = holdings_df["code"].tolist() if not holdings_df.empty else []
            jq_holdings_raw, candidates_df = screener.run(holdings_codes=holdings_codes)

            if not holdings_df.empty and not jq_holdings_raw.empty:
                holdings_df = enrich_holdings(holdings_df, jq_holdings_raw)

            st.write(f"　→ {len(candidates_df)} 銘柄がスクリーニングを通過")

        except Exception as e:
            status.update(label="❌ J-Quants API エラー", state="error")
            err_str = str(e)
            st.error(f"**J-Quants API エラー**\n\n{err_str}")
            if "取引日" in err_str:
                st.info(
                    "💡 **取引日取得エラーの対処法**\n"
                    "- J-Quants の Light プランに登録済みか確認してください\n"
                    "- `prices/daily_quotes` エンドポイントへのアクセス権限があるか確認してください\n"
                    "- J-Quants のステータスページでサービス障害がないか確認してください\n"
                    "- エラー詳細（HTTP ステータスコード）はログを確認してください"
                )
            elif "401" in err_str or "認証" in err_str:
                st.info("💡 メールアドレス・パスワードが正しいか確認してください")
            st.stop()

        # STEP 3: Excel 生成
        st.write("📊 Excel を生成中...")
        try:
            buf = io.BytesIO()
            create_investment_excel(holdings_df, candidates_df, buf)
            buf.seek(0)

            # トップ5候補
            top5 = pd.DataFrame()
            if not candidates_df.empty and "div_yield" in candidates_df.columns:
                top5 = candidates_df.nlargest(5, "div_yield")

            st.session_state.result_excel = buf.getvalue()
            st.session_state.result_summary = {
                "holdings_count":   len(holdings_df),
                "candidates_count": len(candidates_df),
                "top5":             top5,
                "run_time":         datetime.now().strftime("%Y/%m/%d %H:%M"),
            }
            status.update(label="✅ 分析完了！", state="complete")
            st.write("　→ Excel生成完了")

        except Exception as e:
            status.update(label="❌ Excel生成エラー", state="error")
            st.error(f"Excel生成エラー: {e}")
            st.stop()


# ---------------------------------------------------------------------------
# 結果表示 & ダウンロード
# ---------------------------------------------------------------------------
if st.session_state.result_excel:
    s = st.session_state.result_summary

    st.success(f"✅ 分析完了（{s['run_time']}）")

    m1, m2 = st.columns(2)
    m1.metric("保有銘柄", f"{s['holdings_count']} 銘柄")
    m2.metric("新規候補", f"{s['candidates_count']} 銘柄")

    # ダウンロードボタン
    today_str = datetime.now().strftime("%Y%m%d")
    st.download_button(
        label="📥 投資判断シート (Excel) をダウンロード",
        data=st.session_state.result_excel,
        file_name=f"投資判断シート_{today_str}.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        use_container_width=True,
        type="primary",
    )

    # 候補銘柄プレビュー
    top5: pd.DataFrame = s.get("top5", pd.DataFrame())
    if not top5.empty:
        st.subheader("📊 候補銘柄トップ5（配当利回り順）")

        def fmt_yield(v):
            if v is None: return "-"
            return f"{v*100:.2f}%" if v <= 1 else f"{v:.2f}%"

        preview = pd.DataFrame({
            "コード":     top5.get("code", "-"),
            "銘柄名":     top5.get("name", "-").str[:15] if "name" in top5.columns else "-",
            "配当利回り": top5["div_yield"].apply(fmt_yield) if "div_yield" in top5.columns else "-",
            "PBR":        top5["pbr"].apply(lambda x: f"{x:.2f}倍" if pd.notna(x) else "-") if "pbr" in top5.columns else "-",
            "時価総額":   top5["market_cap"].apply(lambda x: f"{x/1e8:.0f}億" if pd.notna(x) else "-") if "market_cap" in top5.columns else "-",
        })
        st.dataframe(preview, use_container_width=True, hide_index=True)
