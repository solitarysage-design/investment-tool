@echo off
chcp 65001 > nul
echo ============================================================
echo  増配バリュー株 週次投資判断ツール - セットアップ
echo ============================================================
echo.

:: Python確認
python --version > nul 2>&1
if errorlevel 1 (
    echo [エラー] Python が見つかりません。
    echo Python 3.10以上をインストールしてください: https://www.python.org/
    pause
    exit /b 1
)

echo [1/3] Python 確認 OK
python --version

:: 仮想環境の作成
if not exist ".venv" (
    echo.
    echo [2/3] 仮想環境を作成中...
    python -m venv .venv
) else (
    echo [2/3] 仮想環境は既に存在します
)

:: ライブラリのインストール
echo.
echo [3/3] 必要なライブラリをインストール中...
call .venv\Scripts\activate.bat
pip install --upgrade pip -q
pip install -r requirements.txt

echo.
echo ============================================================
echo  セットアップ完了！
echo.
echo  次のステップ:
echo  1. .env ファイルをメモ帳等で開いて認証情報を入力
echo     - JQUANTS_EMAIL: J-Quantsに登録したメールアドレス
echo     - JQUANTS_PASSWORD: J-Quantsのパスワード
echo     - PDF_PATH: 楽天証券の保有商品一覧PDFのパス
echo.
echo  2. ツールを実行:  run.bat
echo ============================================================
pause
