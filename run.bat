@echo off
chcp 65001 > nul
echo ============================================================
echo  増配バリュー株 週次投資判断ツール - 実行
echo ============================================================
echo.

if not exist ".venv\Scripts\activate.bat" (
    echo [エラー] 仮想環境が見つかりません。先に setup.bat を実行してください。
    pause
    exit /b 1
)

call .venv\Scripts\activate.bat
python main.py

echo.
pause
