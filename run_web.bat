@echo off
chcp 65001 > nul
echo ============================================================
echo  増配バリュー株ツール - Web アプリ起動
echo ============================================================
echo.
echo ブラウザで http://localhost:8501 を開いてください
echo スマホからは http://[このPCのIPアドレス]:8501 でアクセス可能
echo.
echo 終了するには Ctrl+C を押してください
echo.

call .venv\Scripts\activate.bat
streamlit run app.py --server.address=0.0.0.0 --server.port=8501

pause
