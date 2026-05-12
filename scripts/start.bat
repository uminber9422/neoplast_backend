@echo off
REM =============================================================================
REM Neoplast Lead Dashboard — start in production mode (frontend served by FastAPI)
REM For dev with hot reload, run uvicorn + `npm run dev` manually in two shells.
REM =============================================================================

setlocal
cd /d "%~dp0\.."

if not exist .venv\Scripts\activate.bat (
  echo X Virtual environment not found. Run scripts\setup.bat first.
  exit /b 1
)
if not exist .env (
  echo X .env not found. Copy .env.example and edit it first.
  exit /b 1
)

call .venv\Scripts\activate.bat
python -m uvicorn backend.main:app --host 0.0.0.0 --port 8080
endlocal
