@echo off
REM =============================================================================
REM Neoplast Lead Dashboard — one-shot setup (Windows)
REM Creates venv, installs Python deps, runs migrations, builds frontend.
REM =============================================================================

setlocal enabledelayedexpansion
cd /d "%~dp0\.."

echo.
echo === [1/5] Checking prerequisites...
where python >nul 2>nul || (echo   X Python not found in PATH. Install Python 3.11+ ^& try again. & exit /b 1)
where node   >nul 2>nul || (echo   X Node.js not found in PATH. Install Node 18+ ^& try again. & exit /b 1)
where npm    >nul 2>nul || (echo   X npm not found in PATH. & exit /b 1)
echo   OK

echo.
echo === [2/5] Setting up Python virtual environment...
if not exist .venv (
  python -m venv .venv || exit /b 1
)
call .venv\Scripts\activate.bat
python -m pip install --upgrade pip
pip install -r requirements.txt -r requirements-dev.txt || exit /b 1

echo.
echo === [3/5] Checking .env file...
if not exist .env (
  copy .env.example .env >nul
  echo   ! Created .env from template. EDIT IT BEFORE STARTING THE APP.
  echo   ! Required: SECRET_KEY ^(generate one^), API keys.
)

echo.
echo === [4/5] Running database migrations...
alembic upgrade head || exit /b 1

echo.
echo === [5/5] Installing + building frontend...
pushd frontend
call npm install || (popd & exit /b 1)
call npm run build || (popd & exit /b 1)
popd

echo.
echo ============================================================
echo  Setup complete.
echo  Next:
echo    1) Edit .env (especially SECRET_KEY)
echo    2) Run: python scripts\create_admin.py
echo    3) Run: scripts\start.bat
echo ============================================================
endlocal
