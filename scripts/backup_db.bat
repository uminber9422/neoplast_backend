@echo off
REM =============================================================================
REM Neoplast Lead Dashboard — daily DB backup
REM Schedule via Windows Task Scheduler (e.g. daily at 02:00).
REM =============================================================================

setlocal
cd /d "%~dp0\.."

set DB=data\neoplast.db
if not exist "%DB%" (
  echo X DB file not found at %DB%
  exit /b 1
)

if not exist data\backups mkdir data\backups

REM Use yyyymmdd_HHMMSS for sortable filenames; PowerShell formats it portably.
for /f %%i in ('powershell -NoProfile -Command "Get-Date -Format yyyyMMdd_HHmmss"') do set TS=%%i

set DEST=data\backups\neoplast_%TS%.db

REM SQLite-safe copy: use the .backup pragma via sqlite3 if available, else file copy.
where sqlite3 >nul 2>nul
if %errorlevel%==0 (
  sqlite3 "%DB%" ".backup '%DEST%'" || exit /b 1
) else (
  copy /Y "%DB%" "%DEST%" >nul
)

echo OK: backed up to %DEST%

REM Retention: keep last 14 days
forfiles /P data\backups /M neoplast_*.db /D -14 /C "cmd /c del @path" 2>nul

endlocal
