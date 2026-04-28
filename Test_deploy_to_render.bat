@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  RIO PRINT MEDIA — Deploy to Render (TEST)
::  Source files : D:\Rio\Softwares\Merger\Test\Mongo\files\Rio-Print-Media-Test
::  GitHub repo  : https://github.com/rioprintmediaa/Rio-Print-Media-Test.git
:: ============================================================

set FILES_DIR=D:\Rio\Softwares\Merger\Test\Mongo\files\Rio-Print-Media-Test
set GITHUB_REPO=https://github.com/rioprintmediaa/Rio-Print-Media-Test.git

echo.
echo =====================================================
echo   RIO PRINT MEDIA — Deploy to Render (TEST)
echo =====================================================
echo.

:: ── VERIFY SOURCE FOLDER EXISTS ────────────────────────
if not exist "%FILES_DIR%" (
    echo [ERROR] Source folder not found: %FILES_DIR%
    pause ^& exit /b 1
)

:: ── CHECK GIT INSTALLED ────────────────────────────────
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Git is not installed or not in PATH
    pause ^& exit /b 1
)

:: ── GO TO SOURCE FOLDER ────────────────────────────────
cd /d "%FILES_DIR%"
echo [INFO] Working directory: %CD%
echo.

:: ── INIT REPO IF NOT ALREADY DONE ──────────────────────
if not exist ".git" (
    echo Initializing new git repo...
    git init
    git branch -M main
    git remote add origin %GITHUB_REPO%
    echo [OK] Git repo initialized.
) else (
    git remote remove origin >nul 2>&1
    git remote add origin %GITHUB_REPO%
)

:: ── ONE-TIME CLEANUP ───────────────────────────────────
:: Checks if the nested subfolder is tracked in git index.
:: If yes — removes it once. After that, this check always returns "not found"
:: so the cleanup block is SKIPPED on every subsequent run.
git ls-files --error-unmatch "Rio-Print-Media-Test" >nul 2>&1
if %errorlevel% equ 0 (
    echo [ONE-TIME FIX] Removing nested subfolder from git index...
    git rm -r --cached "Rio-Print-Media-Test" >nul 2>&1
    echo [OK] Done. This will not run again.
)

:: ── ADD FILES ──────────────────────────────────────────
echo Adding files...
git add -f Rio_Sales_Tracker_ONLINE.html
git add -f rio_api.py
git add -f requirements.txt
git add -A

:: ── COMMIT ─────────────────────────────────────────────
git diff --cached --quiet
if %errorlevel% equ 0 (
    echo [INFO] No changes. Forcing commit...
    git commit --allow-empty -m "Force deploy %date% %time%"
) else (
    git commit -m "Auto deploy %date% %time%"
)

:: ── PULL THEN PUSH ─────────────────────────────────────
git pull origin main --allow-unrelated-histories --rebase >nul 2>&1
echo Pushing to GitHub...
git push origin main --force

if %errorlevel% equ 0 (
    echo.
    echo =====================================================
    echo   SUCCESS! Pushed to GitHub
    echo   Render will auto-deploy in 2-3 minutes
    echo   Check: https://rio-print-media-test.onrender.com
    echo =====================================================
) else (
    echo.
    echo [ERROR] Push failed. Check GitHub credentials.
)

echo.
pause
endlocal
