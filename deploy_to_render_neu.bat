@echo off
setlocal enabledelayedexpansion

:: ============================================================
::  RIO PRINT MEDIA — Deploy to Render (Neumorphism v01)
::  Deployment Script for Windows
:: ============================================================

echo.
echo =====================================================
echo   RIO PRINT MEDIA - Deploy to Render (NEU v01)
echo =====================================================
echo.

:: Change to your project directory
cd /d "%~dp0"
echo [INFO] Working directory: %CD%
echo.

:: Check if git is installed
git --version >nul 2>&1
if %errorlevel% neq 0 (
    echo [ERROR] Git is not installed. Please install Git.
    echo Download from: https://git-scm.com/download/win
    pause
    exit /b 1
)

:: Check required files exist
set "files=Rio_Sales_Tracker_ONLINE.html rio_api.py requirements.txt .env"
for %%f in (%files%) do (
    if not exist "%%f" (
        echo [WARNING] Missing file: %%f
    )
)

echo.
echo ---- Step 1: Initialize/Update Git Repo ----
if not exist ".git" (
    echo Initializing git repository...
    git init
    git branch -M main
    git remote add origin https://github.com/rioprintmediaa/Rio-Print-Media-Test.git
    echo [OK] Git repo initialized
) else (
    echo Git repo already exists
    git remote set-url origin https://github.com/rioprintmediaa/Rio-Print-Media-Test.git
)

echo.
echo ---- Step 2: Add Files ----
echo Adding files to git...
git add Rio_Sales_Tracker_ONLINE.html
git add rio_api.py
git add requirements.txt
git add .env
git add .gitignore
git add Procfile
git add -A

echo.
echo ---- Step 3: Commit Changes ----
set "TIMESTAMP=%date% %time%"
git commit -m "Deploy NEU v01 - %TIMESTAMP%" || echo [INFO] No changes to commit

echo.
echo ---- Step 4: Push to GitHub ----
echo Pushing to GitHub main branch...
git push -u origin main --force

if %errorlevel% equ 0 (
    echo.
    echo =====================================================
    echo   SUCCESS! Pushed to GitHub
    echo.
    echo   Next Steps:
    echo   1. Go to https://render.com
    echo   2. Your repo should auto-deploy
    echo   3. Wait 2-3 minutes for build
    echo   4. Check your Render dashboard
    echo.
    echo   Render URL: https://rio-print-media-test.onrender.com
    echo =====================================================
) else (
    echo.
    echo [ERROR] Push failed!
    echo Check your GitHub credentials or internet connection
)

echo.
echo Press any key to exit...
pause
endlocal
