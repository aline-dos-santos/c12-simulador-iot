@echo off
setlocal
cd /d "%~dp0"

echo ============================================
echo Simulador IoT - Preparando ambiente
echo ============================================

where py >nul 2>nul
if %errorlevel%==0 (
    set "PYTHON_CMD=py"
) else (
    set "PYTHON_CMD=python"
)

if not exist ".venv" (
    echo Criando ambiente virtual...
    %PYTHON_CMD% -m venv .venv
    if errorlevel 1 goto :erro
)

call ".venv\Scripts\activate.bat"
if errorlevel 1 goto :erro

echo Instalando/atualizando dependencias...
python -m pip install -r requirements.txt
if errorlevel 1 goto :erro

echo Iniciando interface...
python iot_sim_gui.py
if errorlevel 1 goto :erro

goto :fim

:erro
echo.
echo Ocorreu um erro ao preparar ou executar o programa.
pause
exit /b 1

:fim
endlocal
