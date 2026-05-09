@echo off
setlocal
chcp 65001 >nul

set "ROOT=%~dp0"
pushd "%ROOT%"

set "LOCAL_PYTHON=%ROOT%\.venv\Scripts\python.exe"
set "REQUIREMENTS_FILE=%ROOT%requirements.txt"
set "DEPENDENCY_CHECKER=%ROOT%tools\check_requirements.py"
set "BOOTSTRAP_PYTHON="
set "PYTHON_CMD="

if exist "%LOCAL_PYTHON%" (
    set "PYTHON_CMD=%LOCAL_PYTHON%"
)

if not defined BOOTSTRAP_PYTHON (
    where py >nul 2>nul
    if not errorlevel 1 set "BOOTSTRAP_PYTHON=py -3"
)
if not defined BOOTSTRAP_PYTHON (
    where python >nul 2>nul
    if not errorlevel 1 set "BOOTSTRAP_PYTHON=python"
)

if not defined PYTHON_CMD if not defined BOOTSTRAP_PYTHON (
    echo Python 3 was not found in PATH.
    echo Expected local interpreter: %LOCAL_PYTHON%
    echo Install Python 3 and run this launcher again.
    pause
    popd
    exit /b 1
)

if not exist "%LOCAL_PYTHON%" (
    echo Local virtual environment was not found. Creating .venv...
    %BOOTSTRAP_PYTHON% -m venv "%ROOT%\.venv"
    if errorlevel 1 (
        echo.
        echo Failed to create the local virtual environment.
        pause
        popd
        exit /b 1
    )
    set "PYTHON_CMD=%LOCAL_PYTHON%"
)

if not exist "%REQUIREMENTS_FILE%" (
    echo requirements.txt was not found: %REQUIREMENTS_FILE%
    pause
    popd
    exit /b 1
)

if not exist "%DEPENDENCY_CHECKER%" (
    echo Dependency checker was not found: %DEPENDENCY_CHECKER%
    pause
    popd
    exit /b 1
)

echo Checking Python dependencies...
"%PYTHON_CMD%" "%DEPENDENCY_CHECKER%" "%REQUIREMENTS_FILE%"
if errorlevel 1 (
    echo Missing or incompatible dependencies detected. Installing requirements...
    "%PYTHON_CMD%" -m pip install --disable-pip-version-check -r "%REQUIREMENTS_FILE%"
    if errorlevel 1 (
        echo.
        echo Failed to install RocketCat Shell dependencies.
        pause
        popd
        exit /b 1
    )

    echo Re-checking Python dependencies...
    "%PYTHON_CMD%" "%DEPENDENCY_CHECKER%" "%REQUIREMENTS_FILE%"
    if errorlevel 1 (
        echo.
        echo Dependencies are still missing or incompatible after installation.
        pause
        popd
        exit /b 1
    )
)

echo Starting RocketCat Shell...
"%PYTHON_CMD%" -m rocketcat_shell %*
set "EXIT_CODE=%ERRORLEVEL%"

if not "%EXIT_CODE%"=="0" (
    echo.
    echo RocketCat Shell exited with code %EXIT_CODE%.
    pause
)

popd
exit /b %EXIT_CODE%