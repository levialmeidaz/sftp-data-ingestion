@echo on
setlocal
chcp 65001 >nul
set "PYTHON=C:\Users\atend\AppData\Local\Programs\Python\Python313\python.exe"
set "BASE=C:\Users\atend\OneDrive\Área de Trabalho\git_jb\sftp-data-ingestion\python"
for %%I in ("%BASE%") do set "BASE8=%%~sI"
set "LOGS=%BASE8%\logs"
if not exist "%LOGS%" mkdir "%LOGS%"

echo Python:
"%PYTHON%" -V

pushd "%BASE8%"
echo CWD:
cd

set PYTHONUTF8=1
set PYTHONIOENCODING=utf-8

echo ==== %date% %time% ==== >> "%LOGS%\pipeline.log"

echo [1] ingestion_sftp_pedidos.py
"%PYTHON%" "ingestion_sftp_pedidos.py"  >> "%LOGS%\01_ingestion.log" 2>&1
if errorlevel 1 goto fail

echo [2] load_stg_pedidos.py
"%PYTHON%" "load_stg_pedidos.py"        >> "%LOGS%\02_stg.log" 2>&1
if errorlevel 1 goto fail

echo [3] load_dw_pedidos.py
"%PYTHON%" "load_dw_pedidos.py"         >> "%LOGS%\03_dw.log" 2>&1
if errorlevel 1 goto fail

echo OK >> "%LOGS%\pipeline.log"
popd
exit /b 0

:fail
echo FAIL code %errorlevel% em %date% %time% >> "%LOGS%\pipeline.log"
type "%LOGS%\01_ingestion.log"
type "%LOGS%\02_stg.log"
type "%LOGS%\03_dw.log"
popd
exit /b %errorlevel%
