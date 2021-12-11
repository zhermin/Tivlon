@echo off
pyinstaller --onefile --clean --icon=icon.ico --distpath=../ --specpath=build --add-data "C:/Users/ZM/AppData/Local/Programs/Python/Python37-32/Lib/site-packages/dask/dask.yaml;./dask" --add-data "C:/Users/ZM/AppData/Local/Programs/Python/Python37-32/Lib/site-packages/distributed/distributed.yaml;./distributed" cleandata.py
pause