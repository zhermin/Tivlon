@echo off
pyinstaller --onefile --clean --distpath=../ --specpath=build autowechat.py
pause