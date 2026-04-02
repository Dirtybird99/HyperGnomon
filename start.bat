@echo off
title HyperGnomon v0.7.0
echo.
echo   HyperGnomon - Arena-Accelerated DERO Blockchain Scanner
echo   ========================================================
echo.
echo   Discovering TELA apps...
echo.
hypergnomon.exe --fastsync --turbo --tela-only
pause
