@echo off
echo Setting up git repository...
cd /d "c:\WalmartChjile\source\ContractsWalmartPOC"

echo Current directory:
cd

echo Checking git status...
git status

echo Adding remote origin...
git remote -v

echo Pushing to GitHub...
git push -u origin main

echo.
echo Repository setup complete!
echo Visit: https://github.com/jorgelunams/contratospoc.git

pause
