#!/bin/bash
echo "Creating virtual environment (venv)..."
python3 -m venv venv
if [ $? -ne 0 ]; then
    echo "Failed to create venv. Please install python3-venv: sudo apt install python3-venv"
    exit 1
fi

echo "Activating venv and upgrading pip..."
source venv/bin/activate
pip install --upgrade pip

echo "Installing requirements from requirements.txt..."
pip install -r requirements.txt

echo "Python dependency installation complete."
