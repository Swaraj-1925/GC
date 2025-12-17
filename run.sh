#!/bin/bash

# GemScap Quant Analytics - Setup & Run Script

echo "=================================================="
echo "   GemScap Quant Analytics - Setup & Run"
echo "=================================================="

# Function to check command existence
check_command() {
    if ! command -v "$1" &> /dev/null; then
        echo "Error: $1 is not installed."
        echo "Please install $1 before running this script."
        if [[ "$OSTYPE" == "linux-gnu"* ]]; then
            echo "On Linux, try: sudo apt install $1 (or equivalent)"
        elif [[ "$OSTYPE" == "msys" || "$OSTYPE" == "cygwin" || "$OSTYPE" == "win32" ]]; then
            echo "On Windows, please download the installer from the official website or use a package manager like choco/winget."
        fi
        return 1
    fi
    return 0
}

# 1. Environment Checks
echo "[1/5] Checking environment..."
MISSING_DEPS=0
if ! check_command "python3"; then MISSING_DEPS=1; fi
if ! check_command "npm"; then MISSING_DEPS=1; fi

if [ $MISSING_DEPS -eq 1 ]; then
    echo "Missing dependencies. Exiting."
    exit 1
fi

echo "Environment OK."

# 2. Python Virtual Environment
echo "[2/5] Setting up Python environment..."
if [ ! -d "venv" ]; then
    echo "Creating virtual environment..."
    python3 -m venv venv
fi

source venv/bin/activate
echo "Virtual environment activated."

# 3. Python Dependencies
echo "[3/5] Installing backend dependencies..."
if [ -f "requirements.txt" ]; then
    pip install -r requirements.txt
else
    echo "Warning: requirements.txt not found. Skipping dependency install."
fi

# 4. Frontend Dependencies
echo "[4/5] Installing frontend dependencies..."
cd frontend
if [ ! -d "node_modules" ]; then
    npm install
else
    echo "node_modules found in frontend. Skipping install."
fi
cd ..

# 5. Run Application
echo "[5/5] Starting application..."
echo ""
echo "==========================================================="
echo "   🚀 GemScap Analytics IS READY!"
echo "==========================================================="
echo "   👉 Dashboard: http://localhost:5173"
echo "   👉 API Docs:  http://localhost:8000/docs"
echo "==========================================================="
echo ""

# Trap SIGINT to kill background processes
trap 'kill $(jobs -p); exit' SIGINT

# Start Backend in background
source venv/bin/activate
python main.py &
BACKEND_PID=$!

# Start Frontend
cd frontend
npm run dev

# Wait for backend to finish (if frontend is stopped)
wait $BACKEND_PID
