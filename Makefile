# Define environment variable for the virtual environment directory
VENV_DIR = venv

# Define platform-specific pip and activation paths
PIP = $(VENV_DIR)/bin/pip
ACTIVATE = $(VENV_DIR)/bin/activate
PYTEST = $(VENV_DIR)/bin/pytest
ifeq ($(OS), Windows_NT)
    PIP = $(VENV_DIR)\Scripts\pip.exe
    ACTIVATE = $(VENV_DIR)\Scripts\activate.bat
    PYTEST = $(VENV_DIR)\Scripts\pytest.exe
    RM = rmdir /s /q  # Command for Windows to remove directories
    DEL = del /f /q   # Command for Windows to delete files
else
    RM = rm -rf  # Command for Unix-based systems
    DEL = rm -f  # Command for Unix-based systems
endif

# Command to create a virtual environment
create_venv:
	@echo "Creating a new virtual environment in $(VENV_DIR)..."
	python -m venv $(VENV_DIR)

# Command to install dependencies from requirements.txt
install_requirements:
	@echo "Installing dependencies..."
	$(PIP) install -r requirements.txt

# Command to install the package using setup.py
install_package:
	@echo "Installing the package..."
	$(PIP) install .

# Command to activate the virtual environment (informational message only)
activate_venv:
	@echo "To activate the virtual environment, run:"
	@echo "  source $(ACTIVATE)  # On Unix-based systems"
	@echo "  $(VENV_DIR)\Scripts\activate  # On Windows"
	$(VENV_DIR)\Scripts\activate

# Command to clean existing venv and create a new one, then install dependencies
venv: clean-venv create_venv install_requirements install_package
	@echo "Virtual environment and dependencies are set up!"
	@$(MAKE) activate_venv


# Command to run all test cases under the test folder
test:
	pytest

# Command to run coverate for functions
coverage :
	coverage run -m pytest -m 'not compare'
	coverage report

# Command to run coverate report for functions
coverage-html :
	coverage html
	python -m http.server -d htmlcov --bind "0.0.0.0" 9999
	@echo "Inspect missing coverage by using http:\\localhost:9999"

# Command to run format code
format-python:
		isort ace/ --settings-file setup.cfg
		black ace/ --config black.toml
		isort tests/ --settings-file setup.cfg
		black tests/ --config black.toml

format-python-check:
		isort ace/ --diff --check --settings-file setup.cfg
		black ace/ --diff --check --config black.toml
		isort tests/ --diff --check --settings-file setup.cfg
		black tests/ --diff --check --config black.toml
