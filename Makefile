# Define environment variable for the virtual environment directory
VENV_DIR = venv

# Define platform-specific pip and activation paths
PIP = $(VENV_DIR)/bin/pip
ACTIVATE = $(VENV_DIR)/bin/activate
PYTEST = $(VENV_DIR)/bin/pytest
ifeq ($(OS),Windows_NT)
    PIP = $(VENV_DIR)/Scripts/pip.exe
    ACTIVATE = $(VENV_DIR)/Scripts/activate.bat
	PYTEST = $(VENV_DIR)/Scripts/pytest.exe
endif

# Command to create a virtual environment
create_venv:
	python -m venv $(VENV_DIR)

# Command to install dependencies from requirements.txt
install_requirements:
	$(PIP) install -r requirements.txt

# Command to install the package using setup.py
install_package:
	$(PIP) install .

# Command to activate the virtual environment (for Unix-based systems)
activate_venv:
	@echo "To activate the virtual environment, run:"
	@echo "  source $(ACTIVATE)  # On Unix-based systems"
	@echo "  $(VENV_DIR)\Scripts\activate  # On Windows"
	$(VENV_DIR)\Scripts\activate

# Command to create the virtual environment, install requirements, install the package, and display activation message
venv: create_venv install_requirements install_package activate_venv
	@echo "Virtual environment and dependencies are set up!"


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