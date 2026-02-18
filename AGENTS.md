# AGENTS.md - Guidelines for AI Coding Agents

## Project Overview

WALACTV-SCRAPPER is a Python 3.12 web scraper that extracts sports event data from "Fútbol en la TV" and integrates with Supabase. It includes IPTV playlist synchronization and channel mapping functionality.

## Build & Run Commands

### Setup Environment
```bash
python3.12 -m venv .venv
source .venv/bin/activate
pip install -r docker/config/requirements.txt
```

### Run Main Scripts
```bash
# Run the main scraper
python3 scripts/main.py

# Run with parameters
python3 scripts/main.py --days 3 --debug

# Sync IPTV data
python3 scripts/sync_iptv.py

# Populate channel mappings
python3 scripts/poblar_mapeo_canales.py

# Update EPG
python3 scripts/actualiza_epg.py
```

### Testing (No tests currently exist)
```bash
# Run all tests (when added)
pytest

# Run single test file
pytest tests/test_scrapper.py -v

# Run specific test
pytest tests/test_scrapper.py::test_function_name -v

# Run with coverage
pytest --cov=scripts --cov-report=html
```

### Linting & Formatting
```bash
# Format code with black
black scripts/

# Check with flake8
flake8 scripts/ --max-line-length=100

# Type checking with mypy
mypy scripts/ --ignore-missing-imports

# Import sorting
isort scripts/
```

## Code Style Guidelines

### General
- **Python Version**: 3.12+
- **Indentation**: 4 spaces (not tabs)
- **Line Length**: 100 characters max
- **Encoding**: UTF-8
- **Language**: Spanish for business logic, English for technical terms

### Imports
```python
# Order: stdlib → third-party → local
import json
import os
from datetime import datetime
from typing import Dict, Optional, List, Any

import requests
from bs4 import BeautifulSoup
from supabase import create_client, Client

from database import Database, DataManagerSupabase
from utils.constants import CHANNELS_TABLE
```

### Naming Conventions
- **Classes**: `PascalCase` (e.g., `ScrapperFutbolenlatv`, `SupabaseDB`)
- **Functions**: `snake_case` (e.g., `limpia_html`, `buscar_channel_id`)
- **Variables**: `snake_case` (e.g., `channel_ids`, `mapeos`)
- **Constants**: `UPPER_SNAKE_CASE` (e.g., `COLUMNAS_EVENTO_NORMAL`)
- **Private**: prefix with `_` (e.g., `_load_environment`)
- **Spanish names** preferred for domain concepts (canales, partidos, mapeos)

### Type Hints
- Use type hints for function parameters and return values
- Use `Optional[T]` for nullable values
- Use `List[T]`, `Dict[K, V]` from typing module
```python
def buscar_channel_id(nombre: str) -> Optional[str]:
    """Busca channel por nombre."""
    pass

def process_eventos(eventos: List[Dict[str, Any]]) -> int:
    """Procesa lista de eventos."""
    pass
```

### Docstrings
- Use triple double quotes
- Spanish for description
- Include Args and Returns sections for complex functions
```python
def validate(self, verbose: bool = True) -> bool:
    """
    Valida la configuración y muestra errores.

    Args:
        verbose: Si True, imprime información de diagnóstico

    Returns:
        bool: True si la configuración es válida
    """
```

### Error Handling
- Use try/except for external calls (network, database)
- Print descriptive messages with emojis for user feedback
- Log warnings gracefully without crashing
```python
try:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
except Exception as e:
    print(f"❌ Error accediendo a {url}: {e}")
    return None
```

### Classes
- Use static methods where appropriate
- Implement singleton pattern with `_instance` class variable
- Document class purpose in docstring
```python
class SupabaseDB:
    """Cliente singleton de Supabase."""
    _instance: Optional[Client] = None
```

### File Structure
- Place scripts in `scripts/` directory
- Utilities in `scripts/utils/`
- Services in `scripts/services/`
- Keep resource files in `resources/`
- Use `if __name__ == "__main__":` guard for executable scripts

### Configuration
- Load from environment variables via `.env` files
- Use `python-dotenv` for local development
- Store constants in `utils/constants.py`
- Use `Settings` class pattern for centralized config

### Database
- Use Supabase as primary database
- Implement bulk insert operations for performance
- Use batch size of 5000 for large operations
- Always handle connection errors gracefully

### JSON Files
- Store mappings in `resources/` directory
- Use UPPERCASE keys for channel names
- Pretty print with 2-space indentation
- Use UTF-8 encoding

### Logging & Output
- Use emojis for visual feedback (✅ ❌ ⚠️ ℹ️)
- Print progress messages in Spanish
- Include stats summary at end of batch operations

### Deployment
- Use Ansible playbooks in `ansible/` directory
- Environment variable: `entorno=pro` for production
- Docker configuration in `docker/` directory
