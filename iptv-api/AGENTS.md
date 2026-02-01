# AGENTS.md - IPTV API Development Guide

This file provides guidelines for AI coding agents working on this Python FastAPI IPTV API project.

## Build & Run Commands

```bash
# Install dependencies
pip install -r requirements.txt

# Run the API locally
python -m uvicorn scripts.api:app --reload --host 0.0.0.0 --port 3000

# Run with Docker
cd docker && docker-compose up -d

# Check health
curl http://localhost/health
```

## Lint & Format (Recommended)

```bash
# Setup (one time)
pip install ruff mypy black isort

# Format code
black scripts/ services/ utils/
isort scripts/ services/ utils/

# Lint
ruff check scripts/ services/ utils/
mypy scripts/ services/ utils/

# Run single test (when tests exist)
pytest tests/test_specific.py::test_function -v
```

## Code Style Guidelines

### Naming Conventions
- **Classes**: `PascalCase` (e.g., `UserService`, `PlaylistService`)
- **Functions/Variables**: `snake_case` (e.g., `create_user`, `max_connections`)
- **Constants**: `UPPER_CASE` (e.g., `JWT_SECRET`, `DEFAULT_TIMEOUT`)
- **Private methods**: `_leading_underscore` (e.g., `_hash_password`)
- **Modules**: `snake_case` with underscores (e.g., `user_service.py`)

### Import Style
```python
# Standard library first
import os
import re
from datetime import datetime
from typing import Optional, Dict, Any

# Third-party packages
from fastapi import FastAPI, HTTPException
from supabase import Client
import bcrypt

# Local imports - absolute from project root
from utils.config import get_settings
from utils.models import UserCreate, UserResponse
from utils.constants import JWT_SECRET

# Within-package relative imports (for services/)
from .user_service import UserService
```

### Type Hints
- Always use type hints for function parameters and return types
- Use `Optional[Type]` for nullable values
- Use `Dict[str, Any]` and `List[Type]` for collections
- Import types from `typing` module

### Docstrings
- Use triple quotes `"""` for module, class, and function docstrings
- Keep docstrings in Spanish (existing convention)
- First line: brief description
- Use Args/Returns sections for complex functions

### Error Handling
```python
# Use specific exceptions
raise ValueError(f"El usuario '{username}' ya existe")

# FastAPI HTTP exceptions for API errors
raise HTTPException(status_code=404, detail="Usuario no encontrado")

# Try-except with specific handling
try:
    result = operation()
except Exception as e:
    # Log or handle appropriately
    return None
```

### FastAPI Patterns
```python
# Dependency injection for services
def get_user_service() -> UserService:
    return UserService(supabase_client)

# Route with dependencies
@app.get("/api/users/{user_id}")
async def get_user(
    user_id: str,
    svc: UserService = Depends(get_user_service)
):
    return svc.get_user(user_id)

# Pydantic models for request/response
class UserCreate(BaseModel):
    username: str = Field(..., min_length=3, max_length=50)
    max_connections: int = Field(default=2, ge=1, le=10)
```

## Project Structure

```
scripts/        # Main applications (api.py, sync_iptv.py)
services/       # Business logic (UserService, DeviceService, etc.)
utils/          # Shared utilities (config, models, constants)
database/       # SQL schema
nginx/          # Reverse proxy config
docker/         # Docker configuration
postman/        # API collections
```

## Database & Supabase
- Use Supabase client for all database operations
- Use `utils.constants` for table names
- Handle timezone-aware datetimes carefully
- Use `.execute()` pattern for queries

## Key Patterns to Follow
1. Services are stateless classes receiving Supabase client in `__init__`
2. Configuration goes in `utils.config.Settings`
3. Constants go in `utils.constants`
4. Pydantic models go in `utils.models`
5. Use dependency injection in FastAPI routes
6. Always validate user permissions in admin endpoints
7. Handle JWT authentication using `get_current_user` dependency

## Security Reminders
- Never commit `.env` files
- Use `getattr()` with defaults for optional model fields
- Validate all user inputs with Pydantic
- Check user roles before admin operations
- Hash passwords with bcrypt (never store plain text)
