"""
Modelos Pydantic para IPTV API
"""
from datetime import datetime
from typing import Optional, List
from pydantic import BaseModel, Field
from enum import Enum


# ============================================
# Enums
# ============================================

class DeviceType(str, Enum):
    MOBILE = "mobile"
    TV = "tv"
    DESKTOP = "desktop"
    IPTV_APP = "iptv_app"
    UNKNOWN = "unknown"


# ============================================
# User Models
# ============================================

class UserCreate(BaseModel):
    """Modelo para crear usuario"""
    username: str = Field(..., min_length=3, max_length=50)
    password: str = Field(..., min_length=6)
    max_connections: int = Field(default=2, ge=1, le=10)
    role: str = Field(default="user")  # <--- NUEVO
    expires_at: Optional[datetime] = None


class UserUpdate(BaseModel):
    """Modelo para actualizar usuario"""
    password: Optional[str] = Field(None, min_length=6)
    max_connections: Optional[int] = Field(None, ge=1, le=10)
    is_active: Optional[bool] = None
    expires_at: Optional[datetime] = None
    role: Optional[str] = None  # <--- NUEVO


class UserResponse(BaseModel):
    """Respuesta de usuario"""
    id: str
    username: str
    max_connections: int
    is_active: bool
    expires_at: Optional[datetime]
    created_at: datetime
    active_devices: int = 0
    role: str # <--- NUEVO

    class Config:
        from_attributes = True


class UserWithDevices(UserResponse):
    """Usuario con lista de dispositivos"""
    devices: List["DeviceResponse"] = []


# ============================================
# Device/Session Models
# ============================================

class DeviceResponse(BaseModel):
    """Respuesta de dispositivo"""
    id: str
    device_id: str
    device_name: Optional[str]
    device_type: DeviceType
    ip_address: Optional[str]
    last_activity: datetime
    created_at: datetime

    class Config:
        from_attributes = True


class SessionInfo(BaseModel):
    """Información de sesión activa"""
    user_id: str
    username: str
    device_id: str
    device_name: str
    device_type: DeviceType
    ip_address: str
    connected_since: datetime
    last_activity: datetime


# ============================================
# Auth Models
# ============================================

class ValidateCredentials(BaseModel):
    """Modelo para validar credenciales"""
    username: str
    password: str

class Token(BaseModel): # <--- NUEVO MODELO
    """Modelo de respuesta para Token JWT"""
    access_token: str
    token_type: str
    role: str


class AuthResult(BaseModel):
    """Resultado de autenticación"""
    valid: bool
    user_id: Optional[str] = None
    message: str
    can_connect: bool = False
    current_devices: int = 0
    max_devices: int = 0


# ============================================
# Playlist Models
# ============================================

class PlaylistInfo(BaseModel):
    """Información de playlist generada"""
    username: str
    total_channels: int
    total_movies: int
    total_series: int
    generated_at: datetime
    expires_at: Optional[datetime]


# ============================================
# Stats Models
# ============================================

class SystemStats(BaseModel):
    """Estadísticas del sistema"""
    total_users: int
    active_users: int
    total_sessions: int
    total_channels: int
    total_movies: int
    total_series: int


class UserStats(BaseModel):
    """Estadísticas de usuario"""
    user_id: str
    username: str
    active_devices: int
    max_connections: int
    total_streams_today: int
    is_active: bool
    expires_at: Optional[datetime]


# Actualizar forward references
UserWithDevices.model_rebuild()