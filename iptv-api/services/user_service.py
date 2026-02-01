"""
Servicio de gestión de usuarios
"""
import bcrypt
from datetime import datetime
from typing import Optional, List, Dict, Any
from supabase import Client

from utils.config import get_settings
from utils.models import UserCreate, UserUpdate, UserResponse, AuthResult


class UserService:
    """Servicio para gestión de usuarios"""

    def __init__(self, supabase: Client):
        self.supabase = supabase
        self.settings = get_settings()

    def _hash_password(self, password: str) -> str:
        """Hashea una contraseña"""
        salt = bcrypt.gensalt()
        return bcrypt.hashpw(password.encode('utf-8'), salt).decode('utf-8')

    def _verify_password(self, password: str, hashed: str) -> bool:
        """Verifica una contraseña contra su hash"""
        try:
            return bcrypt.checkpw(password.encode('utf-8'), hashed.encode('utf-8'))
        except Exception:
            return False

    def create_user(self, user_data: UserCreate) -> Dict[str, Any]:
        """Crea un nuevo usuario"""
        # Verificar si ya existe
        existing = self.supabase.table('users').select('id').eq(
            'username', user_data.username
        ).execute()

        if existing.data:
            raise ValueError(f"El usuario '{user_data.username}' ya existe")

        # Determinar el rol (por defecto 'user' si no se especifica)
        # Asegúrate de que tu modelo UserCreate tenga el campo role opcional
        role = getattr(user_data, 'role', 'user')

        # Crear usuario
        user_dict = {
            'username': user_data.username,
            'password_hash': self._hash_password(user_data.password),
            'max_connections': user_data.max_connections,
            'is_active': True,
            'expires_at': user_data.expires_at.isoformat() if user_data.expires_at else None,
            'role': role  # <--- NUEVO CAMPO
        }

        result = self.supabase.table('users').insert(user_dict).execute()

        if result.data:
            user = result.data[0]
            # No retornar el hash
            del user['password_hash']
            return user

        raise Exception("Error al crear usuario")

    def get_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene un usuario por ID"""
        result = self.supabase.table('users').select(
            'id, username, max_connections, is_active, expires_at, created_at, role' # <--- AÑADIDO ROLE
        ).eq('id', user_id).execute()

        if result.data:
            user = result.data[0]
            # Contar dispositivos activos
            sessions = self.supabase.table('active_sessions').select(
                'id', count='exact'
            ).eq('user_id', user_id).execute()
            user['active_devices'] = sessions.count or 0
            return user

        return None

    def get_user_by_username(self, username: str) -> Optional[Dict[str, Any]]:
        """Obtiene un usuario por username (incluyendo password_hash y role)"""
        # Usamos '*' para obtener el hash y el role
        result = self.supabase.table('users').select('*').eq(
            'username', username
        ).execute()

        return result.data[0] if result.data else None

    def list_users(self, skip: int = 0, limit: int = 100) -> List[Dict[str, Any]]:
        """Lista todos los usuarios"""
        result = self.supabase.table('users').select(
            'id, username, max_connections, is_active, expires_at, created_at, role' # <--- AÑADIDO ROLE
        ).range(skip, skip + limit - 1).order('created_at', desc=True).execute()

        users = result.data or []

        # Añadir conteo de dispositivos activos a cada usuario
        for user in users:
            sessions = self.supabase.table('active_sessions').select(
                'id', count='exact'
            ).eq('user_id', user['id']).execute()
            user['active_devices'] = sessions.count or 0

        return users

    def update_user(self, user_id: str, user_data: UserUpdate) -> Optional[Dict[str, Any]]:
        """Actualiza un usuario"""
        update_dict = {}

        if user_data.password is not None:
            update_dict['password_hash'] = self._hash_password(user_data.password)

        if user_data.max_connections is not None:
            update_dict['max_connections'] = user_data.max_connections

        if user_data.is_active is not None:
            update_dict['is_active'] = user_data.is_active

        if user_data.expires_at is not None:
            update_dict['expires_at'] = user_data.expires_at.isoformat()

        # Permitir cambiar rol (si viene en UserUpdate)
        if hasattr(user_data, 'role') and user_data.role is not None:
            update_dict['role'] = user_data.role

        if not update_dict:
            return self.get_user(user_id)

        result = self.supabase.table('users').update(update_dict).eq(
            'id', user_id
        ).execute()

        if result.data:
            return self.get_user(user_id)

        return None

    def delete_user(self, user_id: str) -> bool:
        """Elimina un usuario"""
        # Primero eliminar sesiones activas
        self.supabase.table('active_sessions').delete().eq('user_id', user_id).execute()

        # Luego eliminar usuario
        result = self.supabase.table('users').delete().eq('id', user_id).execute()

        return bool(result.data)

    def validate_credentials(self, username: str, password: str) -> AuthResult:
        """Valida credenciales de usuario (para streams y m3u)"""
        user = self.get_user_by_username(username)

        if not user:
            return AuthResult(
                valid=False,
                message="Usuario no encontrado",
                can_connect=False
            )

        if not self._verify_password(password, user['password_hash']):
            return AuthResult(
                valid=False,
                message="Contraseña incorrecta",
                can_connect=False
            )

        if not user['is_active']:
            return AuthResult(
                valid=True,
                user_id=user['id'],
                message="Cuenta desactivada",
                can_connect=False,
                max_devices=user['max_connections']
            )

        # Verificar expiración
        if user['expires_at']:
            # Manejo seguro de zonas horarias si la cadena viene sin Z
            expires_str = user['expires_at']
            if expires_str.endswith('Z'):
                expires_str = expires_str[:-1] + '+00:00'

            try:
                expires = datetime.fromisoformat(expires_str)
                if datetime.now(expires.tzinfo) > expires:
                    return AuthResult(
                        valid=True,
                        user_id=user['id'],
                        message="Cuenta expirada",
                        can_connect=False,
                        max_devices=user['max_connections']
                    )
            except ValueError:
                print("Error parseando fecha expiración")

        # Contar sesiones activas
        sessions = self.supabase.table('active_sessions').select(
            'id', count='exact'
        ).eq('user_id', user['id']).execute()
        current_devices = sessions.count or 0

        return AuthResult(
            valid=True,
            user_id=user['id'],
            message="Credenciales válidas",
            can_connect=True,
            current_devices=current_devices,
            max_devices=user['max_connections']
        )

    def get_user_stats(self, user_id: str) -> Optional[Dict[str, Any]]:
        """Obtiene estadísticas de un usuario"""
        user = self.get_user(user_id)
        if not user:
            return None

        return {
            'user_id': user['id'],
            'username': user['username'],
            'active_devices': user['active_devices'],
            'max_connections': user['max_connections'],
            'is_active': user['is_active'],
            'expires_at': user['expires_at'],
            'role': user.get('role', 'user')
        }