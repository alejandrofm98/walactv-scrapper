# 🏆 WALACTV-SCRAPPER

[![Python 3.12](https://img.shields.io/badge/python-3.12-blue.svg)](https://www.python.org/downloads/)
[![Ansible](https://img.shields.io/badge/Ansible-2.9+-red.svg)](https://www.ansible.com/)

Un scraper en Python que recopila y organiza información de eventos deportivos de [Fútbol en la TV](https://www.futbolenlatv.es/) y los integra con Firebase, incluyendo mapeo de enlaces Acestream para transmisiones en vivo.

## 📋 Características

- Extracción automática de eventos deportivos programados
- Integración con Firebase para almacenamiento de datos
- Mapeo de enlaces Acestream para transmisiones en vivo
- Ejecución automatizada mediante cron jobs
- Despliegue automatizado con Ansible
- Configuración flexible para múltiples días de programación

## 🚀 Requisitos Previos

- Python 3.12
- Ansible 2.9+
- Cuenta de Firebase
- Acceso SSH a servidor remoto para despliegue

## 🛠 Instalación

### Configuración Local

1. Clona el repositorio:
   ```bash
   git clone https://github.com/tu-usuario/walactv-scrapper.git
   cd walactv-scrapper
   ```

2. Crea un entorno virtual e instala las dependencias:
   ```bash
   python -m venv venv
   source venv/bin/activate  # En Windows: venv\Scripts\activate
   pip install -r requirements.txt
   ```

3. Configura las variables de entorno:
   - Copia `.env.example` a `.env`
   - Completa con tus credenciales de Firebase

### Configuración del Servidor

1. Crea el archivo de inventario basado en el ejemplo:
   ```bash
   cp maquinas.example maquinas
   ```
   Edita el archivo `maquinas` con la información de tu servidor.


2. Despliega la aplicación y configura el cron job:
   ```bash
   cd ansible &&
   ansible-playbook ansible_playbook.yml -e "entorno=pro"
   ```
   
3. Levanta nginx:
   ```bash
   cd ~/IdeaProjects/config-servidores/configuraciones/ansible &&
   ansible-playbook nginx_conf.yaml -e "entorno=pro"
   ```

4. Despliega nueva version apk:
   ```bash
   cd ansible &&
   ansible-playbook despliegue_apk.yml -e "entorno=pro"
   ```
   
## ⚙️ Configuración

### Variables de Entorno

Crea un archivo `.env` en la raíz del proyecto con las siguientes variables:

```
FIREBASE_API_KEY=tu_api_key
FIREBASE_AUTH_DOMAIN=tu_proyecto.firebaseapp.com
FIREBASE_DATABASE_URL=https://tu_proyecto.firebaseio.com
FIREBASE_STORAGE_BUCKET=tu_proyecto.appspot.com
```

### Configuración del Cron Job

Por defecto, el script está configurado para ejecutarse diariamente a las 8:00 AM. Para modificar esta configuración:

1. Edita el archivo `ansible/ansible_playbook.yaml`
2. Busca la sección de configuración del cron job
3. Modifica el horario según sea necesario

## 🚦 Uso

### Ejecución Manual

Para ejecutar el script manualmente:

```bash
python3 scripts/main.py
```

### Parámetros Opcionales

- `--days`: Número de días a consultar (por defecto: 2)
- `--debug`: Activar modo depuración

Ejemplo:
```bash
python3 scripts/main.py --days 3 --debug
```

## 🤝 Contribución

Las contribuciones son bienvenidas. Por favor, lee nuestras pautas de contribución antes de enviar un pull request.

## 📄 Licencia

Este proyecto está bajo la Licencia MIT - ver el archivo [LICENSE](LICENSE) para más detalles.

## 📞 Contacto

Para consultas o soporte, por favor abre un issue en el repositorio.
