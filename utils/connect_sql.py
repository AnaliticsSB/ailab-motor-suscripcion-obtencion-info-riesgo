"""
Este módulo gestiona la conexión a la base de datos de Google Cloud SQL.

Se encarga de obtener las credenciales de forma segura desde Secret Manager
y de establecer un pool de conexiones asíncronas utilizando SQLAlchemy y el
conector de Cloud SQL.
"""
# --- Importaciones ---
import json
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
from google.cloud import secretmanager
from google.cloud.sql.connector import Connector, IPTypes
from fastapi import Request


def get_credentials():
    """Obtiene las credenciales de la base de datos desde Google Secret Manager.

    Se conecta al servicio de Secret Manager, accede a la versión más reciente
    del secreto especificado y devuelve las credenciales en formato de diccionario.

    Returns:
        Un diccionario con las credenciales de la base de datos
        (host, user, password, database).
    """
    client = secretmanager.SecretManagerServiceClient()
    # Ruta del secreto en Google Cloud.
    postgres_creds = 'projects/911414108629/secrets/postgres-db-stage-credentials-usr_dev_stage/versions/latest'
    # Acceso al contenido del secreto.
    response = client.access_secret_version(name=postgres_creds, ).payload.data.decode("UTF-8")
    
    # Se convierte el JSON del secreto a un diccionario de Python.
    creds_dict = json.loads(response)
    return creds_dict


async def create_db_engine_async():
    """Crea y configura el motor de conexión asíncrono de SQLAlchemy.

    Utiliza el conector de Google Cloud SQL para establecer una conexión segura
    y crea un 'engine' de SQLAlchemy que gestionará un pool de conexiones
    asíncronas para ser utilizadas por la aplicación.

    Returns:
        Una tupla que contiene el motor de SQLAlchemy (engine) y la instancia
        del conector de Cloud SQL.
    """
    # Se obtienen las credenciales.
    creds_dict = get_credentials()
    INSTANCE_CONNECTION_NAME = creds_dict['host']
    DB_USER = creds_dict['user']
    DB_PASS = creds_dict['password']
    DB_NAME = creds_dict['database']

    connector = Connector()

    # Función interna que el motor de SQLAlchemy usará para crear cada nueva conexión.
    async def getconn():
        return await connector.connect_async(
            INSTANCE_CONNECTION_NAME,
            driver="asyncpg",
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IPTypes.PUBLIC
        )

    # Se crea el motor (engine) que gestiona el pool de conexiones.
    engine = create_async_engine(
        "postgresql+asyncpg://",
        async_creator=getconn
    )
    
    # Se realiza una conexión de prueba para validar la configuración.
    async with engine.begin():
        pass
    return engine, connector


async def get_raw_connection(request: Request) -> AsyncConnection:
    """Proporciona una conexión a la base de datos como una dependencia de FastAPI.

    Obtiene una conexión del pool gestionado por el motor de SQLAlchemy, que fue
    previamente almacenado en el estado de la aplicación.

    Args:
        request: El objeto de la solicitud de FastAPI, usado para acceder al
                 estado de la aplicación (app.state).

    Yields:
        Una conexión asíncrona a la base de datos.
    """
    engine = request.app.state.db_engine
    async with engine.connect() as conn:
        yield conn





    