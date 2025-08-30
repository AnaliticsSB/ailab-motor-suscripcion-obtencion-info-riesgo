# -*- coding: utf-8 -*-
"""
Este módulo gestiona la conexión a la base de datos de Google Cloud SQL.

Se encarga de obtener las credenciales de forma segura desde Secret Manager
y de establecer un pool de conexiones asíncronas utilizando SQLAlchemy y el
conector de Cloud SQL, optimizado para su uso en un entorno de FastAPI.
"""
# --- Importaciones ---
# Se importa 'json' para procesar las credenciales que vienen en formato JSON.
import json
# Se importan componentes de SQLAlchemy para crear un motor de base de datos asíncrono.
from sqlalchemy.ext.asyncio import create_async_engine, AsyncConnection
# Se importa el cliente de Google Secret Manager para acceder a secretos de forma segura.
from google.cloud import secretmanager
# Se importa el conector de Google Cloud SQL para gestionar la conexión segura.
from google.cloud.sql.connector import Connector, IPTypes
# Se importa 'Request' de FastAPI para poder acceder al estado de la aplicación.
from fastapi import Request

# --- Funciones ---

def get_credentials():
    """Obtiene las credenciales de la base de datos desde Google Secret Manager.

    Se conecta al servicio de Secret Manager, accede a la versión más reciente
    del secreto especificado y devuelve las credenciales en formato de diccionario.

    Returns:
        dict: Un diccionario con las credenciales de la base de datos
              (host, user, password, database).
    """
    # 1. Se inicializa el cliente del servicio de Secret Manager.
    client = secretmanager.SecretManagerServiceClient()
    
    # 2. Se define la ruta completa del secreto en Google Cloud.
    #    Esta ruta identifica de forma única el secreto que contiene las credenciales.
    postgres_creds = 'projects/911414108629/secrets/postgres-db-stage-credentials-usr_dev_stage/versions/latest'
    
    # 3. Se accede al contenido (payload) de la versión del secreto.
    response = client.access_secret_version(name=postgres_creds).payload.data.decode("UTF-8")
    
    # 4. Se convierte la cadena de texto JSON del secreto a un diccionario de Python.
    creds_dict = json.loads(response)
    
    # 5. Se retorna el diccionario con las credenciales listas para usar.
    return creds_dict


async def create_db_engine_async():
    """Crea y configura el motor de conexión asíncrono de SQLAlchemy.

    Utiliza el conector de Google Cloud SQL para establecer una conexión segura
    y crea un 'engine' de SQLAlchemy que gestionará un pool de conexiones
    asíncronas para ser utilizadas por la aplicación.

    Returns:
        tuple: Una tupla que contiene el motor de SQLAlchemy (engine) y la instancia
               del conector de Cloud SQL.
    """
    # 1. Se obtienen las credenciales de la base de datos llamando a la función auxiliar.
    creds_dict = get_credentials()
    
    # 2. Se extraen los valores de las credenciales en variables individuales.
    INSTANCE_CONNECTION_NAME = creds_dict['host']  # Nombre de la instancia de Cloud SQL.
    DB_USER = creds_dict['user']                    # Nombre de usuario de la base de datos.
    DB_PASS = creds_dict['password']                # Contraseña del usuario.
    DB_NAME = creds_dict['database']                # Nombre de la base de datos.

    # 3. Se inicializa el conector de Google Cloud SQL.
    connector = Connector()

    # 4. Se define una función anidada 'getconn' que el motor de SQLAlchemy usará
    #    internamente cada vez que necesite crear una nueva conexión a la base de datos.
    async def getconn():
        # Se utiliza el conector para establecer una conexión segura y asíncrona.
        conn = await connector.connect_async(
            INSTANCE_CONNECTION_NAME,
            "asyncpg",  # Se especifica el driver de base de datos asíncrono.
            user=DB_USER,
            password=DB_PASS,
            db=DB_NAME,
            ip_type=IPTypes.PUBLIC  # Se especifica el tipo de IP a usar para la conexión.
        )
        return conn

    # 5. Se crea el motor (engine) de SQLAlchemy, que gestiona el pool de conexiones.
    #    Se le pasa la función 'getconn' para que sepa cómo crear conexiones.
    engine = create_async_engine(
        "postgresql+asyncpg://",
        creator=getconn
    )
    
    # 6. Se realiza una conexión de prueba para validar que la configuración es correcta.
    #    Si esto falla, la aplicación no se iniciará, previniendo errores en tiempo de ejecución.
    async with engine.begin():
        pass
    
    # 7. Se retorna tanto el motor como el conector para ser gestionados en el ciclo de vida de la app.
    return engine, connector


async def get_raw_connection(request: Request) -> AsyncConnection:
    """Proporciona una conexión a la base de datos como una dependencia de FastAPI.

    Obtiene una conexión del pool gestionado por el motor de SQLAlchemy, que fue
    previamente almacenado en el estado de la aplicación. Esto permite que cada
    endpoint pueda solicitar y recibir una conexión de forma sencilla y eficiente.

    Args:
        request (Request): El objeto de la solicitud de FastAPI, usado para acceder al
                           estado de la aplicación (app.state) donde se guardó el motor.

    Yields:
        AsyncConnection: Una conexión asíncrona a la base de datos, lista para ser usada.
    """
    # 1. Se accede al motor de la base de datos que fue guardado en el estado de la aplicación.
    engine = request.app.state.db_engine
    
    # 2. Se solicita una conexión del pool gestionado por el motor.
    #    El bloque 'async with' asegura que la conexión se devuelva al pool automáticamente
    #    al finalizar la operación del endpoint, incluso si ocurren errores.
    async with engine.connect() as conn:
        # 3. Se cede (yield) la conexión al endpoint que la solicitó.
        yield conn





    