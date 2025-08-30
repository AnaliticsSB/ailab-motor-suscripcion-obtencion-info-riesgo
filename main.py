# -*- coding: utf-8 -*-
# --- 1. Importaciones ---
# Se importa 'sys' para manipular la ruta de búsqueda de módulos de Python.
import sys
# Se añade el directorio raíz del proyecto a la ruta para permitir importaciones absolutas.
sys.path.append('.')

# --- Módulos estándar y de terceros ---
import os
from fastapi import FastAPI, Depends, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncConnection
from typing import Dict, Any

# --- Módulos locales de la aplicación ---
from models.models import IdentificacionRiesgosRequest, InfoRiesgosResponse
from utils.connect_sql import create_db_engine_async, get_raw_connection
from utils.crud_postgres import obtener_identificacion_riesgos
from helpers.obtener_info_riesgos import obtener_info_riesgo_individual, obtener_info_riesgos_colectiva


# --- 2. Gestión del Ciclo de Vida de la Aplicación (Lifespan) ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona los recursos de la aplicación (como la conexión a la BD) durante su ciclo de vida.

    Se ejecuta al iniciar la aplicación para crear y establecer la conexión a la base
    de datos, y al detenerse para cerrar la conexión de forma segura.
    """
    # --- Código de inicio ---
    # 1. Se crea el motor de base de datos asíncrono y el conector.
    engine, connector = await create_db_engine_async()
    # 2. Se guardan en el estado de la aplicación para que sean accesibles globalmente.
    app.state.db_engine = engine
    app.state.db_connector = connector
    print("INFO:     Conexión a la base de datos establecida.")
    
    # La aplicación se ejecuta en este punto.
    yield
    
    # --- Código de finalización ---
    # 3. Se liberan los recursos de la base de datos de forma ordenada.
    await app.state.db_engine.dispose()
    await app.state.db_connector.close_async()
    print("INFO:     Conexión a la base de datos cerrada.")


# --- 3. Inicialización de la Aplicación FastAPI ---
# Se crea la instancia de la aplicación FastAPI con metadatos para la documentación.
app = FastAPI(
    title="Obtención de Información de Riesgos",
    description="API para identificar y obtener la información de riesgos de un caso para el motor de suscripción.",
    version="1.0.0",
    lifespan=lifespan # Se asocia el gestor de ciclo de vida.
)

# --- Configuración de Middlewares ---
# Se añade middleware para comprimir respuestas grandes (más de 1000 bytes).
app.add_middleware(GZipMiddleware, minimum_size=1000)
# Se lee el puerto desde variables de entorno, con un valor por defecto para desarrollo.
puerto = os.environ.get("PORT", 8080)
# Se configura CORS para permitir peticiones desde cualquier origen.
origins = ["*"]
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# --- 4. Definición del Endpoint Principal ---
# Metadatos para la documentación automática de la API.
descripcion_path = 'API que identifica y obtiene la información de riesgos de un caso.'
summary_path = 'Obtención de información de riesgos.'
endpoint_end = '/api/obtener_info_riesgos'

# Se decora la función para definirla como un endpoint POST.
# 'response_model' asegura que la salida cumpla con la estructura de 'InfoRiesgosResponse'.
@app.post(endpoint_end, summary=summary_path, description=descripcion_path, response_model=InfoRiesgosResponse)
async def api_obtener_info_riesgos(
    request: IdentificacionRiesgosRequest,
    pool: AsyncConnection = Depends(get_raw_connection)
) -> Dict[str, Any]:
    """
    Endpoint principal que orquesta la obtención de información de riesgos.

    1.  Consulta la tabla de configuración `ms_identificacion_riesgos` en PostgreSQL.
    2.  Determina si el producto es de tipo 'INDIVIDUAL' o 'COLECTIVO'.
    3.  Delega el procesamiento a la función helper correspondiente, la cual se
        encarga de llamar a las APIs externas y procesar los resultados.
    4.  Retorna la información del riesgo o riesgos en el formato de respuesta definido.
    """
    # Paso 1: Obtener la configuración desde la base de datos.
    df_info_riesgos = await obtener_identificacion_riesgos(
        pool=pool,
        codigo_producto=request.codigo_producto,
        codigo_subproducto=request.codigo_subproducto,
        codigo_movimiento=request.codigo_movimiento,
        codigo_modificacion=request.codigo_modificacion
    )

    # Si no se encuentra configuración, se devuelve un error 404 (Not Found).
    if df_info_riesgos.empty:
        raise HTTPException(status_code=404, detail="No se encontró configuración para los parámetros proporcionados.")

    # Paso 2: Determinar el tipo de producto. Se toma de la primera fila de la configuración.
    tipo_producto = df_info_riesgos['TIPO_PRODUCTO'].iloc[0].strip().upper()
    
    resultado = {}
    
    # Paso 3: Seleccionar y ejecutar el flujo de procesamiento adecuado.
    if tipo_producto == 'INDIVIDUAL':
        # Para productos individuales, se llama a la función de procesamiento en paralelo.
        resultado = await obtener_info_riesgo_individual(
            df_info_riesgos=df_info_riesgos,
            consecutivo=request.consecutivo,
            pool=pool,
            codigo_producto=request.codigo_producto,
            codigo_subproducto=request.codigo_subproducto,
            codigo_movimiento=request.codigo_movimiento,
            codigo_modificacion=request.codigo_modificacion
        )
    elif tipo_producto == 'COLECTIVO':
        # Para productos colectivos, se llama a la función de procesamiento secuencial.
        resultado = await obtener_info_riesgos_colectiva(
            df_info_riesgos=df_info_riesgos,
            consecutivo=request.consecutivo,
            pool=pool,
            codigo_producto=request.codigo_producto,
            codigo_subproducto=request.codigo_subproducto,
            codigo_movimiento=request.codigo_movimiento,
            codigo_modificacion=request.codigo_modificacion
        )
    else:
        # Si el tipo de producto no es uno de los esperados, se devuelve un error 400 (Bad Request).
        raise HTTPException(status_code=400, detail=f"Tipo de producto '{tipo_producto}' no es válido.")

    # Si el procesamiento no encontró ningún riesgo, se añade un mensaje informativo a la respuesta.
    if not resultado.get("riesgos"):
        resultado["mensaje"] = "No se encontró ningún riesgo con los parámetros proporcionados."

    # Paso 4: Retornar el resultado final.
    return resultado


# --- 5. Endpoint de Redirección a la Documentación ---
@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def redirect_to_docs():
    """
    Redirige la ruta raíz ("/") a la documentación interactiva de la API ("/docs").
    """
    return "/docs"


# --- 6. Ejecución Local de la Aplicación ---
# Este bloque solo se ejecuta si el script es llamado directamente (ej. `python main.py`).
if __name__ == "__main__":
    import uvicorn
    # Se utiliza 'uvicorn' para correr el servidor de la aplicación FastAPI.
    # 'reload=True' reinicia el servidor automáticamente al detectar cambios en el código.
    uvicorn.run("main:app", host="0.0.0.0", port=int(puerto), reload=True)


