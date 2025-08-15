# Configuración de paths para imports relativos desde la raíz
import sys
sys.path.append('.')

# Librerías estándar y frameworks
import os
import pandas as pd
from fastapi import FastAPI, Depends
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import RedirectResponse
from fastapi.middleware.gzip import GZipMiddleware
from contextlib import asynccontextmanager
from sqlalchemy.ext.asyncio import AsyncConnection
from typing import Dict, Any
from fastapi import HTTPException

# Funciones internas del servicio
from models.models import IdentificacionRiesgosRequest, InfoRiesgosResponse
from utils.connect_sql import create_db_engine_async, get_raw_connection
from utils.crud_postgres import obtener_identificacion_riesgos
from helpers.obtener_info_riesgos import obtener_info_riesgo_individual, obtener_info_riesgos_colectiva


# Ciclo de vida de la aplicación: apertura y cierre de recursos (BD)
@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Gestiona recursos de BD durante el ciclo de vida de la aplicación.

    Parámetros:
    - app (FastAPI): Instancia de la aplicación FastAPI.

    Retorna:
    - None: Usa 'yield' para ejecutar la app y luego cerrar recursos.
    """
    # Crea el engine y conector de Cloud SQL
    engine, connector = await create_db_engine_async()
    # Guarda referencias en el estado de la app
    app.state.db_engine = engine
    app.state.db_connector = connector
    print("INFO:     Conexión a la base de datos establecida.")
    # Cede el control mientras la app está activa
    yield
    # Al finalizar, cierra los recursos
    await app.state.db_engine.dispose()
    await app.state.db_connector.close_async()
    print("INFO:     Conexión a la base de datos cerrada.")


# Instancia FastAPI con metadatos
app = FastAPI(
    title="Obtención de Información de Riesgos",
    description="API para identificar y obtener la información de riesgos de un caso filenet para el motor de suscripción.",
    version="1.0.0",
    lifespan=lifespan
)

# Middlewares de compresión y CORS
app.add_middleware(GZipMiddleware, minimum_size=1000)
puerto = os.environ.get("PORT", 8080)  # Puerto configurable por entorno
origins = ["*"]  # Orígenes permitidos
app.add_middleware(
    CORSMiddleware,
    allow_origins=origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Metadatos y ruta del endpoint principal
descripcion_path = 'API que identifica y obtiene la información de riesgos de un caso filenet.'
summary_path = 'Obtención de información de riesgos.'
endpoint_end = '/api/obtener_info_riesgos'

# Ejemplo de entrada para documentación.
"""
{
    "codigo_producto": 1,
    "codigo_subproducto": 1,
    "codigo_movimiento": "1",
    "codigo_modificacion": "1",
    "consecutivo": 203586
}
"""

# Endpoint principal: identifica y enruta según el tipo de producto
@app.post(endpoint_end, summary=summary_path, description=descripcion_path, response_model=InfoRiesgosResponse)
async def api_obtener_info_riesgos(
    request: IdentificacionRiesgosRequest,  # Body con 4 códigos de filtro + consecutivo
    pool: AsyncConnection = Depends(get_raw_connection)
) -> Dict[str, Any]:
    """
    Endpoint principal para la obtención de información de riesgos.

    Este endpoint orquesta todo el flujo:
    1.  Consulta la tabla de configuración `ms_identificacion_riesgos` en PostgreSQL
        utilizando los códigos proporcionados en la solicitud.
    2.  Determina si el producto es de tipo 'INDIVIDUAL' o 'COLECTIVO'.
    3.  Delega el procesamiento a la función correspondiente (`obtener_info_riesgo_individual`
        u `obtener_info_riesgos_colectiva`), que se encarga de llamar a las APIs
        externas y procesar los resultados.
    4.  Retorna la información del riesgo o los riesgos en el formato de respuesta definido.
    """
    # Obtiene el DataFrame con la configuración de identificación de riesgos desde la base de datos.
    df_info_riesgos = await obtener_identificacion_riesgos(
        pool=pool,
        codigo_producto=request.codigo_producto,
        codigo_subproducto=request.codigo_subproducto,
        codigo_movimiento=request.codigo_movimiento,
        codigo_modificacion=request.codigo_modificacion
    )

    # Si no se encuentra configuración, se devuelve una respuesta de error.
    if df_info_riesgos.empty:
        raise HTTPException(status_code=404, detail="No se encontró configuración para los parámetros proporcionados.")

    # Se extrae el tipo de producto de la configuración.
    tipo_producto = df_info_riesgos['TIPO_PRODUCTO'].iloc[0].strip().upper()
    
    # Se inicializa la variable de resultado.
    resultado = {}
    
    # Se selecciona el flujo de procesamiento basado en el tipo de producto.
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
        # Si el tipo de producto no es válido, se devuelve un error.
        raise HTTPException(status_code=400, detail=f"Tipo de producto '{tipo_producto}' no es válido.")

    # Si no se encontraron riesgos, agregar un mensaje informativo
    if not resultado.get("riesgos"):
        resultado["mensaje"] = "No se encontró ningún riesgo con los parámetros proporcionados."

    # Se retorna el resultado obtenido.
    return resultado


# Redirección a documentación interactiva
@app.get("/", response_class=RedirectResponse, include_in_schema=False)
async def redirect_to_docs():
    """
    Redirige la ruta raíz a la documentación interactiva (/docs).

    Parámetros:
    - Ninguno.

    Retorna:
    - str: Cadena con la ruta a la documentación.
    """
    return "/docs"


# Ejecución local con recarga automática
if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=int(puerto), reload=True)


