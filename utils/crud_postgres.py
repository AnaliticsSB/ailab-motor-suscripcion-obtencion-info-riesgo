# Operaciones de lectura a Postgres para identificación de riesgos
import sqlalchemy
import pandas as pd
from utils.connect_sql import get_raw_connection
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncConnection
from models.models import IdentificacionRiesgosRequest
from typing import List, Dict, Any
import json


async def obtener_identificacion_riesgos(
    pool: AsyncConnection,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str,
    codigo_modificacion: str
) -> pd.DataFrame:
    """
    Consulta la tabla de identificación de riesgos y retorna un DataFrame.

    Parámetros:
    - request (IdentificacionRiesgosRequest): Contiene los 4 códigos de filtro (producto, subproducto, movimiento, modificación).
    - db (AsyncConnection): Conexión asíncrona a la base de datos proporcionada por FastAPI.

    Retorna:
    - pandas.DataFrame: DataFrame con los registros encontrados; vacío si no hay resultados.
    """
    # Consulta parametrizada a la tabla de identificación de riesgos
    sql = """
        SELECT *
        FROM "motor_suscripcion"."ms_identificacion_riesgos"
        WHERE "CODIGO_PRODUCTO" = :codigo_producto
          AND "CODIGO_SUBPRODUCTO" = :codigo_subproducto
          AND "CODIGO_MOVIMIENTO" = :codigo_movimiento
          AND COALESCE("CODIGO_MODIFICACION", '') = COALESCE(:codigo_modificacion, '')
    """

    # Construcción de query SQLAlchemy segura
    query = sqlalchemy.text(sql)
    # Parámetros para la consulta tomados del request
    params = {
        "codigo_producto": codigo_producto,
        "codigo_subproducto": codigo_subproducto,
        "codigo_movimiento": codigo_movimiento,
        "codigo_modificacion": codigo_modificacion,
    }

    # Ejecución asíncrona de la consulta
    results = await pool.execute(query, params)
    # Extrae registros como lista de mapeos (dict-like)
    registros = results.mappings().all()

    # Si no hay resultados, retorna DataFrame vacío
    if not registros:
        return pd.DataFrame()

    # Convierte a DataFrame para consumo posterior
    return pd.DataFrame(registros)


async def obtener_caso_id_por_consecutivo(
    pool: AsyncConnection, 
    consecutivo: int,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str,
    codigo_modificacion: str
) -> Any:
    """
    Obtiene el máximo CASO_ID de la tabla de estados a partir del CANAL_ID (consecutivo)
    y otros filtros.
    """
    sql = """
        SELECT MAX("CASO_ID")
        FROM "motor_suscripcion"."ms_estados_casos"
        WHERE "CANAL_ID" = :consecutivo
          AND "CODIGO_PRODUCTO" = :codigo_producto
          AND "CODIGO_SUBPRODUCTO" = :codigo_subproducto
          AND "CODIGO_MOVIMIENTO" = :codigo_movimiento
          AND COALESCE("CODIGO_MODIFICACION", '') = COALESCE(:codigo_modificacion, '')
          AND "ESTADO" = 'PENDIENTE'
    """
    query = sqlalchemy.text(sql)
    params = {
        "consecutivo": consecutivo,
        "codigo_producto": codigo_producto,
        "codigo_subproducto": codigo_subproducto,
        "codigo_movimiento": codigo_movimiento,
        "codigo_modificacion": codigo_modificacion,
    }
    
    result = await pool.execute(query, params)
    caso_id_result = result.scalar_one_or_none()
    
    return caso_id_result


async def insertar_resultados_riesgos(
    pool: AsyncConnection, 
    riesgos: List[Dict[str, Any]], 
    caso_id: Any,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str
):
    """
    Inserta una lista de riesgos en la tabla ms_resultados.
    Si "RIESGO_MOTOR_ID" ya existe (PK/UNIQUE), no inserta ese registro.
    """
    if not riesgos:
        return

    registros_a_insertar = []
    for riesgo in riesgos:
        tipo_documento = riesgo.get("TIPO_DOCUMENTO")
        numero_documento = riesgo.get("NUMERO_DOCUMENTO")
        placa = riesgo.get("PLACA")

        riesgo_motor_id = f"{caso_id}-{codigo_producto}-{codigo_subproducto}-{codigo_movimiento}-{tipo_documento}-{numero_documento}"
        if placa:
            riesgo_motor_id += f"-{placa}"

        registro = {
            "RIESGO_MOTOR_ID": riesgo_motor_id,
            "CASO_ID": caso_id,
            "TIPO_DOCUMENTO_ASEGURADO": tipo_documento,
            "NUMERO_DOCUMENTO_ASEGURADO": int(numero_documento) if numero_documento else None,
            "NOMBRE_ASEGURADO": riesgo.get("NOMBRE")
        }
        print(f"Registro a insertar: {registro}")
        registros_a_insertar.append(registro)

    if not registros_a_insertar:
        return

    sql = """
        INSERT INTO "motor_suscripcion"."ms_resultados" (
            "RIESGO_MOTOR_ID", "CASO_ID", "TIPO_DOCUMENTO_ASEGURADO", 
            "NUMERO_DOCUMENTO_ASEGURADO", "NOMBRE_ASEGURADO"
        ) VALUES (
            :RIESGO_MOTOR_ID, :CASO_ID, :TIPO_DOCUMENTO_ASEGURADO, 
            :NUMERO_DOCUMENTO_ASEGURADO, :NOMBRE_ASEGURADO
        )
        ON CONFLICT ("RIESGO_MOTOR_ID") DO NOTHING;
    """
    query = sqlalchemy.text(sql)
    
    try:
        await pool.execute(query, registros_a_insertar)
        print(f"Registros insertados: {len(registros_a_insertar)}")
        await pool.commit()
    except Exception as e:
        await pool.rollback()
        # En un entorno real, aquí se debería loguear el error 'e'.
        print(f"Error al insertar en la base de datos: {e}")
        raise

    await actualizar_estado_caso(pool, caso_id)


async def actualizar_estado_caso(pool: AsyncConnection, caso_id: Any):
    """
    Actualiza el estado de un caso a 'RIESGOS IDENTIFICADOS'.
    """
    sql = """
        UPDATE "motor_suscripcion"."ms_estados_casos"
        SET "ESTADO" = 'RIESGOS IDENTIFICADOS'
        WHERE "CASO_ID" = :caso_id;
    """
    query = sqlalchemy.text(sql)
    params = {"caso_id": caso_id}

    try:
        await pool.execute(query, params)
        await pool.commit()
    except Exception as e:
        await pool.rollback()
        print(f"Error al actualizar estado en 'ms_estados_casos': {e}")
        raise 