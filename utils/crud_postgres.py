# -*- coding: utf-8 -*-
"""
Este módulo contiene las operaciones CRUD (Crear, Leer, Actualizar, Borrar)
para interactuar con la base de datos PostgreSQL.

Centraliza toda la lógica de acceso a datos, manteniendo las consultas SQL
separadas del resto de la lógica de la aplicación.
"""

# --- Importaciones ---
# Se importa sqlalchemy para construir y ejecutar consultas SQL de forma segura.
import sqlalchemy
# Se importa pandas para estructurar los resultados de las consultas en DataFrames.
import pandas as pd
# Se importa el tipo 'AsyncConnection' para el tipado estático de la conexión.
from sqlalchemy.ext.asyncio import AsyncConnection
# Se importan tipos de Python para definir la estructura de los datos.
from typing import List, Dict, Any

# --- Funciones CRUD ---

async def obtener_identificacion_riesgos(
    pool: AsyncConnection,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str,
    codigo_modificacion: str
) -> pd.DataFrame:
    """
    Consulta la tabla de configuración 'ms_identificacion_riesgos'.

    Filtra los registros basándose en los códigos del producto, subproducto,
    movimiento y modificación para obtener la configuración específica de cómo
    identificar y procesar los riesgos para un caso determinado.

    Args:
        pool (AsyncConnection): La conexión asíncrona a la base de datos.
        codigo_producto (int): Código del producto a filtrar.
        codigo_subproducto (int): Código del subproducto a filtrar.
        codigo_movimiento (str): Código del movimiento a filtrar.
        codigo_modificacion (str): Código de la modificación a filtrar.

    Returns:
        pd.DataFrame: Un DataFrame de pandas con la configuración encontrada.
                      Retorna un DataFrame vacío si no hay resultados.
    """
    # 1. Se define la consulta SQL parametrizada para evitar inyección SQL.
    #    COALESCE se usa para manejar correctamente los valores nulos en 'CODIGO_MODIFICACION'.
    sql = """
        SELECT *
        FROM "motor_suscripcion"."ms_identificacion_riesgos"
        WHERE "CODIGO_PRODUCTO" = :codigo_producto
          AND "CODIGO_SUBPRODUCTO" = :codigo_subproducto
          AND "CODIGO_MOVIMIENTO" = :codigo_movimiento
          AND COALESCE("CODIGO_MODIFICACION", '') = COALESCE(:codigo_modificacion, '')
    """

    # 2. Se convierte la cadena SQL a un objeto 'text' de SQLAlchemy.
    query = sqlalchemy.text(sql)
    # 3. Se definen los parámetros para la consulta a partir de los argumentos de la función.
    params = {
        "codigo_producto": codigo_producto,
        "codigo_subproducto": codigo_subproducto,
        "codigo_movimiento": codigo_movimiento,
        "codigo_modificacion": codigo_modificacion,
    }

    # 4. Se ejecuta la consulta de forma asíncrona.
    results = await pool.execute(query, params)
    # 5. Se obtienen todos los registros como una lista de diccionarios.
    registros = results.mappings().all()

    # 6. Si no se encontraron registros, se retorna un DataFrame vacío.
    if not registros:
        return pd.DataFrame()

    # 7. Se convierten los resultados a un DataFrame de pandas y se retorna.
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
    Obtiene el 'CASO_ID' más reciente de la tabla de estados para un caso específico.

    Filtra por 'CANAL_ID' (que es el consecutivo) y los demás códigos del caso,
    buscando un estado 'PENDIENTE' para asegurar que se está trabajando sobre un
    caso que aún no ha sido procesado por este microservicio.

    Args:
        pool (AsyncConnection): La conexión asíncrona a la base de datos.
        consecutivo (int): El ID del caso en el sistema de origen.
        (otros): Códigos para identificar unívocamente el caso.

    Returns:
        Any: El 'CASO_ID' (entero) si se encuentra, de lo contrario None.
    """
    # 1. Se define la consulta para obtener el máximo CASO_ID.
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
    # 2. Se prepara la consulta y los parámetros.
    query = sqlalchemy.text(sql)
    params = {
        "consecutivo": consecutivo,
        "codigo_producto": codigo_producto,
        "codigo_subproducto": codigo_subproducto,
        "codigo_movimiento": codigo_movimiento,
        "codigo_modificacion": codigo_modificacion,
    }
    
    # 3. Se ejecuta la consulta.
    result = await pool.execute(query, params)
    # 4. Se obtiene el resultado como un valor único (escalar).
    caso_id_result = result.scalar_one_or_none()
    
    # 5. Se retorna el ID del caso o None.
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
    Inserta una lista de riesgos en la tabla 'ms_resultados'.

    Construye un 'RIESGO_MOTOR_ID' único para cada riesgo y lo inserta en la tabla.
    Utiliza `ON CONFLICT DO NOTHING` para evitar errores de clave duplicada si
    un riesgo ya fue insertado previamente para el mismo caso.

    Args:
        pool (AsyncConnection): La conexión a la base de datos.
        riesgos (List[Dict[str, Any]]): Lista de diccionarios, cada uno representando un riesgo.
        caso_id (Any): El ID del caso al que pertenecen los riesgos.
        (otros): Códigos usados para construir el RIESGO_MOTOR_ID.
    """
    # 1. Si no hay riesgos en la lista, no hay nada que hacer.
    if not riesgos:
        return

    # 2. Se inicializa una lista para almacenar los registros que se van a insertar.
    registros_a_insertar = []
    # 3. Se itera sobre cada riesgo para prepararlo para la inserción.
    for riesgo in riesgos:
        # 4. Se extraen los identificadores primarios del riesgo.
        tipo_documento = riesgo.get("TIPO_DOCUMENTO")
        numero_documento = riesgo.get("NUMERO_DOCUMENTO")
        placa = riesgo.get("PLACA")

        # 5. Se construye el 'RIESGO_MOTOR_ID' concatenando los identificadores.
        #    Este ID único identifica a un riesgo específico dentro de un caso.
        id_parts = [caso_id, codigo_producto, codigo_subproducto, codigo_movimiento, tipo_documento, numero_documento]
        if placa:
            id_parts.append(placa)
        # Se filtran partes nulas y se unen
        riesgo_motor_id = "-".join(map(str, filter(None, id_parts)))

        # 6. Se crea un diccionario con los datos a insertar, mapeando a las columnas de la tabla.
        registro = {
            "RIESGO_MOTOR_ID": riesgo_motor_id,
            "CASO_ID": caso_id,
            "TIPO_DOCUMENTO_ASEGURADO": tipo_documento,
            "NUMERO_DOCUMENTO_ASEGURADO": int(numero_documento) if numero_documento else None,
            "NOMBRE_ASEGURADO": riesgo.get("NOMBRE")
        }
        registros_a_insertar.append(registro)

    # 7. Si no se preparó ningún registro, se termina la función.
    if not registros_a_insertar:
        return

    # 8. Se define la consulta de inserción. `ON CONFLICT DO NOTHING` es clave para la idempotencia.
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
    
    # 9. Se ejecuta la inserción en un bloque transaccional.
    try:
        # 10. SQLAlchemy ejecuta la consulta para cada diccionario en la lista de registros.
        await pool.execute(query, registros_a_insertar)
        print(f"Registros insertados: {len(registros_a_insertar)}")
        # 11. Si todo es exitoso, se confirma la transacción.
        await pool.commit()
    except Exception as e:
        # 12. Si hay un error, se revierte la transacción.
        await pool.rollback()
        print(f"Error al insertar en la base de datos: {e}")
        # 13. Se relanza la excepción para que sea manejada por el nivel superior.
        raise

    # 14. Después de una inserción exitosa, se actualiza el estado del caso.
    await actualizar_estado_caso(pool, caso_id)


async def actualizar_estado_caso(pool: AsyncConnection, caso_id: Any):
    """
    Actualiza el estado de un caso a 'RIESGOS IDENTIFICADOS'.

    Args:
        pool (AsyncConnection): La conexión a la base de datos.
        caso_id (Any): El ID del caso a actualizar.
    """
    # 1. Se define la consulta de actualización.
    sql = """
        UPDATE "motor_suscripcion"."ms_estados_casos"
        SET "ESTADO" = 'RIESGOS IDENTIFICADOS'
        WHERE "CASO_ID" = :caso_id;
    """
    # 2. Se prepara la consulta y los parámetros.
    query = sqlalchemy.text(sql)
    params = {"caso_id": caso_id}

    # 3. Se ejecuta la actualización en un bloque transaccional.
    try:
        await pool.execute(query, params)
        await pool.commit()
    except Exception as e:
        await pool.rollback()
        print(f"Error al actualizar estado en 'ms_estados_casos': {e}")
        raise 