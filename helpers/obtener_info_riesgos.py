
# -*- coding: utf-8 -*-

"""
Módulo para la orquestación de la obtención de información de riesgos.

Contiene la lógica para procesar riesgos de tipo 'INDIVIDUAL' y 'COLECTIVO',
invocando APIs externas de forma paralela o secuencial según sea necesario,
y opcionalmente utilizando Vertex AI (Gemini) para el procesamiento de
documentos.
"""

# ==============================================================================
# 1. IMPORTACIONES
# ==============================================================================
import os
import json
import asyncio
import httpx
import pandas as pd
import base64
import io
import ast
import warnings
from typing import Dict, Any, List, Optional
import vertexai
from vertexai.generative_models import GenerativeModel, Part
from sqlalchemy.ext.asyncio import AsyncConnection
from utils.crud_postgres import obtener_caso_id_por_consecutivo, insertar_resultados_riesgos

# Suprimir warnings específicos de openpyxl
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=UserWarning, module="vertexai")


# ==============================================================================
# 2. FUNCIONES AUXILIARES
# ==============================================================================

def _parse_mapping(value: Any) -> Dict[str, Any]:
    """
    Convierte de forma segura un string que representa un diccionario
    (ej. '{"key": "value"}') en un diccionario de Python.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return {}
    return {}

def _format_values(obj: Any, placeholders: Dict[str, Any]) -> Any:
    """
    Recorre recursivamente un objeto (diccionario o lista) y reemplaza
    placeholders en los strings (ej. '{consecutivo}') con sus valores reales.
    """
    if isinstance(obj, dict):
        return {k: _format_values(v, placeholders) for k, v in obj.items()}
    if isinstance(obj, list):
        return [_format_values(elem, placeholders) for elem in obj]
    if isinstance(obj, str):
        try:
            return obj.format(**placeholders)
        except KeyError:
            return obj
    return obj

def _safe_eval_extraction(expression: str, result: Any) -> Any:
    """
    Evalúa de forma segura una expresión de Python para extraer datos de un
    resultado (generalmente un JSON/diccionario).
    """
    if not isinstance(expression, str) or not expression.strip():
        return None
    try:
        return eval(expression, {"result": result})
    except Exception:
        return None

def _excel_bytes_from_result(result_json: Dict[str, Any]) -> Optional[bytes]:
    """
    Busca un adjunto de Excel en base64 en la respuesta de una API,
    lo decodifica y lo devuelve como bytes.
    """
    adjunto_b64 = result_json.get("adjunto")
    if isinstance(adjunto_b64, str) and adjunto_b64.strip():
        try:
            return base64.b64decode(adjunto_b64)
        except (base64.binascii.Error, ValueError) as e:
            return None
    return None

async def _procesar_con_gemini(
    prompt: str,
    excel_bytes: bytes
) -> Optional[List[Dict]]:
    """
    Invoca al modelo Gemini, procesa la respuesta y extrae la lista de diccionarios.
    En producción, asume que la autenticación se maneja a través de ADC.
    """
    try:
        # En producción, vertexai.init() sin credenciales usará ADC.
        vertexai.init(project='sb-xops-stage', location="us-central1") 
        
        model = GenerativeModel("gemini-2.5-pro")

        dict_de_dfs = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=None)
        partes_csv = [f"--- INICIO DE HOJA: {nombre} ---\n{df.to_csv(index=False)}\n--- FIN DE HOJA: {nombre} ---\n\n" for nombre, df in dict_de_dfs.items()]
        datos_csv_completos = "".join(partes_csv)
        csv_part = Part.from_data(mime_type="text/csv", data=datos_csv_completos.encode('utf-8'))

        response = model.generate_content([csv_part, prompt])
        
        raw_text = response.text
        start_index = raw_text.find('{')
        end_index = raw_text.rfind('}')
        
        json_string = None
        if start_index != -1 and end_index != -1:
            json_string = raw_text[start_index : end_index + 1]
        
        if not json_string:
            # En un entorno de producción, sería ideal loguear esto.
            return None

        try:
            parsed_data = json.loads(json_string)
            if isinstance(parsed_data, dict) and 'riesgos' in parsed_data:
                if isinstance(parsed_data['riesgos'], list):
                    return parsed_data['riesgos']
        except json.JSONDecodeError as e:
            # Ideal para loguear el `json_string` que falló.
            return None
        
        return None
    except Exception as e:
        print(f"--- [ERROR] Excepción en _procesar_con_gemini: {e}")
        print(f"--- [ERROR] Tipo de excepción: {type(e).__name__}")
        import traceback
        print(f"--- [ERROR] Traceback completo:")
        traceback.print_exc()
        return None

# ==============================================================================
# 3. LÓGICA PRINCIPAL DE ORQUESTACIÓN
# ==============================================================================

async def _request_por_fuente(
    session: httpx.AsyncClient,
    fuente_info: pd.Series,
    placeholders: Dict[str, Any]
) -> Any:
    """
    Realiza una única solicitud HTTP asíncrona.
    """
    url_template = fuente_info.get('URL', '')
    metodo = fuente_info.get('METODO', 'GET').upper()
    header_str = fuente_info.get('HEADER', '{}')
    payload_str = fuente_info.get('PAYLOAD', '{}')
    params_str = fuente_info.get('PARAMS', '{}')

    url = _format_values(url_template, placeholders)
    headers = _format_values(_parse_mapping(header_str), placeholders)
    payload = _format_values(_parse_mapping(payload_str), placeholders)
    params = _format_values(_parse_mapping(params_str), placeholders)

    try:
        response = await session.request(
            method=metodo,
            url=url,
            headers=headers,
            json=payload if metodo != 'GET' and payload else None,
            params=params if metodo == 'GET' and params else None,
            timeout=90.0
        )
        response.raise_for_status()
        return response.json()
    except Exception as e:
        # Ideal para loguear el error.
        return None

async def obtener_info_riesgo_individual(
    df_info_riesgos: pd.DataFrame, 
    consecutivo: int,
    pool: AsyncConnection,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str,
    codigo_modificacion: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Orquesta la obtención de información para un riesgo INDIVIDUAL en paralelo.
    """
    placeholders = {'consecutivo': consecutivo}
    variables_dict = {var: None for var in df_info_riesgos['VARIABLE'].unique()}
    
    fuentes_agrupadas = df_info_riesgos.groupby('FUENTE')
    
    async def _procesar_fuente_individual(session, nombre_fuente, df_config):
        config_api = df_config.iloc[0]
        resultado_api = await _request_por_fuente(session, config_api, placeholders)

        if resultado_api and not isinstance(resultado_api, Exception):
            for _, fila_variable in df_config.iterrows():
                variable = fila_variable.get('VARIABLE')
                extraccion = fila_variable.get('EXTRACCION')
                if variable and extraccion:
                    valor = _safe_eval_extraction(extraccion, resultado_api)
                    variables_dict[variable] = valor

    async with httpx.AsyncClient() as session:
        tasks = [
            _procesar_fuente_individual(session, nombre_fuente, df_config)
            for nombre_fuente, df_config in fuentes_agrupadas
        ]
        await asyncio.gather(*tasks, return_exceptions=True)

    # Convertir el diccionario de variables en una lista, que es el formato esperado por la función de inserción.
    riesgos_a_insertar = [variables_dict]

    # Obtener el CASO_ID
    caso_id = await obtener_caso_id_por_consecutivo(
        pool, 
        consecutivo,
        codigo_producto,
        codigo_subproducto,
        codigo_movimiento,
        codigo_modificacion
    )

    # Insertar los resultados en la base de datos
    if caso_id:
        await insertar_resultados_riesgos(pool, riesgos_a_insertar, caso_id, codigo_producto, codigo_subproducto, codigo_movimiento)
    else:
        # Manejar el caso en que no se encuentra el caso_id.
        # Por ejemplo, se podría registrar un error o lanzar una excepción.
        print(f"Advertencia: No se encontró CASO_ID para el consecutivo {consecutivo}. No se insertarán datos.")

    return {"riesgos": riesgos_a_insertar}

async def obtener_info_riesgos_colectiva(
    df_info_riesgos: pd.DataFrame, 
    consecutivo: int,
    pool: AsyncConnection,
    codigo_producto: int,
    codigo_subproducto: int,
    codigo_movimiento: str,
    codigo_modificacion: str
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Orquesta la obtención de información para un riesgo COLECTIVO de forma SECUENCIAL.
    """
    placeholders = {'consecutivo': consecutivo}
    df_info_riesgos['FUENTE'] = df_info_riesgos['FUENTE'].astype(str).str.strip()
    fuentes_en_orden = ["FILENET_LIST_DOCUMENTOS", "FILENET_GET_DOCUMENTO"]

    async with httpx.AsyncClient() as client:
        for nombre_fuente in fuentes_en_orden:
            df_config_fuente = df_info_riesgos[df_info_riesgos['FUENTE'] == nombre_fuente]
            
            if df_config_fuente.empty:
                continue

            config_api = df_config_fuente.iloc[0]
            resultado_api = await _request_por_fuente(client, config_api, placeholders)
            
            if not resultado_api or isinstance(resultado_api, Exception):
                continue

            if nombre_fuente == "FILENET_LIST_DOCUMENTOS":
                extraccion = config_api.get('EXTRACCION')
                variable = config_api.get('VARIABLE')
                if variable and extraccion:
                    valor = _safe_eval_extraction(extraccion, resultado_api)
                    if variable == 'ID_DOC_LISTA_RIESGOS':
                        placeholders['id_doc_lista_riesgos'] = valor

            elif nombre_fuente == "FILENET_GET_DOCUMENTO":
                prompt = config_api.get('PROMPT')
                # El flujo de Gemini se activa si hay un prompt.
                if prompt:
                    excel_bytes = _excel_bytes_from_result(resultado_api)
                    if excel_bytes:
                        lista_de_riesgos = await _procesar_con_gemini(
                            prompt=prompt,
                            excel_bytes=excel_bytes
                        )
                        if isinstance(lista_de_riesgos, list):
                            # Obtener el CASO_ID
                            caso_id = await obtener_caso_id_por_consecutivo(
                                pool, 
                                consecutivo,
                                codigo_producto,
                                codigo_subproducto,
                                codigo_movimiento,
                                codigo_modificacion
                            )

                            # Insertar los resultados en la base de datos
                            if caso_id:
                                await insertar_resultados_riesgos(pool, lista_de_riesgos, caso_id, codigo_producto, codigo_subproducto, codigo_movimiento)
                            
                            return {"riesgos": lista_de_riesgos}
                    else:
                        print("DEBUG: No se pudo extraer el archivo Excel de la respuesta de FILENET_GET_DOCUMENTO.")
    
    return {"riesgos": []} 
