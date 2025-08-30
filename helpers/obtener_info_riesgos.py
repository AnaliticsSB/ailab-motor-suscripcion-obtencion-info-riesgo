
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
# --- Módulos estándar de Python ---
import os
import json
import asyncio
import base64
import io
import ast
import warnings
from typing import Dict, Any, List, Optional

# --- Módulos de terceros ---
import httpx  # Para realizar llamadas a APIs de forma asíncrona.
import pandas as pd  # Para manipulación de datos, especialmente desde Excel.
import vertexai # SDK de Google para Vertex AI.
from vertexai.generative_models import GenerativeModel, Part # Componentes específicos para modelos generativos.
from sqlalchemy.ext.asyncio import AsyncConnection # Tipado para la conexión a la BD.

# --- Módulos locales de la aplicación ---
from utils.crud_postgres import obtener_caso_id_por_consecutivo, insertar_resultados_riesgos

# --- Configuración de Warnings ---
# Se suprimen warnings que pueden aparecer al leer archivos Excel o al inicializar Vertex AI.
warnings.filterwarnings("ignore", category=UserWarning, module="openpyxl")
warnings.filterwarnings("ignore", category=UserWarning, module="vertexai")


# ==============================================================================
# 2. FUNCIONES AUXILIARES
# ==============================================================================

def _parse_mapping(value: Any) -> Dict[str, Any]:
    """
    Convierte de forma segura un string que representa un diccionario en un diccionario real.
    Si el valor ya es un diccionario, lo devuelve directamente.
    Si la conversión falla, devuelve un diccionario vacío.
    """
    if isinstance(value, dict):
        return value
    if isinstance(value, str) and value.strip():
        try:
            # ast.literal_eval es más seguro que eval() porque solo procesa literales de Python.
            return ast.literal_eval(value)
        except (ValueError, SyntaxError):
            return {}
    return {}

def _format_values(obj: Any, placeholders: Dict[str, Any]) -> Any:
    """
    Recorre recursivamente una estructura (dict o list) y reemplaza placeholders en los strings.
    Por ejemplo, reemplaza '{consecutivo}' con el valor real del consecutivo.
    """
    if isinstance(obj, dict):
        # Si es un diccionario, aplica el formateo a cada valor.
        return {k: _format_values(v, placeholders) for k, v in obj.items()}
    if isinstance(obj, list):
        # Si es una lista, aplica el formateo a cada elemento.
        return [_format_values(elem, placeholders) for elem in obj]
    if isinstance(obj, str):
        try:
            # Intenta formatear la cadena con los placeholders.
            return obj.format(**placeholders)
        except KeyError:
            # Si una clave no existe en placeholders, devuelve la cadena original.
            return obj
    # Para cualquier otro tipo de dato, lo devuelve sin cambios.
    return obj

def _safe_eval_extraction(expression: str, result: Any) -> Any:
    """
    Evalúa de forma segura una expresión de Python para extraer datos de un resultado.
    Limita el entorno de `eval` a una única variable 'result' para seguridad.
    """
    # Si no hay una expresión válida, no hay nada que evaluar.
    if not isinstance(expression, str) or not expression.strip():
        return None
    try:
        # `eval` ejecuta la expresión. El segundo argumento define las variables globales disponibles.
        return eval(expression, {"result": result})
    except Exception:
        # Si la expresión falla (ej. la clave no existe), devuelve None.
        return None

def _excel_bytes_from_result(result_json: Dict[str, Any]) -> Optional[bytes]:
    """
    Busca un adjunto en base64 en la respuesta de una API, lo decodifica y devuelve como bytes.
    Esto es útil para manejar archivos Excel devueltos por servicios como Filenet.
    """
    # Busca la clave 'adjunto' que debería contener la cadena en base64.
    adjunto_b64 = result_json.get("adjunto")
    if isinstance(adjunto_b64, str) and adjunto_b64.strip():
        try:
            # Decodifica la cadena base64 a bytes.
            return base64.b64decode(adjunto_b64)
        except (base64.binascii.Error, ValueError):
            # Si la cadena no es un base64 válido, retorna None.
            return None
    return None

async def _procesar_con_gemini(
    prompt: str,
    excel_bytes: bytes
) -> Optional[List[Dict]]:
    """
    Invoca al modelo Gemini de Vertex AI para procesar un archivo Excel.
    Convierte el Excel a CSV, lo envía al modelo junto con un prompt y extrae
    la lista de riesgos del resultado.
    """
    try:
        # Inicializa el SDK de Vertex AI. En un entorno de producción en GCP,
        # la autenticación se maneja automáticamente (Application Default Credentials).
        vertexai.init(project='sb-xops-stage', location="us-central1") 
        
        # Carga el modelo generativo especificado.
        model = GenerativeModel("gemini-1.5-pro-001")

        # Lee todas las hojas del archivo Excel en un diccionario de DataFrames.
        dict_de_dfs = pd.read_excel(io.BytesIO(excel_bytes), sheet_name=None)
        # Convierte cada hoja a formato CSV y las concatena en un solo string.
        partes_csv = [f"--- INICIO DE HOJA: {nombre} ---\n{df.to_csv(index=False)}\n--- FIN DE HOJA: {nombre} ---\n\n" for nombre, df in dict_de_dfs.items()]
        datos_csv_completos = "".join(partes_csv)
        # Crea un objeto 'Part' para enviar los datos CSV al modelo.
        csv_part = Part.from_data(mime_type="text/csv", data=datos_csv_completos.encode('utf-8'))

        # Envía el CSV y el prompt al modelo para generar contenido.
        response = model.generate_content([csv_part, prompt])
        
        # --- Extracción Robusta de JSON de la Respuesta del Modelo ---
        raw_text = response.text
        # Encuentra el primer '{' y el último '}' para aislar el objeto JSON principal.
        start_index = raw_text.find('{')
        end_index = raw_text.rfind('}')
        
        json_string = None
        if start_index != -1 and end_index != -1:
            json_string = raw_text[start_index : end_index + 1]
        
        if not json_string:
            return None # Si no se puede aislar un JSON, no se puede continuar.

        try:
            # Intenta parsear la cadena extraída como JSON.
            parsed_data = json.loads(json_string)
            # Verifica que la estructura sea la esperada (un dict con una lista de 'riesgos').
            if isinstance(parsed_data, dict) and 'riesgos' in parsed_data:
                if isinstance(parsed_data['riesgos'], list):
                    return parsed_data['riesgos'] # Devuelve la lista de riesgos.
        except json.JSONDecodeError:
            # Si el string no es un JSON válido, retorna None.
            return None
        
        return None
    except Exception as e:
        # Captura cualquier otra excepción durante el proceso y la registra.
        print(f"--- [ERROR] Excepción en _procesar_con_gemini: {type(e).__name__} - {e}")
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
    Realiza una única solicitud HTTP asíncrona a una fuente de datos (API).
    """
    # 1. Extrae los detalles de la API desde la configuración (un renglón del DataFrame).
    url_template = fuente_info.get('URL', '')
    metodo = fuente_info.get('METODO', 'GET').upper()
    header_str = fuente_info.get('HEADER', '{}')
    payload_str = fuente_info.get('PAYLOAD', '{}')
    params_str = fuente_info.get('PARAMS', '{}')

    # 2. Formatea la URL, headers, payload y params reemplazando los placeholders.
    url = _format_values(url_template, placeholders)
    headers = _format_values(_parse_mapping(header_str), placeholders)
    payload = _format_values(_parse_mapping(payload_str), placeholders)
    params = _format_values(_parse_mapping(params_str), placeholders)

    try:
        # 3. Realiza la solicitud HTTP usando el cliente httpx.
        response = await session.request(
            method=metodo,
            url=url,
            headers=headers,
            json=payload if metodo != 'GET' and payload else None,
            params=params if metodo == 'GET' and params else None,
            timeout=90.0
        )
        # 4. Lanza una excepción si la respuesta es un código de error (4xx o 5xx).
        response.raise_for_status()
        # 5. Devuelve la respuesta parseada como JSON.
        return response.json()
    except Exception as e:
        # Si la solicitud falla, lo notifica y devuelve None.
        print(f"--- [ERROR] Falla en request a {url}: {e}")
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
    Orquesta la obtención de información para un riesgo de tipo INDIVIDUAL.
    Las llamadas a las diferentes fuentes de datos se realizan en paralelo.
    """
    # 1. Prepara los placeholders y un diccionario para almacenar los resultados.
    placeholders = {'consecutivo': consecutivo}
    variables_dict = {var: None for var in df_info_riesgos['VARIABLE'].unique()}
    
    # 2. Agrupa la configuración por fuente para procesar cada una.
    fuentes_agrupadas = df_info_riesgos.groupby('FUENTE')
    
    # --- Función anidada para procesar una fuente ---
    async def _procesar_fuente_individual(session, nombre_fuente, df_config):
        # Toma la primera fila de configuración para la API.
        config_api = df_config.iloc[0]
        # Realiza la llamada a la API.
        resultado_api = await _request_por_fuente(session, config_api, placeholders)

        # Si la llamada fue exitosa...
        if resultado_api:
            # ...itera sobre las variables que esta fuente debe proveer.
            for _, fila_variable in df_config.iterrows():
                variable = fila_variable.get('VARIABLE')
                extraccion = fila_variable.get('EXTRACCION')
                # Si hay una expresión de extracción, la evalúa y guarda el valor.
                if variable and extraccion:
                    valor = _safe_eval_extraction(extraccion, resultado_api)
                    variables_dict[variable] = valor

    # 3. Crea un cliente HTTP asíncrono.
    async with httpx.AsyncClient() as session:
        # 4. Crea una lista de tareas (corrutinas), una para cada fuente.
        tasks = [
            _procesar_fuente_individual(session, nombre_fuente, df_config)
            for nombre_fuente, df_config in fuentes_agrupadas
        ]
        # 5. Ejecuta todas las tareas en paralelo con asyncio.gather.
        await asyncio.gather(*tasks, return_exceptions=True)

    # 6. Convierte el diccionario de variables en una lista de riesgos (en este caso, solo uno).
    riesgos_a_insertar = [variables_dict]

    # 7. Obtiene el CASO_ID de la base de datos.
    caso_id = await obtener_caso_id_por_consecutivo(
        pool, consecutivo, codigo_producto, codigo_subproducto,
        codigo_movimiento, codigo_modificacion
    )

    # 8. Si se encontró un CASO_ID, inserta los resultados en la base de datos.
    if caso_id:
        await insertar_resultados_riesgos(
            pool, riesgos_a_insertar, caso_id, codigo_producto, 
            codigo_subproducto, codigo_movimiento
        )
    else:
        print(f"Advertencia: No se encontró CASO_ID para el consecutivo {consecutivo}.")

    # 9. Devuelve el resultado en el formato de respuesta esperado.
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
    Este flujo suele implicar:
    1. Listar documentos.
    2. Obtener un documento específico (ej. un Excel).
    3. Procesar ese documento (ej. con Gemini) para extraer la lista de riesgos.
    """
    # 1. Prepara los placeholders y define el orden de las llamadas.
    placeholders = {'consecutivo': consecutivo}
    df_info_riesgos['FUENTE'] = df_info_riesgos['FUENTE'].astype(str).str.strip()
    fuentes_en_orden = ["FILENET_LIST_DOCUMENTOS", "FILENET_GET_DOCUMENTO"]

    # 2. Crea un cliente HTTP asíncrono.
    async with httpx.AsyncClient() as client:
        # 3. Itera sobre las fuentes en el orden definido.
        for nombre_fuente in fuentes_en_orden:
            # Obtiene la configuración para la fuente actual.
            df_config_fuente = df_info_riesgos[df_info_riesgos['FUENTE'] == nombre_fuente]
            if df_config_fuente.empty: continue

            config_api = df_config_fuente.iloc[0]
            # Realiza la llamada a la API.
            resultado_api = await _request_por_fuente(client, config_api, placeholders)
            
            if not resultado_api: continue

            # --- Lógica específica para cada paso del flujo secuencial ---
            if nombre_fuente == "FILENET_LIST_DOCUMENTOS":
                # Extrae el ID del documento de la lista para usarlo en el siguiente paso.
                extraccion = config_api.get('EXTRACCION')
                variable = config_api.get('VARIABLE')
                if variable == 'ID_DOC_LISTA_RIESGOS' and extraccion:
                    valor = _safe_eval_extraction(extraccion, resultado_api)
                    placeholders['id_doc_lista_riesgos'] = valor

            elif nombre_fuente == "FILENET_GET_DOCUMENTO":
                prompt = config_api.get('PROMPT')
                # Si hay un prompt, significa que el documento debe ser procesado por Gemini.
                if prompt:
                    # Extrae el archivo Excel en bytes de la respuesta de la API.
                    excel_bytes = _excel_bytes_from_result(resultado_api)
                    if excel_bytes:
                        # Llama a Gemini para procesar el Excel y obtener la lista de riesgos.
                        lista_de_riesgos = await _procesar_con_gemini(prompt=prompt, excel_bytes=excel_bytes)
                        if isinstance(lista_de_riesgos, list):
                            # Si Gemini devuelve una lista, la obtiene, la inserta en la BD y la retorna.
                            caso_id = await obtener_caso_id_por_consecutivo(
                                pool, consecutivo, codigo_producto, codigo_subproducto,
                                codigo_movimiento, codigo_modificacion
                            )
                            if caso_id:
                                await insertar_resultados_riesgos(
                                    pool, lista_de_riesgos, caso_id, codigo_producto,
                                    codigo_subproducto, codigo_movimiento
                                )
                            return {"riesgos": lista_de_riesgos}
    
    # 4. Si el flujo termina sin haber retornado una lista de riesgos, devuelve una lista vacía.
    return {"riesgos": []} 
