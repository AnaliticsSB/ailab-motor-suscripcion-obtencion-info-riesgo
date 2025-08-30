# -*- coding: utf-8 -*-
"""
Este módulo define los modelos de datos Pydantic para la API de obtención de riesgos.

- IdentificacionRiesgosRequest: Define la estructura del cuerpo (body) de la solicitud
  POST entrante, asegurando que los parámetros de filtrado necesarios estén presentes.
- InfoRiesgosResponse: Define la estructura de la respuesta JSON que la API
  devuelve, garantizando un formato de salida consistente.
"""
# --- Importaciones ---
# Se importa la clase BaseModel, que es la base para crear todos los modelos Pydantic.
from pydantic import BaseModel
# Se importan tipos de datos para definir campos complejos y opcionales.
from typing import List, Dict, Any, Optional

# --- Definición de Modelos ---

class IdentificacionRiesgosRequest(BaseModel):
    """
    Modelo para el cuerpo (body) de la solicitud del endpoint de obtención de riesgos.
    
    Valida que la solicitud entrante contenga todos los campos necesarios para
    consultar la configuración en la base de datos y para formatear las
    llamadas a las APIs externas.
    """
    # --- Atributos del Modelo ---
    
    # Código numérico que identifica el producto principal del seguro.
    codigo_producto: int
    # Código numérico que identifica una especialización del producto.
    codigo_subproducto: int
    # Código de texto que describe el tipo de operación (ej. 'Emisión', 'Renovación').
    codigo_movimiento: str
    # Código de texto para sub-operaciones o modificaciones (puede estar vacío).
    codigo_modificacion: str
    # Identificador único del caso en el sistema de origen (ej. Filenet).
    # Se usa para encontrar el 'CASO_ID' interno y como placeholder en llamadas a APIs.
    consecutivo: int


class InfoRiesgosResponse(BaseModel):
    """
    Modelo para la respuesta del endpoint, asegurando un formato de salida consistente.
    """
    # --- Atributos del Modelo ---
    
    # Una lista de diccionarios, donde cada diccionario representa un riesgo encontrado
    # con su información correspondiente.
    riesgos: List[Dict[str, Any]]
    
    # Un campo de texto opcional para devolver mensajes informativos al cliente,
    # como "No se encontró ningún riesgo con los parámetros proporcionados.".
    # El valor por defecto es None si no se proporciona.
    mensaje: Optional[str] = None