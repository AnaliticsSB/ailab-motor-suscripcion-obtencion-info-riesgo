"""
Modelos para la API de obtención de información de riesgos.
- IdentificacionRiesgosRequest: body de entrada con filtros para Postgres
- InfoRiesgosResponse: respuesta estándar con lista de resultados
"""
from pydantic import BaseModel
from typing import List, Dict, Any, Optional


class IdentificacionRiesgosRequest(BaseModel):
    """
    Body de entrada del endpoint de obtención de riesgos.

    Parámetros:
    - codigo_producto (int): Código del producto.
    - codigo_subproducto (int): Código del subproducto.
    - codigo_movimiento (int): Código del movimiento.
    - codigo_modificacion (int): Código de la modificación (puede compararse con COALESCE en SQL).
    - consecutivo (int): Identificador usado para completar placeholders en URLs (ej. {consecutivo}).

    Retorna:
    - Instancia validada Pydantic utilizada para filtrar en Postgres y formatear URLs.
    """
    codigo_producto: int
    codigo_subproducto: int
    codigo_movimiento: str
    codigo_modificacion: str
    consecutivo: int


class InfoRiesgosResponse(BaseModel):
    """
    Respuesta del endpoint con la lista de resultados.

    Parámetros:
    - riesgos (List[Dict[str, Any]]): Lista de registros devueltos por el servicio.
    - mensaje (Optional[str]): Mensaje informativo opcional.

    Retorna:
    - Objeto de respuesta validado por Pydantic.
    """
    riesgos: List[Dict[str, Any]]
    mensaje: Optional[str] = None