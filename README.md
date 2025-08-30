# AILab - Motor Suscripción - Obtención de Información de Riesgos

Servicio FastAPI cuya responsabilidad es obtener la información detallada de uno o varios riesgos asociados a un caso, basándose en una configuración dinámica almacenada en la base de datos.

## Descripción General

Este servicio es un componente clave del motor de suscripción. Expone un único endpoint (`/api/obtener_info_riesgos`) que, a partir de identificadores de un caso, consulta una tabla de configuración para determinar cómo y de dónde debe extraer los datos de los riesgos.

El motor soporta dos flujos de procesamiento distintos:

1.  **Riesgo Individual:** Diseñado para productos que se procesan de uno en uno (ej. un solo vehículo, una sola persona). El servicio identifica todas las fuentes de datos configuradas (APIs externas) y las consulta de forma **paralela** para maximizar la eficiencia. Luego, extrae las variables específicas de cada respuesta y las consolida en un único objeto de riesgo.

2.  **Riesgo Colectivo:** Orientado a productos que involucran un conjunto de riesgos, típicamente definidos en un listado (ej. un archivo Excel con múltiples asegurados). En este modo, el servicio ejecuta las llamadas a las APIs de forma **secuencial**, ya que la salida de una llamada (como la obtención de un ID de documento) puede ser la entrada para la siguiente. Este flujo tiene la capacidad avanzada de:
    -   Extraer un archivo (ej. `.xlsx`) que viene adjunto y codificado en base64 en la respuesta de una API.
    -   Enviar este archivo, junto con un `prompt` definido en la configuración, a un modelo de IA generativa (**Vertex AI Gemini**).
    -   Interpretar la respuesta del modelo, que se espera sea una lista de riesgos en formato JSON, y retornarla como el resultado final.

Independientemente del flujo, el servicio registra cada riesgo obtenido en la tabla `motor_suscripcion.ms_resultados` para su posterior uso en el motor de reglas y otros procesos.

## Estructura del Proyecto

-   `main.py`: Aplicación FastAPI, endpoint principal y lógica de enrutamiento (individual vs. colectivo).
-   `models/models.py`: Modelos Pydantic para la validación de las solicitudes y respuestas.
-   `utils/connect_sql.py`: Lógica para la conexión asíncrona a PostgreSQL en Google Cloud.
-   `utils/crud_postgres.py`: Funciones para leer la configuración e insertar los resultados en la base de datos.
-   `helpers/obtener_info_riesgos.py`: Módulo central que contiene la lógica de orquestación para los flujos individual y colectivo, incluyendo las llamadas a APIs y la interacción con Vertex AI.

## Endpoint Principal

### `POST /api/obtener_info_riesgos`

Inicia el proceso de identificación y obtención de datos de riesgo.

**Ejemplo de Request Body:**
```json
{
  "codigo_producto": 250,
  "codigo_subproducto": 367,
  "codigo_movimiento": "MN01",
  "codigo_modificacion": "",
  "consecutivo": 203585
}
```

**Ejemplo de Response Body (Flujo Individual):**
```json
{
  "riesgos": [
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": 123456789,
      "NOMBRE": "Juan Perez",
      "PLACA": "FKN098"
    }
  ],
  "mensaje": null
}
```

**Ejemplo de Response Body (Flujo Colectivo - Respuesta de Gemini):**
```json
{
  "riesgos": [
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": "987654321",
      "NOMBRE": "Ana Lopez"
    },
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": "456789123",
      "NOMBRE": "Carlos Ramirez"
    }
  ],
  "mensaje": null
}
```
*(Nota: Si no se encuentran riesgos, la lista `riesgos` estará vacía y el campo `mensaje` contendrá una nota informativa).*

## Ejecución Local

1.  **Crear y activar un entorno virtual:**
    ```bash
    python -m venv .venv
    # Windows
    .venv\Scripts\activate
    # Linux/macOS
    # source .venv/bin/activate
    ```
2.  **Instalar dependencias:**
    ```bash
    pip install -r requirements.txt
    ```
3.  **Ejecutar la API:**
    ```bash
    uvicorn main:app --reload --host 0.0.0.0 --port 8080
    ```
4.  **Acceder a la documentación interactiva** en `http://localhost:8080/docs`.

## Despliegue

El proyecto incluye `Dockerfile` y `cloudbuild.yaml` para facilitar la containerización y el despliegue en un entorno como Google Cloud Run. Es crucial asegurarse de que el entorno de despliegue tenga los permisos necesarios para acceder a **Google Cloud Secret Manager** (para las credenciales de la BD) y a **Vertex AI**.
