# Servicio de Obtención de Información de Riesgos

Este servicio forma parte del Motor de Suscripción y su responsabilidad principal es obtener la información detallada de uno o varios riesgos asociados a un caso, basándose en una configuración dinámica almacenada en la base de datos.

## Descripción General

El servicio expone un único endpoint (`/api/obtener_info_riesgos`) que recibe un conjunto de códigos para identificar una póliza y un consecutivo de caso. Con esta información, consulta una tabla de configuración para determinar cómo y de dónde debe extraer los datos del riesgo.

El motor soporta dos flujos principales:

1.  **Riesgo Individual:** Para productos que se procesan de uno en uno. El servicio identifica todas las fuentes de datos (APIs externas) y las consulta de forma **paralela** para maximizar la eficiencia. Luego, extrae las variables configuradas de cada respuesta y las consolida en un único objeto.
2.  **Riesgo Colectivo:** Para productos que involucran un conjunto de riesgos (por ejemplo, a través de un listado en un archivo Excel). El servicio ejecuta las llamadas a las APIs de forma **secuencial**, ya que la salida de una puede ser la entrada de la siguiente. Este flujo tiene la capacidad de:
    - Extraer un archivo (ej. Excel) adjunto en una respuesta de API.
    - Enviar este archivo, junto con un `prompt` de la configuración, a un modelo de IA generativa (Vertex AI Gemini).
    - Interpretar la respuesta del modelo (que se espera sea una lista de riesgos en formato JSON) y retornarla como el resultado final.

Finalmente, el servicio registra cada riesgo obtenido en la tabla de resultados (`motor_suscripcion.ms_resultados`) para su posterior consulta y análisis.

## Endpoint

- **URL:** `/api/obtener_info_riesgos`
- **Método:** `POST`

### Cuerpo de la Solicitud (Request Body)

```json
{
  "codigo_producto": 250,
  "codigo_subproducto": 367,
  "codigo_movimiento": "MN01",
  "codigo_modificacion": "",
  "consecutivo": 203585
}
```

### Respuesta Exitosa (Response Body)

La respuesta siempre es un objeto JSON con una única clave, `riesgos`, que contiene una lista de los riesgos procesados.

**Ejemplo para un riesgo individual:**
```json
{
  "riesgos": [
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": 123456789,
      "NOMBRE": "Juan Perez",
      "PLACA": "FKN098"
    }
  ]
}
```

**Ejemplo para un riesgo colectivo (respuesta de Gemini):**
```json
{
  "riesgos": [
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": 987654321,
      "NOMBRE": "Ana Lopez"
    },
    {
      "TIPO_DOCUMENTO": "CC",
      "NUMERO_DOCUMENTO": 456789123,
      "NOMBRE": "Carlos Ramirez"
    }
  ]
}
```

## Configuración del Entorno

Para la correcta ejecución del servicio, se deben configurar las siguientes variables de entorno si se desea sobreescribir los valores por defecto:

- `GCP_PROJECT_ID`: ID del proyecto de Google Cloud donde reside el servicio de Vertex AI.
- `GCP_REGION`: Región donde se desplegará el servicio y se realizarán las llamadas a Vertex AI.
- `DB_USER`: Usuario para la conexión a la base de datos PostgreSQL.
- `DB_PASS`: Contraseña para el usuario de la base de datos.
- `DB_NAME`: Nombre de la base de datos.
- `DB_HOST`: Host o IP de la instancia de la base de datos.
- `DB_PORT`: Puerto de la base de datos.
