# Sistema de Cola de Impresión Distribuida (Kafka)

Este proyecto implementa un sistema avanzado de gestión de colas de impresión utilizando Java y Apache Kafka. Cumple con los requisitos de procesamiento paralelo, transformación de documentos y gestión multihilo de impresoras.

## 📋 Arquitectura del Sistema

El sistema sigue una arquitectura de Productores y Consumidores desacoplados para máxima eficiencia:

1.  **Recepción (`ProductorEmpleado`)**:
    *   Los empleados envían trabajos (JSON) con Título, Documento, Tipo (B/N o Color) y Remitente.
    *   Se envían al topic de entrada `print-jobs-incoming`.

2.  **Procesamiento Paralelo (Servicios)**:
    Dos servicios funcionan simultáneamente escuchando el topic de entrada:
    *   **Archivado (`ArchivadorDocumentos`)**:
        *   Guarda una copia exacta del JSON original en `docs_archivados/<sender>/` con timestamp.
        *   Garantiza la auditoría de todos los trabajos recibidos.
    *   **Transformación (`TransformadorDocumentos`)**:
        *   Deserializa el trabajo y divide el texto en páginas de máximo 400 caracteres.
        *   Enruta cada página a la cola de impresión correspondiente (`print-docs-bn` o `print-docs-color`) según el tipo.
        *   Utiliza el Título del documento como clave de particionado para garantizar el orden.

3.  **Impresión (`GestorImpresoras`)**:
    *   Gestiona un pool de hilos (`Impresora.java`) que simulan dispositivos físicos.
    *   **Impresoras B/N (3 unidades)**: Escuchan `print-docs-bn` (Topic con 3 particiones).
    *   **Impresoras Color (2 unidades)**: Escuchan `print-docs-color` (Topic con 2 particiones).
    *   La impresión se simula guardando ficheros en `docs_imprimidos/<tipo>/<nombre_impresora>/`.

---

## 🚀 Guía de Puesta en Marcha

### Requisitos
*   Java JDK 17 o superior.
*   Maven.
*   Apache Kafka.

### Paso 1: Inicialización del Entorno (Manual)

Siga estos pasos desde powershell, en la carpeta \bin\windows de kafka para configurar el servidor:

1.  **Formatear el Almacenamiento**:
    Generar un ID de cluster y formatear los directorios de logs.
    ```powershell
    .\kafka-storage.bat random-uuid
    .\kafka-storage.bat format --standalone -t "ID generado en el paso anterior" -c ..\..\config\server.properties
    ```

2.  **Iniciar el Servidor Kafka**:
    ```powershell
    .\kafka-server-start.bat ..\..\config\server.properties
    ```
    *(Mantener esta ventana abierta)*.

3.  **Crear Topics** (En una nueva terminal):
    Es crítico crear los topics con el número exacto de particiones para que funcionen las impresoras en paralelo.
    
    *   **Entrada de Trabajos** (1 Partición):
        ```powershell
        .\kafka-topics.bat --create --topic print-jobs-incoming --partitions 1 --bootstrap-server localhost:9092
        ```
    *   **Cola B/N** (3 Particiones = 3 Impresoras):
        ```powershell
        .\kafka-topics.bat --create --topic print-docs-bn --partitions 3 --bootstrap-server localhost:9092
        ```
    *   **Cola Color** (2 Particiones = 2 Impresoras):
        ```powershell
        .\kafka-topics.bat --create --topic print-docs-color --partitions 2 --bootstrap-server localhost:9092
        ```

### Paso 2: Ejecución de Componentes
Ejecutar las siguientes clases Java (desde Eclipse o Terminal) en este orden:

1.  **`ArchivadorDocumentos`**: Inicia el servicio de backup (`docs_archivados`).
2.  **`TransformadorDocumentos`**: Inicia el servicio de routing y paginación.
3.  **`GestorImpresoras`**: Arranca las 5 impresoras virtuales (`docs_imprimidos`).
4.  **`ProductorEmpleado`**: Envía una carga de trabajo de prueba.

---

## 🛠️ Información para el Mantenedor

### Estructura de Topics Kafka
| Topic | Particiones | Uso |
|-------|-------------|-----|
| `print-jobs-incoming` | 1 | Entrada de trabajos nuevos (JSON). |
| `print-docs-bn` | 3 | Cola de documentos paginados B/N. 1 Partición por Impresora BN. |
| `print-docs-color` | 2 | Cola de documentos paginados Color. 1 Partición por Impresora Color. |

### Limpieza y Reinicio
Si se necesita reiniciar el entorno completamente (borrar colas y logs corruptos):

1.  Detener el servidor Kafka (Ctrl+C).
2.  Borrar la carpeta de logs temporales:
    ```powershell
    Remove-Item -Recurse -Force C:\tmp\kraft-combined-logs
    ```
3.  Borrar las carpetas de salida en el proyecto de Maven
4.  Repetir el **Paso 1** (Formatear y Arrancar).
