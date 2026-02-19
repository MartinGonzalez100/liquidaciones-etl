# Liquidaciones ETL Project

Este proyecto es una herramienta ETL (Extracción, Transformación y Carga) construida con Node.js para procesar archivos de liquidaciones desde Excel, limpiarlos y unificarlos en un archivo CSV maestro, exponiendo luego los datos a través de una API REST.

## Requisitos Previos

- [Node.js](https://nodejs.org/) (Versión 14 o superior recomendada)
- NPM (Incluido con Node.js)

## Instalación

1.  Clonar el repositorio (si aún no lo has hecho):
    ```bash
    git clone https://github.com/MartinGonzalez100/liquidaciones-etl.git
    cd liquidaciones-etl
    ```

2.  Instalar las dependencias del proyecto:
    ```bash
    npm install
    ```

## Estructura de Directorios Clave

El sistema utiliza y crea las siguientes carpetas automáticamente:
- `excel-a-convertir/`: **Entrada**. Aquí debes colocar los archivos `.xlsx` que deseas procesar.
- `csv-convertido/`: **Intermedio**. Aquí se generan los archivos CSV individuales y sus versiones limpias (`_limpio.csv`).
- `csv-unidos/`: **Salida**. Aquí encontrarás el archivo final `liquidaciones_unificadas.csv` y reportes derivados como `AcApJub.csv`.

## Cómo Ejecutar la Aplicación

### 1. Iniciar el Servidor
Para iniciar el servidor web y la API:

```bash
node server.js
```
El servidor se iniciará en `http://localhost:3000`.

### 2. Procesar Archivos (ETL)
Tienes dos opciones para correr el proceso de transformación:

**Opción A: Vía Interfaz Web (Si se implementa en un futuro frontend)**
El backend soporta carga de archivos mediante el endpoint POST `/api/process`.

**Opción B: Colocación Manual (Recomendada para pruebas)**
1.  Coloca tus archivos Excel en la carpeta `excel-a-convertir`.
2.  Reinicia el servidor o simplemente llama al endpoint de procesamiento (si estuviera configurado para leer la carpeta) o usa el script de prueba manual:

```bash
node test_etl_manual.js
```
Este script ejecutará todo el ciclo: Conversión -> Limpieza -> Unificación.

### 3. Consumir Datos (APIs)
Una vez procesados los datos, puedes acceder a ellos vía navegador o Postman:

-   **Liquidación Completa (Filtrada):**
    `http://localhost:3000/api/liquidacion-completa`
    *(Muestra registros donde Periodo Imputado coincide con Liquidado)*

-   **Residentes:**
    `http://localhost:3000/api/residentes`
    *(Filtra por PLANTA 'Residentes')*

-   **Ley 100%:**
    `http://localhost:3000/api/ley100`
    *(Filtra registros con valor en AP100_090_54)*

-   **Generar Reporte Acumulado:**
    `http://localhost:3000/api/preparar-acumulado`
    *(Genera el archivo `AcApJub.csv`)*

## Scripts de Prueba (Para Desarrolladores)

El proyecto incluye scripts para verificar el funcionamiento rápidamente:

-   **Prueba ETL Manual:** Ejecuta la conversión de los archivos presentes en `excel-a-convertir`.
    ```bash
    node test_etl_manual.js
    ```

-   **Prueba de APIs:** Levanta un servidor temporal y prueba que los endpoints respondan correctamente.
    ```bash
    node test_api.js
    ```
