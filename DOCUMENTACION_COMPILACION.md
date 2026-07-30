# Manual de Compilación y Empaquetado de Liquidaciones ETL para Windows

Este manual detalla los pasos para realizar la compilación y empaquetado de la aplicación en un instalable para Windows (`.exe`). Está diseñado para que puedas repetir este proceso de forma manual en el futuro cuando desarrolles nuevas versiones del sistema.

---

## 1. Requisitos Previos

Antes de comenzar, asegúrate de tener instalados los siguientes componentes en tu máquina de desarrollo:

1.  **Node.js** (versión 14 o superior recomendada).
2.  **Inno Setup Compiler** (herramienta gratuita estándar de Windows para generar instaladores). Puedes descargarla de su [sitio web oficial](https://jrsoftware.org/isdl.php).

---

## 2. Paso a Paso para Compilar y Empaquetar

El proceso consta de 3 simples pasos: instalar dependencias, compilar el servidor a binario y compilar el instalador.

### Paso 1: Instalación de Dependencias
Abre una terminal (PowerShell o CMD) en la carpeta raíz del proyecto (`c:\proyecto-aud`) y ejecuta:
```bash
npm install
```
Esto instalará todas las dependencias necesarias, incluyendo la herramienta `pkg` que se encarga de convertir el código Node.js en un ejecutable binario.

### Paso 2: Compilación del Servidor (Node.js a `.exe`)
Ejecuta el siguiente comando para compilar el backend de la aplicación:
```bash
npm run build:exe
```
**¿Qué sucede tras ejecutar este comando?**
*   La herramienta `pkg` compila el archivo `server.js` y todo su árbol de dependencias (`services/`, node_modules, etc.) para la plataforma Windows x64.
*   Crea una carpeta llamada `dist/` en la raíz del proyecto.
*   Dentro de la carpeta `dist/` se generará el ejecutable binario **`aud-server.exe`**.

> [!NOTE]
> La carpeta `public/` (frontend) se mantiene intencionadamente fuera del ejecutable compilado. Esto te permite realizar cambios rápidos en la interfaz visual (HTML, CSS, JS) sin tener que volver a compilar el backend. Simplemente actualizas los archivos en la carpeta `public` y vuelves a generar el instalador con Inno Setup.

### Paso 3: Generación del Instalador de Windows (`.exe` final)

Tienes dos alternativas para realizar este paso, una visual y otra completamente automatizada por consola:

#### Alternativa A: Vía Gráfica (Inno Setup Compiler)
1.  Abre el programa **Inno Setup Compiler** que instalaste en Windows.
2.  Ve a `File` -> `Open` y selecciona el archivo **`setup.iss`** ubicado en la raíz del proyecto (`c:\proyecto-aud\setup.iss`).
3.  En el menú superior, selecciona **Build** -> **Compile** (o presiona la tecla **F9**).
4.  Inno Setup leerá el script y generará el instalador **`Instalador-Liquidaciones-ETL.exe`** en la carpeta raíz del proyecto.

#### Alternativa B: Vía Consola (100% Automática y Recomendada)
Si deseas generar tanto el binario del servidor como el instalador con un solo comando en la terminal, puedes usar:
```bash
npm run build:all
```
*(Este script ejecuta primero `npm run build:exe` y luego llama a `iscc setup.iss` para empaquetar todo).*

> [!IMPORTANT]
> **Nota de Configuración para la Consola:** Para que la **Alternativa B** funcione, la carpeta de instalación de Inno Setup (donde se encuentra el ejecutable `ISCC.exe`, por defecto `C:\Program Files (x86)\Inno Setup 6`) debe estar agregada a las Variables de Entorno del Sistema (`PATH` de Windows). Durante la instalación de Inno Setup, asegúrate de marcar la casilla que ofrece agregar el compilador de línea de comandos al PATH.

#### Alternativa C: Carpeta Portable Compartida (.zip) (No requiere Inno Setup)
Si decides distribuir el sistema como una carpeta comprimida `.zip` sin usar ningún instalador gráfico, puedes empaquetar toda la aplicación con estos comandos:
```bash
npm run build:exe
npm run build:portable
```
**¿Qué hace este proceso?**
1.  Compila el servidor en `dist/aud-server.exe`.
2.  Genera una carpeta limpia llamada `dist-portable/` en la raíz del proyecto.
3.  Copia de forma automática el ejecutable compilado, el script lanzador silencioso (`lanzador.vbs`), la interfaz HTML (`public/`), los archivos de configuración iniciales, y crea todas las carpetas vacías requeridas de entrada, salida y temporales.
4.  **Para distribuir:** Solo debes comprimir la carpeta `dist-portable/` en un archivo `.zip` (haciendo clic derecho sobre ella en Windows -> *Enviar a -> Carpeta comprimida (en zip)*) y compartir ese archivo con los usuarios. Ellos solo tendrán que descomprimirlo y hacer doble clic en `lanzador.vbs` para comenzar a trabajar.

---

## 3. Estructura de la Aplicación Instalada

Cuando un usuario ejecute el instalador en su máquina, Inno Setup creará la siguiente estructura en la carpeta de instalación (por defecto `C:\Program Files (x86)\LiquidacionesETL`):

*   **`aud-server.exe`**: El servidor web Express compilado que procesa los archivos.
*   **`lanzador.vbs`**: Script que ejecuta el servidor en segundo plano (ocultando la consola de comandos de Node) y abre Google Chrome en modo aplicación limpia.
*   **`public/`**: Contiene la interfaz HTML/CSS/JS.
*   **`configuracion_parametros/`**: Contiene los archivos CSV de configuración (`LD_config.csv`, etc.).
*   **Carpetas de datos (vacías)**:
    *   `excel-a-convertir/` (carpeta de entrada)
    *   `excel-a-convertir-novedades/` (carpeta de entrada novedades)
    *   `csv-convertido/` (carpeta temporal)
    *   `csv-unidos/` (carpeta de salida liquidaciones)
    *   `csv-unidos-novedades/` (carpeta de salida novedades)
    *   `uploads/` (carpeta temporal de subida)

---

## 4. Prueba del Instalable en Producción

Una vez generado el instalador:
1.  Instala la aplicación en tu máquina ejecutando `Instalador-Liquidaciones-ETL.exe`.
2.  Usa el acceso directo creado en el escritorio ("Liquidaciones ETL") para iniciarla. Debe abrirse una ventana dedicada de Google Chrome sin barras de direcciones.
3.  Realiza un procesamiento de prueba arrastrando archivos de Excel a la interfaz y verifica que los CSV resultantes se escriban correctamente en la carpeta de instalación local correspondientes.
4.  Para cerrar la aplicación de manera segura, presiona el botón **"Cerrar Aplicación"** en la barra lateral. Esto apagará el servidor local de Node en segundo plano y cerrará la pestaña de Chrome.
