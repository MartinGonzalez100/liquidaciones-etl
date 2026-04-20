# Manual de Usuario - Sistema de Liquidaciones ETL

Este documento describe la funcionalidad completa del sistema de gestión, procesamiento y visualización de Liquidaciones y Novedades. Se detallan exhaustivamente las reglas y condiciones lógicas (filtros) aplicadas en cada una de las pantallas.

## 1. Módulo de Inicio (Conversión ETL)
Esta es la pantalla inicial del sistema, encargada de procesar archivos Excel en bruto y unificarlos en formato CSV para su consumo rápido por parte del sistema.

### 1.1 Conversión de Liquidaciones
- **Función:** Permite procesar los archivos de Liquidación.
- **Acciones Principales:**
  - **Usar carpeta local:** Procesa todos los archivos `.xlsx` y `.xls` ubicados en la carpeta `excel-a-convertir/`.
  - **Subir archivos manualmente:** Abre un diálogo para seleccionar archivos Excel.
  - **Borrado de Liquidación:** Elimina de forma segura y permanente los archivos procesados para reiniciar el proceso. Se encarga de vaciar todos los archivos contenidos dentro de tres directorios: 
    - `excel-a-convertir/` (Archivos fuente originales).
    - `csv-convertido/` (Versiones transformadas).
    - `csv-unidos/` (Archivos consolidados finales como `liquidaciones_unificadas.csv`).
- **Proceso (ETL):** Los archivos leídos son transformados a CSV. Luego son unificados en un único gran archivo llamado `liquidaciones_unificadas.csv`.

### 1.2 Conversión de Novedades
- **Validación:** El sistema verifica que el nombre del archivo comience con ciertos prefijos válidos (`ld`, `reem`, `gc`, `residentes`, `criticidad`, `fortalecimiento`).
- **Acción Adicional:**
  - **Borrado de Novedad:** Elimina únicamente los archivos correspondientes a las dependencias de novedades, permitiendo mantener las liquidaciones intactas y reiniciar el ciclo de las novedades. Vacía por completo las siguientes carpetas:
    - `excel-a-convertir-novedades/` (Archivos origen de novedades subidos).
    - `csv-unidos-novedades/` (Lotes e informes generados de novedades).

---

## 2. Menú de Procesos (Barra Lateral)
Aquí se encuentra la navegación principal hacia los reportes analíticos de los datos unificados.

### 2.1 Liquidación Completa
- Muestra una tabla masiva utilizando *DataTables* con todos los registros encontrados en `liquidaciones_unificadas.csv`.
- **Filtros UI (Buscador universal):** Permite filtrar cualquier palabra clave (nombre, DNI, área, planta) en tiempo real.
- **Filtros de Período (Botones Superiores):**
  - **Todos:** Muestra el universo de datos completo, sin aplicar ninguna exclusión.
  - **Mensual:** Filtra la tabla mostrando **únicamente** los registros en los que el campo `PERIODO_IMPUTADO` coincide de forma exacta con el `PERIODO_LIQUIDADO` (ej. Imputado '202603' y Liquidado '202603'). Se excluyen las diferencias.
  - **Retroactivo:** Filtra la tabla mostrando **únicamente** los registros en los que el campo `PERIODO_IMPUTADO` es **diferente** al `PERIODO_LIQUIDADO` (ej. liquidando meses pasados en el mes actual).

### 2.2 Informe de Liquidación (Dashboard)
- Es el cuadro de mando principal basado en gráficos y tablas agregadas por diferentes categorías: Planta, Área, Organismo, Reemplazos y configuraciones combinadas (LD, GC).
- **Filtros de Período (Botones Superiores - Aplica a todos los gráficos):**
  - **Todos:** Los gráficos y sumas procesan el 100% de la base.
  - **Mensuales:** Filtra y suma únicamente los registros donde `PERIODO_IMPUTADO` **es igual** a `PERIODO_LIQUIDADO`.
  - **Retroactivos:** Filtra y suma únicamente los registros donde `PERIODO_IMPUTADO` **es diferente** a `PERIODO_LIQUIDADO`.
- **Modo Editor:** Haciendo clic en "Editar Informe" se habilita un panel lateral derecho para arrastrar, redimensionar e inyectar texto a la impresión en PDF.
- **Reporte Especial Interno (Distribución de Area 1):** Un listado particular que extrae exclusivamente a los empleados donde `Area2 = 'A1'`, agrupando el costo laboral total y el conteo por componente de `PLANTA`.

### 2.3 Informe de Gestión (Actualmente oculto)
- Muestra un reporte tabulado diseñado para emular el PDF modelo "Informe de Gestión Interna".
- **Filtros Fijos:**
  - Hoja Área 1: Filtra registros con `Area2 = 'A1'`.
  - Hoja Área 26: Filtra registros con `Area2 = 'A26'`.
  - Hoja Área 27: Filtra registros con `Area2 = 'A27'`.
  - Hoja Área 3: Filtra registros con `Area2 = 'A3'`.
  - Hoja Retroactivos: Filtra registros donde `PERIODO_IMPUTADO != PERIODO_LIQUIDADO`.

### 2.4 Control de Auditoría: Observar por Importes
- Filtra directamente de la base de datos casos que requieren auditoría humana por posibles anomalías e incoherencias financieras.
- **Filtros Lógicos de Consulta (Un agente aparece si cumple AL MENOS UNA de estas alertas):**
  1. Sueldo Neto Anómalo: `SUELDO_MANO` (o líquido) es negativo/muy bajo (`< 50000.00`) **O BIEN** desproporcionado (`> 5000000.00`).
  2. Ley 7991 en negativo: El saldo del campo `LIQUIDO_LEY7991` tiene un importe negativo (`< 0.00`).
  3. Aportes Jubilatorios en negativo: El importe base (`ApjLabelPer`) es negativo (`< 0.00`).
  4. Obra Social en negativo: El importe base (`ObSocPer`) resulta negativo (`< 0.00`).

### 2.5 Control de Auditoría: Observar por Planta
- Detecta incoherencias entre la designación en el nomenclador de origen (`PLANTA`) y el área final del empleado (`Area2`).
- **Filtros Lógicos de Consulta (Un agente aparece si cumple alguna inconsistencia):**
  1. **Inconsistencia de Reemplazos en A1:** Se lista al empleado si su columna `Area2` indica **A1**, pero su tipo de `PLANTA` indica que es un reemplazo precario: `Reemplazante no permanente` o `Reemplazante no permanente-LD`.
  2. **Inconsistencia de Personal Estable/Hospitalario en A3:** Se lista al empleado si su columna `Area2` indica **A3**, pero su designación (`PLANTA`) es netamente de carrera u hospitalaria: `Transitorios`, `Permanente Interino`, `Permanente Titular`, `Residentes`, o `Residentes Nacionales`.

### 2.6 Reportes Auxiliares (Reemplazos, Residentes, GC, Ley 100%)
- **Reemplazos:** Utiliza el filtro que extrae aquellos empleados cuya `PLANTA` corresponda a reemplazos (ej: `Reemplazante no permanente` o `Reemplazante no permanente-LD`), agrupándolos por Nivel Educativo y Organismo/Efector.
- **Residentes:** Utiliza un filtro exacto donde la columna `PLANTA` debe ser `Residentes` o `Residente Nacionales`.
- **GC Liquidación / LD:** Utilizan combinaciones configurables. El sistema intercepta las columnas contables específicas que el usuario ha mapeado a "Guardias Críticas" o "Libre Disponibilidad" durante la pestaña de Configuración (ej: mapear `H00902` e integrarla al cálculo final).
- **Ley 100%:** Aplica los siguientes filtros concurrentes (lógica `AND`) en la simulación predictiva de jubilaciones:
  1. El empleado corresponde a las zonas contables `A1`, `A26` o `A27` (`Area2`).
  2. Si su sexo contable extraído del CUIL denota ser mujer, debe tener una edad superior al parámetro `Edad F` provisto en el calendario del frontend (típicamente 60).
  3. Si denota ser hombre, debe superar el parámetro `Edad M` (típicamente 65).
  4. Su antigüedad calculada (generalmente leída desde columnas nativas como `ANTIGUEDAD`) debe superar el parámetro numérico base (ej. 30 años).

### 2.7 Reportes de Control GC (Guardias Críticas)
- **Función:** Permite realizar un control detallado de los montos de Guardias Críticas agrupados por agente y organismo, facilitando la auditoría de liquidaciones mensuales y retroactivas.
- **Generación de Datos (`AuxGCLiquidacion.csv`):** El sistema crea automáticamente una tabla auxiliar para el cálculo:
  - **CLAVE_AGRUPACION:** Unión de `NRO_DOCUMENTO` + `ORGANISMO` (sin espacios a la derecha).
  - **SUMA_GC:** Suma horizontal de todos los montos de las columnas configuradas como GC en ese registro.
- **Submenú: GC Para Control:**
  - **Filtro:** Registros donde `PERIODO_IMPUTADO` == `PERIODO_LIQUIDADO`.
  - **Lógica:** Agrupa por `CLAVE_AGRUPACION` y muestra la suma total de `SUMA_GC` como la columna **SUMA**. 
- **Submenú: GC Para Control Retro:**
  - **Filtro:** Registros donde `PERIODO_IMPUTADO` < `PERIODO_LIQUIDADO`.
  - **Lógica:** Agrupa por `CLAVE_AGRUPACION` y suma los montos retroactivos de GC.

---

## 3. Configuración del Sistema
Esta sección es el "cerebro" del sistema, donde se definen las reglas de negocio que transforman los datos crudos en información analítica.

### 3.1 Mapeo de Columnas (LD y GC)
El sistema detecta automáticamente columnas en los archivos originales que sigan ciertos patrones de nomenclatura técnicos. Estas columnas son las que el usuario puede clasificar:
- **Prefijos Detectados:** El buscador de configuración rastrea encabezados que comiencen con: `LIB_0`, `GC_0`, `LIB_D`, `LIB_N`, `LIB_SC`, `LIB_COB`, `LIB_B`.
- **Libres Disponibilidad (LD):** El usuario selecciona qué columnas de este universo corresponden a incentivos o disponibilidades libres.
- **Guardias Críticas (GC):** El usuario selecciona las columnas restantes para conformar el reporte de Guardias Críticas. 
  - *Nota:* Una columna no puede pertenecer a ambos grupos simultáneamente.

### 3.2 Campos Estándar Predefinidos
Independientemente de la configuración de LD o GC, el sistema siempre proyecta y valida un conjunto de 33 campos base (Campos de Liquidación Completa). Entre los más importantes:
- **Identificación:** `NRO_DOCUMENTO`, `DESCAGENTE`, `CUIT_CUIL`.
- **Laborales:** `PLANTA`, `ORGANISMO`, `NIVEL`, `NUMERO_CARGO`, `FUNCION`.
- **Financieros:** `TOT_HAB`, `LIQUIDO`, `COSTO_LABORAL_02`, `SUELDO_MANO`.
- **Períodos:** `PERIODO_IMPUTADO`, `PERIODO_LIQUIDADO`.

### 3.3 Impacto de la Configuración en Reportes
La configuración guardada se almacena en los archivos `LD_config.csv` y `GC_config.csv` dentro de la carpeta `configuracion_parametros/`. Su impacto es:
1. **Reportes Específicos:** Los submenús "LD Liquidación" y "GC Liquidación" solo filtran registros que tengan valores distintos de cero en las columnas mapeadas.
2. **Cálculo de Control GC:** La columna `SUMA_GC` de la tabla auxiliar `AuxGCLiquidacion.csv` se calcula sumando únicamente las columnas mapeadas como GC.
3. **Dashboards:** Los gráficos de sectores (Tortas/Donuts) de LD y GC en el Informe de Liquidación se alimentan directamente de estos mapeos.
4. **Resumen de Gestión:** Las hojas de cálculo de áreas inyectan estos montos si están configurados.

---

## 4. Historial de Cambios (Abril 2026)
- **Correcciones UI:** Se solucionó un bug de navegación donde el bloque de "Lotes de Liquidaciones Trabajados" y "Lotes de Novedades Trabajadas" permanecía visible al cambiar de pantalla. Se le asignó el identificador `lotes-wrapper` y lógica nativa en `showSection()` para ocultarse temporalmente.
- **Parametrización del Asistente:** Se configuró el archivo `.antigravity_rules.md` para estandarizar los procesos automatizados de despliegue y documentación en el repositorio.
