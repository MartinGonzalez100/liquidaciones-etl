# Manual de Usuario - Sistema de Liquidaciones ETL

Este documento describe la funcionalidad completa del sistema de gestión, procesamiento y visualización de Liquidaciones y Novedades.

## 1. Módulo de Inicio (Conversión ETL)
Esta es la pantalla inicial del sistema, encargada procesar archivos Excel en bruto y unificarlos en formato CSV para su consumo rápido por parte del sistema.

### 1.1 Conversión de Liquidaciones
- **Función:** Permite procesar los archivos de Liquidación.
- **Acciones:**
  - **Usar carpeta local:** Procesa todos los archivos `.xlsx` y `.xls` ubicados en la carpeta `excel-a-convertir/`.
  - **Subir archivos manualmente:** Abre un diálogo para seleccionar archivos Excel desde la computadora.
- **Proceso (ETL):** Los archivos leídos son transformados a CSV. Luego, todos los CSV generados son leídos, limpiados de espacios en blanco irrelevantes y **unificados en un único gran archivo** llamado `liquidaciones_unificadas.csv`.

### 1.2 Conversión de Novedades
- **Función:** Permite procesar los archivos específicos de Novedades.
- **Validación:** El sistema verifica que el nombre del archivo comience con ciertos prefijos válidos (`ld`, `reem`, `gc`, `residentes`, `criticidad`, `fortalecimiento`). Si el archivo no tiene este prefijo, es ignorado.
- **Proceso:** Convierte los Excel válidos y guarda los resultados en archivos CSV independientes con el nombre del prefijo dentro de la carpeta `csv-unidos-novedades/` (ej: `ld.csv`). Limpia los espacios al final de las celdas de texto.

---

## 2. Menú de Procesos (Barra Lateral)
Aquí se encuentra la navegación principal hacia los reportes analíticos de los datos unificados.

### 2.1 Liquidación Completa
- Muestra una tabla masiva utilizando *DataTables* con todos los registros encontrados en `liquidaciones_unificadas.csv`.
- **Filtros UI:** Posee un buscador flotante universal que permite filtrar cualquier palabra clave (nombre, DNI, área, planta) en tiempo real, además de botones para exportar los datos visibles a Excel, CSV o PDF al instante.

### 2.2 Informe de Liquidación (Dashboard)
- Es el cuadro de mando principal basado en gráficos y tablas agregadas por diferentes categorías: Planta, Área, Organismo, Reemplazos y configuraciones combinadas (LD, GC).
- **Filtros Superiores (Botones):**
  - **Todos:** Muestra el universo de datos completo.
  - **Mensuales:** Filtra y suma únicamente los registros donde el `PERIODO_IMPUTADO` es **igual** al `PERIODO_LIQUIDADO`. Excluye retroactivos.
  - **Retroactivos:** Filtra y suma únicamente los registros donde el `PERIODO_IMPUTADO` es **diferente** al `PERIODO_LIQUIDADO`.
- **Modo Editor:** Haciendo clic en "Editar Informe" se habilita un panel lateral derecho.
  - Permite arrastrar, soltar e intercambiar la posición de los gráficos (Drag & Drop).
  - Permite redimensionar dinámicamente el alto de los contenedores (los gráficos se auto-ajustan).
  - Permite inyectar texto personalizado enriquecido (Negritas, colores, alineación, saltos de página) que será visible en la impresión.
- **Exportación:** Permite generar un reporte en PDF optimizado estructuralmente para papel Oficio apaisado.

### 2.3 Informe de Gestión (Actualmente oculto)
- Muestra un reporte tabulado estricto diseñado para igualar visualmente el PDF modelo "Informe de Gestión Interna".
- Genera 6 hojas distintas e independientes (Área 1, Área 26, Área 27, Área 3, Retroactivos, Resumen mensual en % y agentes).
- **Exportación:** Crea un PDF estandarizado tamaño A4 con orientación vertical, forzando saltos de página perfectos.

### 2.4 Control de Auditoría: Observar por Importes
- Filtra directamente de la base de datos casos que requieren auditoría humana por incoherencias financieras.
- **Condiciones / Filtros Internos (Backend):**
  - Casos donde el `SUELDO_MANO` (o líquido) es negativo/irrisorio (`< 50000`) o astronómico (`> 5000000`).
  - Casos donde el saldo `LIQUIDO_LEY7991` tiene importes negativos (`< 0`).
  - Casos donde hay errores en Aportes Jubilatorios o de Obra Social (`ApjLabelPer < 0` y `ObSocPer < 0`).
- Mapea las columnas contables específicas de aportes (ApJubPer, ApjLabelPer, etc.) para su visualización.

### 2.5 Control de Auditoría: Observar por Planta
- Detecta incoherencias entre el cruce del Área donde trabaja un empleado y la designación formal de su Planta.
- **Condiciones / Filtros Internos (Backend):**
  - Busca empleados en el **Área 1 (A1)** que erróneamente estén designados como `Reemplazante no permanente` o `Reemplazante no permanente-LD`.
  - Busca empleados en el **Área 3 (A3)** que erróneamente estén en plantas estables u hospitalarias (`Permanente Titular`, `Interino`, `Transitorios`, o `Residentes`).

### 2.6 Reportes Auxiliares (Reemplazos, LD Liquidación, GC Liquidación, Residentes)
- Son enlaces directos a listados pre-filtrados dependiendo de la configuración almacenada o de mapeos de bases de datos.
- **LD / GC:** Utilizan las combinaciones y los mapeos cargados previamente en la vista de *Configuración* (ej: Mapear la columna `H00902` a la guardia crítica). 

---

## 3. Configuración del Sistema
- **Función:** Vista dedicada a mapear columnas crudas del sistema de origen a descripciones legibles (Ej: "Concepto 4440" => "Libre Disponibilidad B").
- Permite subir, visualizar y editar el mapeo para Guardias Críticas (GC) y Libres Disponibilidades (LD).
- Esta configuración modifica dinámicamente en tiempo real los gráficos del Dashboard y la forma en que los cálculos suman los importes.
