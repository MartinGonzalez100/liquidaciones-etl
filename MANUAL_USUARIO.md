# Manual de Usuario - Sistema de Liquidaciones ETL

Este documento describe la funcionalidad completa del sistema de gestión, procesamiento y visualización de Liquidaciones y Novedades. Se detallan exhaustivamente las reglas y condiciones lógicas (filtros) aplicadas en cada una de las pantallas.

## 1. Módulo de Inicio (Conversión ETL)
Esta pantalla se encarga de procesar archivos Excel en bruto y unificarlos en formato CSV para su consumo rápido por parte del sistema. Es el punto de partida del flujo operativo tras haberse autenticado en la plataforma.

### 1.1 Conversión de Liquidaciones
- **Función:** Permite procesar los archivos de Liquidación.
- **Acciones Principales:**
  - **Usar carpeta local:** Procesa todos los archivos `.xlsx` y `.xls` ubicados en la carpeta `excel-a-convertir/`.
  - **Subir archivos manualmente:** Abre un diálogo para seleccionar archivos Excel.
- **Borrado de Liquidación:** Elimina de forma segura y permanente los archivos procesados para reiniciar el proceso. Se encarga de vaciar todos los archivos contenidos dentro de tres directorios: 
    - `excel-a-convertir/` (Archivos fuente originales).
    - `csv-convertido/` (Versiones transformadas).
    - `csv-unidos/` (Archivos consolidados finales como `liquidaciones_unificadas.csv`).
  - **Lotes de Liquidaciones Trabajados:** Muestra un listado histórico de los nombres de archivos procesados exitosamente. Este listado se actualiza automáticamente tras cada proceso de conversión y se limpia de forma sincronizada al ejecutar el **Borrado de Liquidación**, eliminando el archivo de registro `excel-convertidos.csv`.
- **Proceso (ETL) y Limpieza Automática:** Al hacer clic en "Iniciar Conversión y Unificación", el sistema ejecuta de forma silenciosa un **borrado automático** de liquidaciones anteriores (`csv-convertido/` y `csv-unidos/`) antes de procesar los nuevos archivos. Esto garantiza una conversión completamente limpia y previene la duplicación de datos. A diferencia del "Borrado de Liquidación" manual, este proceso automático **conserva intactos los archivos base en la carpeta `excel-a-convertir/`** para que puedan seguir siendo procesados. Los archivos leídos son luego transformados a CSV y unificados en un único archivo llamado `liquidaciones_unificadas.csv`.

### 1.2 Conversión de Novedades
- **Validación:** El sistema verifica que el nombre del archivo comience con ciertos prefijos válidos (`ld`, `reem`, `gc`, `residentes`, `criticidad`, `fortalecimiento`).
- **Acción Adicional:**
  - **Borrado de Novedad:** Elimina únicamente los archivos correspondientes a las dependencias de novedades, permitiendo reiniciar el ciclo sin afectar las liquidaciones. Vacía por completo:
    - `excel-a-convertir-novedades/` (Archivos origen de novedades).
    - `csv-unidos-novedades/` (Lotes e informes generados).
  - **Borrar Novedades de Guardias (Borrado Individual):** Elimina de forma selectiva únicamente las novedades de Guardias Críticas, permitiendo reiniciar ese flujo de manera individual. Vacía de manera específica:
    - En la carpeta `excel-a-convertir-novedades/` cualquier archivo de entrada de novedades que comience con el prefijo `gc` (ej. `gc.xlsx`, `gc_periodo.xls`).
    - En la carpeta `csv-unidos-novedades/` los archivos unificados y auxiliares de guardias: `gc.csv` y `AuxGCNovedades.csv`.
    - En el archivo de registro `excel-convertidos-novedades.csv` (ubicado en `configuracion_parametros/`), se filtran y eliminan las referencias a los archivos procesados de Guardias. Si no quedan elementos tras el filtrado, se remueve el archivo CSV para indicar la ausencia de lotes de novedades.
  - **Borrar Novedades de Residentes (Borrado Individual):** Elimina de forma selectiva únicamente las novedades de Residentes, para limpiar este flujo. Vacía:
    - Archivos base que comiencen con `residentes` en `excel-a-convertir-novedades/`.
    - Los archivos consolidados `residentes.csv` y `AuxResidentesNovedades.csv` de `csv-unidos-novedades/`.
    - Elimina automáticamente el archivo de control `AuxResidentesLiquidacion.csv` de la carpeta `csv-unidos/`.
    - Actualiza el historial `excel-convertidos-novedades.csv` quitando los lotes de residentes.
  - **Lotes de Novedades Trabajadas:** Listado que rastrea los archivos de novedades integrados al sistema. Al igual que en liquidaciones, este panel se actualiza en tiempo real tras la carga y se vacía automáticamente al realizar el **Borrado de Novedad** (o se actualiza parcialmente al realizar el **Borrado Individual** de guardias o residentes), eliminando/modificando el archivo `excel-convertidos-novedades.csv`.

---

## 2. Menú de Procesos (Barra Lateral)
Aquí se encuentra la navegación principal hacia los reportes analíticos de los datos unificados. La interfaz utiliza un sistema de **acordeones interactivos** que agrupan los submenús por categorías lógicas. Al pasar el ratón sobre cada categoría, se desplegarán suavemente sus respectivas opciones.

- **Botón de Menú Flotante:** La aplicación cuenta con un botón de menú anclado de forma fija en la esquina superior izquierda de la pantalla. Este control sigue dinámicamente al usuario mientras realiza "scroll" hacia abajo o a la derecha, garantizando el acceso permanente a la barra de navegación desde cualquier área de lectura sin necesidad de regresar al tope de la página.

- **Ajuste de Control de Navegación (Visibilidad y Acceso):** Para poder interactuar con los diferentes módulos y reportes de la barra lateral, es **requisito obligatorio** que el sistema cuente con lotes de liquidaciones trabajados y lotes de novedades trabajadas de forma simultánea.
  - Si falta alguno de los dos tipos de lotes (o ambos), la barra lateral ocultará automáticamente todos los accesos a los demás menús, submenús y acordeones de configuración, dejando únicamente visible el enlace de **Inicio (Conversión)**.
  - Asimismo, se restringe el acceso programático a cualquier otra sección; si los lotes son borrados mientras se visualiza un reporte, el sistema redirige automáticamente al usuario a la pantalla de **Inicio (Conversión)**. Los programadores conservan acceso a la Gestión de Usuarios en caso de ser necesario.

### 2.1 Controles Manuales
#### Detalle vs Detalle
- Módulo en desarrollo para visualización y comparación de detalles.

### 2.2 Informes
#### Informe de Liquidación (Dashboard)
- Es el cuadro de mando principal basado en gráficos y tablas agregadas por diferentes categorías: Planta, Área, Organismo, Reemplazos y configuraciones combinadas (LD, GC).
- **Filtros de Período (Botones Superiores - Aplica a todos los gráficos):**
  - **Todos:** Los gráficos y sumas procesan el 100% de la base.
  - **Mensuales:** Filtra y suma únicamente los registros donde `PERIODO_IMPUTADO` **es igual** a `PERIODO_LIQUIDADO`.
  - **Retroactivos:** Filtra y suma únicamente los registros donde `PERIODO_IMPUTADO` **es diferente** a `PERIODO_LIQUIDADO`.
- **Modo Editor:** Haciendo clic en "Editar Informe" se habilita un panel lateral derecho para arrastrar, redimensionar e inyectar texto a la impresión en PDF.
- **Reporte Especial Interno (Distribución de Area 1):** Un listado particular que extrae exclusivamente a los empleados donde `Area2 = 'A1'`, agrupando el costo laboral total y el conteo por componente de `PLANTA`.

#### Informe de Gestión (Actualmente oculto)
- Muestra un reporte tabulado diseñado para emular el PDF modelo "Informe de Gestión Interna".
- **Filtros Fijos:**
  - Hoja Área 1: Filtra registros con `Area2 = 'A1'`.
  - Hoja Área 26: Filtra registros con `Area2 = 'A26'`.
  - Hoja Área 27: Filtra registros con `Area2 = 'A27'`.
  - Hoja Área 3: Filtra registros con `Area2 = 'A3'`.
  - Hoja Retroactivos: Filtra registros donde `PERIODO_IMPUTADO != PERIODO_LIQUIDADO`.

### 2.3 Liquidación Completa
Muestra una tabla masiva utilizando *DataTables* con **todos** los registros encontrados en `liquidaciones_unificadas.csv`, permitiendo visualizar el padrón completo sin exclusiones.
- **Filtros UI (Buscador universal):** Permite filtrar cualquier palabra clave en tiempo real.
- **Filtros de Período (Botones Superiores):**
  - **Todos:** Muestra el universo de datos completo, sin aplicar ninguna exclusión.
  - **Mensual:** Filtra la tabla mostrando **únicamente** los registros en los que el campo `PERIODO_IMPUTADO` coincide de forma exacta con el `PERIODO_LIQUIDADO`.
  - **Retroactivo:** Filtra la tabla mostrando **únicamente** los registros en los que el campo `PERIODO_IMPUTADO` es **diferente** al `PERIODO_LIQUIDADO`.

#### Unicos de Planta
- Extrae de manera deduplicada los registros de personal de planta, mostrando únicamente el cargo más alto por cada agente (DNI).
- **Filtro de Planta:** Solo procesa los registros de `Permanente Interino`, `Permanente Titular`, `Transitorios`, `Residentes`, `Residentes Nacionales`, `RetVol2024-Permanente Titular` y `RetVol2024-Permanente Interino`.
- **Filtro Lógico:** Exige que los días trabajados sean mayores a cero (`D_TRAB > 0`) y que corresponda a liquidaciones del mes actual (`PERIODO_IMPUTADO == PERIODO_LIQUIDADO`).

#### Observar por Importes
- Filtra directamente de la base de datos casos que requieren auditoría humana por posibles anomalías e incoherencias financieras.
- **Filtros Lógicos de Consulta (Un agente aparece si cumple AL MENOS UNA de estas alertas):**
  1. Sueldo Neto Anómalo: `SUELDO_MANO` (o líquido) es negativo/muy bajo (`< 50000.00`) **O BIEN** desproporcionado (`> 5000000.00`).
  2. Ley 7991 en negativo: El saldo del campo `LIQUIDO_LEY7991` tiene un importe negativo (`< 0.00`).
  3. Aportes Jubilatorios en negativo: El importe base (`ApjLabelPer`) es negativo (`< 0.00`).
  4. Obra Social en negativo: El importe base (`ObSocPer`) resulta negativo (`< 0.00`).

#### Observar por Planta
- Detecta incoherencias entre la designación en el nomenclador de origen (`PLANTA`) y el área final del empleado (`Area2`).
- **Filtros Lógicos de Consulta (Un agente aparece si cumple alguna inconsistencia):**
  1. **Inconsistencia de Reemplazos en A1:** Se lista al empleado si su columna `Area2` indica **A1**, pero su tipo de `PLANTA` indica que es un reemplazo precario: `Reemplazante no permanente` o `Reemplazante no permanente-LD`.
  2. **Inconsistencia de Personal Estable/Hospitalario en A3:** Se lista al empleado si su columna `Area2` indica **A3**, pero su designación (`PLANTA`) es netamente de carrera u hospitalaria: `Transitorios`, `Permanente Interino`, `Permanente Titular`, `Residentes`, o `Residentes Nacionales`.

#### Control GC Liquidacion a Eps
- **Lógica:** Combina la base mensual de *GC Para Control* con los datos de *Novedades GC Para Control*. Realiza un cruce por el campo `CLAVE_AGRUPACION` (DNI + Efector) contra la `clave` de Novedades.
- **Columnas añadidas:** Muestra `Importes_Eps` con la suma de los montos cargados en Novedades, y `Control`, que calcula la diferencia exacta (`SUMA - Importes_Eps`). Si un agente en liquidación no posee novedades cargadas, su valor en `Importes_Eps` asume un 0.
- **Control Interactivo (OBSERVACION):** Una columna inicializada de forma inteligente y editable. El sistema evalúa dinámicamente el campo de control para inyectar una alerta inicial: "Verificado" (si Control es 0), "Verificar Liquidado de Mas" (si Control es > 0) y "Verificar Liquidado de Menos" (si Control es < 0). El usuario puede modificar libremente el texto de cada celda a través del input interactivo y guardar estas anotaciones de manera persistente en `AuxGCLiquidacion.csv` mediante el botón superior "Guardar Observaciones". Además, este registro se limpia de manera automática, eliminando todas las observaciones anteriores guardadas, cada vez que se ejecutan los procesos de borrado y regeneración de novedades o liquidaciones, asegurando así que los controles de un nuevo ciclo partan desde cero sin arrastrar anotaciones previas.

#### Control GC No Liquidados
- **Lógica:** Identifica aquellas Novedades que NO fueron procesadas o liquidadas en el sistema contable final. Compara la `clave` de "Novedades GC Para Control" con las claves liquidadas `CLAVE_AGRUPACION` de "GC Para Control", y visualiza exclusivamente aquellas novedades huérfanas (sin contrapartida en las liquidaciones mensuales).

#### Reemplazos
- Utiliza el filtro que extrae aquellos empleados cuya `PLANTA` corresponda a reemplazos (ej: `Reemplazante no permanente` o `Reemplazante no permanente-LD`), agrupándolos por Nivel Educativo y Organismo/Efector.

#### LD Liquidación
- Utiliza combinaciones configurables. El sistema intercepta las columnas contables específicas que el usuario ha mapeado a "Libre Disponibilidad" durante la pestaña de Configuración e integrarla al cálculo final. Genera automáticamente de forma física el archivo `AuxLDLiquidacion.csv` ordenado numéricamente por documento y período de liquidación si no existe.

##### LD Codigos Separados
- (Submenú de LD Liquidación) Genera y visualiza códigos separados provenientes de los datos de LD Liquidación. Detecta las columnas que contienen los códigos predefinidos (ej. 003, 031, 032, 033) con importes mayores a cero.
- Por cada columna encontrada, crea un registro individual que incluye nuevas columnas de control:
  - `clave`: Ubicada al inicio, conformada por la unión sin espacios de `NRO_DOCUMENTO`, `Codigo_Optimo`, `ORGANISMO` y `numero_ld_agente`.
  - `Codigo_Optimo`: Extraído del nombre de la columna.
  - `Importe_Optimo`: Valor del importe a liquidar.
  - `numero_Copia`: Contador del código dentro del mismo registro original.
  - `numero_ld_agente`: Contador general de códigos para un mismo agente (`NRO_DOCUMENTO`), que se incrementa de a 1 por cada registro generado y se reinicia al cambiar de documento.
- **Excepciones de cálculo:**
  - Si la columna es `LIB_003_34`, se toma como `Importe_Optimo` la suma de `LIB_003_34` + `LIB_032_34`.
  - Si la columna es `LIB_031_34`, se toma como `Importe_Optimo` la suma de `LIB_031_34` + `LIB_033_34`.
  - Las columnas `LIB_032_34` y `LIB_033_34` no generan un registro individual de código separado por sí solas.

#### GC Liquidación
- Similar a LD, el sistema intercepta las columnas contables específicas que el usuario ha mapeado a "Guardias Críticas" durante la pestaña de Configuración e integrarla al cálculo final.

#### GC Para Control
- **Filtro:** Registros donde `PERIODO_IMPUTADO` == `PERIODO_LIQUIDADO`.
- **Lógica:** Agrupa por `CLAVE_AGRUPACION` y muestra la suma total de `SUMA_GC` como la columna **SUMA**.

#### GC Para Control Retro
- **Filtro:** Registros donde `PERIODO_IMPUTADO` < `PERIODO_LIQUIDADO`.
- **Lógica:** Agrupa por `CLAVE_AGRUPACION` y suma los montos retroactivos de GC.

#### Residentes
- **Filtro:** Utiliza un filtro exacto donde la columna `PLANTA` debe ser `Residentes` o `Residentes Nacionales`.
- **Generación de la Tabla:** Esta tabla es procesada dinámicamente por la API del sistema (`/api/residentes`). El proceso consiste en:
  1. Leer el archivo unificado de liquidaciones (`liquidaciones_unificadas.csv`).
  2. Filtrar todos los registros asegurando que la columna `PLANTA` sea estrictamente igual a `Residentes` o `Residentes Nacionales`.
  3. Realizar una **proyección de columnas**, donde solo se envían al panel de visualización frontal (frontend) los campos esenciales estandarizados del sistema (por ejemplo, identificación, labor, periodos y totales financieros).
  4. Los datos son expuestos finalmente a través de DataTables para su correcta exploración visual.

#### Control Residentes de Liquidacion a Novedades
- **Filtro:** Procesa y cruza los registros de planta `Residentes` y `Residentes Nacionales` con los datos mensuales de novedades de Residentes. Para optimizar el control, solo se consideran los registros de planta que cumplan simultáneamente con:
  - `PERIODO_IMPUTADO` = `PERIODO_LIQUIDADO`
  - `D_TRAB` diferente de 0
- **Generación y Lógica Auxiliar:**
  - El sistema verifica la existencia del archivo `AuxResidentesLiquidacion.csv` en la carpeta `csv-unidos`. Si no existe, lo crea de forma dinámica.
  - Cruza la información de liquidaciones con el archivo de novedades (`residentes.csv`) utilizando una llave altamente específica. Desde las liquidaciones toma el `NRO_DOCUMENTO` y le anexa el carácter 7 (en mayúscula) y el carácter 8 de la columna `NIVEL` (creando, por ejemplo, `38023219A5`). Esta llave generada se cruza contra la concatenación del `DNI` y el `NIVEL` extraídos directamente de las novedades. Únicamente se realiza el cruce con aquellas novedades donde la columna `estado` sea exactamente igual a `Activo`.
  - Calcula las columnas:
    - **BRUTO 003-31:** Importe obtenido de la columna `ImporteEscalaCriticidad003_31` de novedades de Residentes.
    - **DIFERENCIAS 003-31:** Calculado como `LIB_003_31 - ((BRUTO 003-31 / 30) * (D_TRAB - DIAS_INASIST))`.
    - **BRUTO 003-91:** Importe obtenido de la columna `ImporteEscalaForta003_91` de novedades de Residentes.
    - **DIFERENCIAS 003-91:** Calculado como `LIB_003_91 - ((BRUTO 003-91 / 30) * (D_TRAB - DIAS_INASIST))`.
    - **OBSERVACION:** Un campo de entrada de texto editable. Si no existe una coincidencia de novedad activa para el residente, se inicializa automáticamente con el valor `"01-No Encontrado en Novedades de Residentes"`. En caso de que sí exista coincidencia, se inicializa con el valor por defecto `"Obs-"`. Permite cargar y guardar observaciones de forma manual preservando modificaciones personalizadas. A nivel interfaz, esta columna está optimizada para permitir el **ordenamiento alfabético y filtrado** preciso, a pesar de contener elementos interactivos de texto.
  - Las nuevas columnas se sitúan al inicio de la tabla (antes de `NIVEL`).
  - **Persistencia:** Al presionar el botón "Guardar Observaciones" situado en la barra de título, los cambios ingresados en la columna `OBSERVACION` se guardan directamente en el archivo `AuxResidentesLiquidacion.csv` en el servidor, manteniéndose para futuras cargas de la vista.

#### Residentes No Liquidados
Este reporte, accesible desde el submenú de Novedades Residentes, permite visualizar la discrepancia inversa del cruce: los agentes que se encuentran detallados en el archivo de Novedades de Residentes pero que **no aparecen** en el archivo unificado de liquidaciones.
- **Filtro y Generación:**
  - Extrae de la base de novedades (`residentes.csv`) todos los registros cuyo estado sea explícitamente `Activo`.
  - Construye la llave de cruce idéntica al módulo de control: `DNI` anexado al `NIVEL` (en Novedades) versus `NRO_DOCUMENTO` anexado al carácter 7 (en mayúscula) y carácter 8 del `NIVEL` (en Liquidaciones).
  - Devuelve íntegramente la fila cruda de la novedad del residente para todos aquellos agentes activos cuya llave **no** se encuentre en el listado de residentes liquidados del periodo actual (con `D_TRAB !== 0`).

### 2.4 Ley 100%
#### Calculo de ley 100%
- Realiza una simulación predictiva de jubilaciones para aquellos agentes que están en condiciones de percibir la Ley 100% pero que aún no la tienen liquidada. Aplica los siguientes filtros concurrentes (lógica `AND`):
  1. El empleado corresponde a las zonas contables `A1`, `A26` o `A27` (`Area2`).
  2. El periodo imputado debe coincidir exactamente con el periodo liquidado (`PERIODO_IMPUTADO == PERIODO_LIQUIDADO`).
  3. No debe pertenecer a plantas precarias de reemplazos (`PLANTA` distinta de `Reemplazante no permanente` y `Reemplazante no permanente-LD`).
  4. **No** debe estar cobrando la Ley 100% actualmente (el importe en `AP100_090_54` debe ser igual a cero o nulo/vacío).
  5. Su antigüedad calculada (`ANTIGUEDAD`) debe ser mayor o igual al parámetro numérico de Antigüedad mínima (ej. 30 años).
  6. La edad se calcula dinámicamente frente a la "Fecha de Cálculo" seleccionada. Si es mujer (`Sexo = F`), debe alcanzar o superar la `Edad F` ingresada (típicamente 60). Si es hombre (`Sexo = M`), debe alcanzar o superar la `Edad M` ingresada (típicamente 65).
  7. Se aplica una deduplicación, de forma que si un agente tiene múltiples cargos, solo se proyecta el de mayor `NUMERO_CARGO`.

### 2.5 Asignaciones
- Módulo dedicado a mostrar reportes de los agentes que perciben Asignaciones Familiares, extrayendo los datos de la liquidación unificada basándose en reglas estrictas de periodo y tipo de planta.
- **Filtros Base Universales:** Solo se toman en cuenta aquellos registros donde el monto liquidado de la asignación sea distinto de cero (`ASIG_FAM <> 0`), que además reporten días trabajados (`D_TRAB > 0`), y cuya planta de origen no sea explícitamente `Reemplazante no permanente-LD`.

#### Asignaciones Familiares
- Aplica los filtros base.
- **Lógica de Periodos:** Para todos los agentes, se requiere que el periodo imputado y el liquidado sean exactamente iguales. La única excepción a esta regla aplica a la planta de `Reemplazante no permanente`, donde el sistema también autoriza mostrar registros cuyo periodo imputado pertenezca a exactamente un mes anterior al periodo liquidado.

#### Asignaciones Familiares - Reemp
- Replica exactamente las lógicas del submenú principal, pero añade un filtro forzoso donde la planta del agente debe ser estrictamente `Reemplazante no permanente`.

### 2.6 Acumulado Aporte Jub.
#### Topes de Ap. Jubilatorios
- Visualización y filtrado sobre los aportes jubilatorios y sus límites respectivos establecidos por normativa.

### 2.7 Novedades Mensuales
- **Función:** Es un módulo dedicado exclusivamente a la visualización y control de las novedades cargadas al sistema (Guardias Críticas, Residentes, etc.), listándolas por categoría.
- **Resumen:** Haciendo clic en el menú principal "Novedades Mensuales", se despliega una vista de resumen mostrando un listado de todos los archivos (lotes generados) a partir de los documentos Excel de novedades que fueron previamente subidos y procesados.

#### Novedades GC
- Despliega una tabla consolidada con toda la información pertinente a Guardias Críticas subida por novedad.

#### Novedades GC Para Control
- Muestra los datos del archivo auxiliar `AuxGcNovedades.csv`, pero de forma **agrupada por la columna `clave`** (Documento + EfectorTransformado). La columna de `importe_guardia` representa la sumatoria total calculada para dicha clave, facilitando el cruce visual y algorítmico contra el sistema de Liquidaciones.
- **Funcionamiento y Limpieza de Datos:** Para evitar errores de visualización, el sistema detecta saltos de línea (`Enter`) en celdas y los reemplaza por espacios. Además, normaliza textos para prevenir errores de parsing.
- **Proceso de Generación y Cálculos de AuxGcNovedades:** Se genera a partir de `gc.csv`. Normaliza `EfectorTransformado` y calcula `clave_importe` usando indicadores como SDYF. Luego aplica tarifas desde `GC_config_codigos_importes.csv`.

#### Novedades Residentes
- Despliega el listado completo de los registros contenidos en el archivo `residentes.csv`. Este reporte es dinámico y muestra toda la información biográfica, administrativa y de haberes de la planta de Residentes procesada.
- **Campos Detallados Clave:**
  - **DNI / Apellido y Nombre:** Identificación unívoca del agente.
  - **ORGANISMO:** Efector asignado.
  - **RESIDENCIA / MODALIDAD:** Especialidad en formación y tipo de beca/residencia.
  - **Resolución / DescrpResol:** Datos del acto administrativo (altas, promociones, reasignaciones).
  - **Importe Escala Criticidad 003-31:** Monto asignado por criticidad.
  - **Importe Escala Forta. 003-91:** Monto asignado por fortalecimiento.

#### Residentes - Criticidad
- Muestra específicamente el detalle de los datos correspondientes a la categoría de Criticidad, extrayendo los datos del archivo `criticidad.csv`. Este reporte se enfoca en los montos y escalas de criticidad aplicados a los residentes.
- **Campos Detallados Clave:**
  - **DNI / Apellido y Nombre:** Identificación unívoca del agente.
  - **ORGANISMO:** Efector asignado.
  - **RESIDENCIA / MODALIDAD:** Especialidad en formación y tipo de beca/residencia.
  - **Escala / Tipo Escala / Escala Criticidad:** Parámetros de categorización que determinan el nivel de criticidad.
  - **Importe Escala Criticidad 003-31:** Monto asignado por el concepto de criticidad.

#### Residentes - Fortalecimiento
- Muestra el detalle de los complementos de Fortalecimiento a partir del archivo `fortalecimiento.csv`. Este reporte detalla las asignaciones adicionales orientadas al fortalecimiento de las residencias.
- **Campos Detallados Clave:**
  - **DNI / Apellido y Nombre:** Identificación unívoca del agente.
  - **ORGANISMO:** Efector asignado.
  - **RESIDENCIA / MODALIDAD:** Especialidad en formación y tipo de beca/residencia.
  - **Clave 003-91:** Código o clave identificatoria del complemento.
  - **Importe Escala Forta. 003-91:** Monto asignado por fortalecimiento.

---

## 3. Configuración del Sistema
Esta sección es el "cerebro" del sistema, donde se definen las reglas de negocio que transforman los datos crudos en información analítica. El acceso a cada uno de sus submenús está regulado de manera individual a través del sistema de permisos de usuario.

Toda la vista de esta sección posee un diseño centrado con un ancho máximo de `1000px` para optimizar su legibilidad en pantallas de gran tamaño. Visualmente, el sistema se organiza en la siguiente estructura:
- **Configuración de Paletas de Colores:** Situada en el nivel superior de manera independiente.
- **Grupo Libre Disponibilidad:** Agrupa las configuraciones que tienen relación con esta área contable (como "Configuración de Libres Disponibilidad").
- **Grupo Guardias Críticas:** Agrupa todas las configuraciones específicas del módulo ("Configuración de Guardias Críticas", "Efectores Guardias Críticas", "Códigos Guardias Críticas Importes" y "Fines de Semana y Feriados"). Para facilitar la gestión del calendario de fines de semana y feriados (SDYF), el contenido interno de este sub-acordeón se visualiza en una sección compacta con un ancho máximo del `30%` y centrada en pantalla.
- **Grupo Asignaciones:** Agrupa las configuraciones vinculadas a este rubro (como "Asignaciones A3").

Los acordeones principales agrupan a las configuraciones individuales mediante una jerarquía de doble acordeón (acordeón dentro de acordeón). Por defecto, todos los grupos principales y sub-acordeones se cargan colapsados para una experiencia visual limpia.

### 3.1 Temas Visuales y Paletas de Colores
- **Función:** Permite al usuario personalizar la experiencia visual de toda la plataforma web. Esta configuración afecta a tablas, dashboards, barra de navegación y fondos, guardando la preferencia de manera persistente en la memoria local del navegador.
- **Opciones Disponibles:**
  - **Normal:** Interfaz gráfica base con diseño plano tradicional de alto contraste.
  - **Pro:** Diseño moderno (estilo SaaS) con interfaz en tonos *slate*, sombras profundas flotantes, acentos blancos estructurales y tipografía actualizada.
  - **Oscura:** Modo bajo contraste para descanso visual o ambientes nocturnos. Anula fondos blancos de manera inteligente y adapta las tablas de datos para una excelente legibilidad. Al exportar PDF, este modo aplica una corrección dinámica temporal de un segundo hacia la "vista Normal", logrando que las impresiones en hojas blancas no salgan vacías.
  - **Siprosa Tucumán:** Paleta con identidad institucional basada en azules profundos, celestes de jerarquía y acentos en verde salud.

### 3.2 Mapeo de Columnas (LD y GC)
El sistema detecta automáticamente columnas en los archivos originales que sigan ciertos patrones de nomenclatura técnicos. Estas columnas son las que el usuario puede clasificar:
- **Prefijos Detectados:** El buscador de configuración rastrea encabezados que comiencen con: `LIB_0`, `GC_0`, `LIB_D`, `LIB_N`, `LIB_SC`, `LIB_COB`, `LIB_B`.
- **Libres Disponibilidad (LD):** El usuario selecciona qué columnas de este universo corresponden a incentivos o disponibilidades libres.
- **Guardias Críticas (GC):** El usuario selecciona las columnas restantes para conformar el reporte de Guardias Críticas. 
  - *Nota:* Una columna no puede pertenecer a ambos grupos simultáneamente.

### 3.3 Campos Estándar Predefinidos
Independientemente de la configuración de LD o GC, el sistema siempre proyecta y valida un conjunto de 33 campos base (Campos de Liquidación Completa). Entre los más importantes:
- **Identificación:** `NRO_DOCUMENTO`, `DESCAGENTE`, `CUIT_CUIL`.
- **Laborales:** `PLANTA`, `ORGANISMO`, `NIVEL`, `NUMERO_CARGO`, `FUNCION`.
- **Financieros:** `TOT_HAB`, `LIQUIDO`, `COSTO_LABORAL_02`, `SUELDO_MANO`.
- **Períodos:** `PERIODO_IMPUTADO`, `PERIODO_LIQUIDADO`.

### 3.4 Impacto de la Configuración en Reportes
La configuración guardada se almacena en los archivos `LD_config.csv` y `GC_config.csv` dentro de la carpeta `configuracion_parametros/`. Su impacto es:
1. **Reportes Específicos:** Los submenús "LD Liquidación" y "GC Liquidación" solo filtran registros que tengan valores distintos de cero en las columnas mapeadas.
2. **Cálculo de Control GC:** La columna `SUMA_GC` de la tabla auxiliar `AuxGCLiquidacion.csv` se calcula sumando únicamente las columnas mapeadas como GC.
3. **Dashboards:** Los gráficos de sectores (Tortas/Donuts) de LD y GC en el Informe de Liquidación se alimentan directamente de estos mapeos.
4. **Resumen de Gestión:** Las hojas de cálculo de áreas inyectan estos montos si están configurados.

### 3.5 Configuración de Asignaciones (Base A3)
- **Función:** Permite la gestión (Alta, Baja, Modificación) de los datos de base de Asignaciones Familiares correspondientes al Área 3, desde una tabla interactiva en la interfaz.
- **Archivo de Persistencia:** Toda modificación realizada a través de la interfaz visual se guarda directamente en el archivo `Asig_A3_config_agentes_base.csv` ubicado en la carpeta `configuracion_parametros/`.
- **Columnas Administrables:**
  - `DESCAGENTE`: Nombre y apellido del agente.
  - `NRO_DOCUMENTO`: Número de documento identificatorio.
  - `ASIG_FAM`: Monto de la asignación familiar.
  - `PLANTA`: Categoría o régimen de contratación (Ej. Reemplazante no permanente).
  - `ORGANISMO`: Efector o entidad de prestación de servicio.

*Nota:* Los paneles de configuración que son independientes del archivo de liquidaciones unificadas (como Paletas de Colores, Efectores, Códigos de Guardias Críticas, SDYF y Asignaciones A3) cargan y son totalmente editables incluso si no se ha realizado ninguna conversión o importación de archivos de liquidación en el sistema.

### 3.6 Regeneración Automática de Reportes Auxiliares
El sistema cuenta con un mecanismo de limpieza en segundo plano que garantiza la precisión de los datos auditados. Al modificar y guardar cualquier parámetro desde la pantalla de Configuración del Sistema (tales como Mapeo de LD/GC, Efectores, Códigos e Importes, Fines de Semana y Feriados, o Asignaciones A3), el servidor ejecuta de forma silenciosa el borrado de las planillas auxiliares generadas previamente (`AuxGcNovedades.csv`, `AuxGCLiquidacion.csv` y `AuxResidentesLiquidacion.csv`). 
Esta acción asegura que, la próxima vez que el usuario consulte un reporte de control, el sistema no utilice archivos cacheados obsoletos, sino que recalcule y regenere estas planillas instantáneamente leyendo la configuración más reciente.

---

## 4. Control de Acceso y Sistema de Permisos
El sistema cuenta con un esquema de seguridad basado en roles y control de accesos para garantizar que cada tipo de usuario visualice y opere exclusivamente sobre las secciones y funciones autorizadas.

### 4.1 Pantalla de Acceso (Login)
Al ingresar a la plataforma, se presenta un panel de autenticación que requiere un nombre de usuario y contraseña válidos. Esta pantalla cuenta con un fondo interactivo y dinámico, adornado con partículas de colores que siguen de forma reactiva los movimientos del cursor del mouse, proporcionando una interfaz moderna y fluida de bienvenida.
- **Persistencia de Sesión:** La sesión activa del usuario se almacena en el almacenamiento de sesión (`sessionStorage`) y expira automáticamente al cerrar la pestaña o el navegador.
- **Cierre de Sesión (Logout):** El usuario puede cerrar su sesión de forma manual mediante el botón de cierre de sesión provisto en la tarjeta de perfil de la barra lateral, ubicado de manera elegante directamente debajo del nombre de usuario y su rol. Este botón premium de ancho completo incluye un icono descriptivo y texto de acción. Al hacer clic, limpia de forma segura todos los datos cargados en la sesión activa y retorna inmediatamente a la pantalla de acceso para evitar fugas de información.
- **Cierre de la Aplicación (Apagado del Servidor):** Cuando la aplicación se ejecuta en su versión instalable de escritorio o en su versión portable comprimida (.zip) para Windows, el usuario dispone del botón **"Cerrar Aplicación"** (representado con un icono de apagado rojo) en la barra lateral, ubicado debajo del botón de Cierre de Sesión. Al hacer clic y confirmar la acción, el sistema envía una solicitud de apagado seguro al backend, deteniendo inmediatamente el servidor de Node.js en segundo plano (`aud-server.exe`) y cerrando la ventana/pestaña activa de Google Chrome. Esto garantiza que no queden procesos de fondo huérfanos consumiendo recursos en la máquina del usuario.

### 4.2 Roles y Perfiles Preconfigurados
El sistema define tres roles principales basados en el perfil del usuario:
1. **Programador (Usuario: `progra`):** 
   - Es el rol de superadministrador técnico.
   - Posee privilegios globales (`all`) que le permiten visualizar todas las pantallas y realizar cualquier operación.
   - Tiene acceso exclusivo a la sección de **Gestión de Usuarios** para administrar los accesos del sistema.
   - Posee una protección especial: el usuario principal `progra` no puede ser eliminado ni se le pueden despojar sus permisos de administrador/programador.
2. **Administrador (Usuario: `Admin`):**
   - Diseñado para personal de auditoría e informes.
   - De forma predeterminada, accede a las pantallas de conversión (Inicio), Controles Manuales, Informe de Liquidación (Dashboard) y Configuración de Parámetros.
3. **Personal (Usuario: `Perso`):**
   - Orientado a operadores que necesitan un espectro completo de visualización y control sobre las liquidaciones y novedades mensuales.
   - Tiene acceso de manera predeterminada al Procesador de Lotes, Informe de Liquidación, Liquidación Completa (incluyendo Controles Auxiliares, Reemplazos, Residentes, Ley 100%, Asignaciones) y el módulo completo de visualización de Novedades.

### 4.3 Gestión de Usuarios (Exclusivo de Programador)
El usuario con rol de **Programador** dispone de una sección dedicada accesible desde el menú lateral. Las funciones habilitadas son:
- **Listar Usuarios:** Visualización consolidada de todas las cuentas creadas en el sistema, mostrando el nombre de usuario, su rol y un resumen simplificado de sus permisos de acceso.
- **Registrar Nuevo Usuario:** Formulario interactivo que permite añadir un nuevo usuario ingresando su nombre, contraseña, rol y seleccionando mediante casillas de verificación las pantallas específicas del sistema a las que tendrá autorización.
- **Editar Usuario Existente:** Permite modificar la contraseña, cambiar el rol asignado y reconfigurar en tiempo real las pantallas autorizadas.
- **Eliminar Usuario:** Permite remover definitivamente una cuenta de usuario del sistema (a excepción de la cuenta raíz `progra`).
- **Control Granular de Configuración:** Permite habilitar o deshabilitar de forma independiente cada uno de los 7 submenús (acordeones) de la sección de Configuración de Parámetros. Si el usuario cuenta con el permiso general de Configuración o es una cuenta antigua (sin sub-permisos definidos en su registro), dispondrá de acceso completo por defecto hasta que se restrinja selectivamente.
- **Persistencia de Datos:** Todos los usuarios, credenciales y permisos se guardan y persisten de manera estructurada en el archivo `usuarios.json` en la carpeta `configuracion_parametros/`.

### 4.4 Lógica de Permisos Dinámicos y Granularidad de Submenús
El sistema implementa un control de accesos granular a nivel de submenús. Cuando a un usuario se le asigna de manera específica uno o varios submenús dentro de categorías como **Novedades Mensuales**, **Liquidación Completa**, **Ley 100%**, **Asignaciones** o **Acumulado**, el sistema habilita de forma automática y transparente el acceso a la vista interna de grillas y reportes compartida (`liquidacion-completa-view`). 
Esta lógica garantiza que el usuario pueda interactuar al 100% con la información autorizada, cargar las tablas de datos y ejecutar los procesos correspondientes, sin requerir el permiso general del acordeón ni generar advertencias de acceso restringido en la consola.
De igual manera, al cerrar la sesión (`logout`), el sistema realiza una desconexión segura limpiando la vista mediante la desactivación de secciones activas, evitando forzar la carga de la pantalla de inicio si el usuario no tiene permisos para ella, previniendo advertencias de denegación de acceso en el navegador.

---

## 5. Agregado: Dependencia de Archivos y Origen de Datos en Vistas
Para el correcto funcionamiento de las pantallas y reportes de la aplicación, es necesario comprender el origen de los datos que consume cada vista:

### 5.1 Vistas y Reportes que dependen del archivo de Liquidación Unificada (`liquidaciones_unificadas.csv`)
Estas pantallas requieren que se haya subido y procesado al menos un lote de Liquidaciones en el Módulo de Inicio:
- **Inicio (Conversión)**: El panel de "Lotes de Liquidaciones Trabajados" lee el archivo de control `excel-convertidos.csv`.
- **Informe de Liquidación (Dashboard)**: Los gráficos analíticos y cálculos agregados de costo laboral se computan desde la liquidación consolidada.
- **Liquidación Completa (Padre)**: Carga y expone la grilla general con todos los registros.
- **Submenús de Liquidación Completa**:
  - **Únicos de Planta**: Realiza filtros por tipo de planta y días trabajados.
  - **Observar por Importes** y **Observar por Planta**: Ejecutan cruces y validaciones sobre los importes y áreas/planta de la liquidación.
  - **Reemplazos**, **LD Liquidación** y **GC Liquidación**: Consumen directamente la liquidación parametrizada.
  - **GC Para Control** y **GC Para Control Retro**: Agrupan los haberes de guardias críticas de la liquidación mensual y retroactiva.
  - **Residentes**: Filtra agentes de residencias del padrón unificado.
- **Ley 100% (Cálculo de ley 100%)**: Simula la proyección de jubilaciones a partir de los datos de la liquidación.
- **Asignaciones (Asignaciones Familiares y Asignaciones Familiares - Reemp)**: Filtra y expone los montos cobrados en las liquidaciones.
- **Acumulado (Topes de Ap. Jubilatorios)**: Lee e identifica agentes al límite de sus aportes en la liquidación.
- **Configuración de Parámetros -> Libres Disponibilidad (LD) y Guardias Críticas (GC)**: Estos dos paneles de mapeo extraen los encabezados del archivo unificado (`liquidaciones_unificadas.csv`). **Nota:** Si no se ha procesado ninguna liquidación en el sistema, ambos paneles mostrarán el mensaje *"No se encontraron columnas de configuración en el archivo unificado."* (este comportamiento es normal e independiente del resto de configuraciones).

### 5.2 Vistas y Reportes que dependen de los archivos de Novedades consolidados
Estas pantallas muestran datos provenientes del procesamiento de archivos Excel de novedades en el Módulo de Inicio:
- **Inicio (Conversión)**: El panel de "Lotes de Novedades Trabajadas" lee el archivo de control `excel-convertidos-novedades.csv`.
- **Resumen de Novedades Mensuales**: Carga el listado consolidado de lotes en la carpeta de novedades.
- **Novedades GC**: Consume el archivo unificado de novedades de guardias críticas (`gc.csv`).
- **Novedades GC Para Control**: Muestra el consolidado agrupado desde el archivo auxiliar de novedades (`AuxGcNovedades.csv`).
- **Novedades Residentes**: Lee y expone los datos de `residentes.csv`.
- **Residentes - Criticidad**: Consume el archivo `criticidad.csv`.
- **Residentes - Fortalecimiento**: Consume el archivo `fortalecimiento.csv`.

### 5.3 Vistas y Reportes que dependen del Cruce de Ambos Orígenes (Liquidaciones y Novedades)
Estas vistas requieren la coexistencia de ambos tipos de archivos para su correcto análisis:
- **Control GC Liquidacion a Eps**: Cruza la liquidación mensual contra las novedades de guardias críticas.
- **Control GC No Liquidados**: Identifica novedades huérfanas sin contrapartida en la liquidación mensual.
- **Control Residentes de Liquidacion a Novedades**: Realiza cálculos y cruces entre los agentes de planta residentes y sus novedades correspondientes, permitiendo guardar observaciones en `AuxResidentesLiquidacion.csv`.

