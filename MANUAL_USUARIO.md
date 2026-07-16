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
  - **Lotes de Novedades Trabajadas:** Listado que rastrea los archivos de novedades integrados al sistema. Al igual que en liquidaciones, este panel se actualiza en tiempo real tras la carga y se vacía automáticamente al realizar el **Borrado de Novedad**, eliminando el archivo `excel-convertidos-novedades.csv`.

---

## 2. Menú de Procesos (Barra Lateral)
Aquí se encuentra la navegación principal hacia los reportes analíticos de los datos unificados. La interfaz utiliza un sistema de **acordeones interactivos** que agrupan los submenús por categorías lógicas. Al pasar el ratón sobre cada categoría, se desplegarán suavemente sus respectivas opciones.

- **Botón de Menú Flotante:** La aplicación cuenta con un botón de menú anclado de forma fija en la esquina superior izquierda de la pantalla. Este control sigue dinámicamente al usuario mientras realiza "scroll" hacia abajo o a la derecha, garantizando el acceso permanente a la barra de navegación desde cualquier área de lectura sin necesidad de regresar al tope de la página.

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

#### Control GC No Liquidados
- **Lógica:** Identifica aquellas Novedades que NO fueron procesadas o liquidadas en el sistema contable final. Compara la `clave` de "Novedades GC Para Control" con las claves liquidadas `CLAVE_AGRUPACION` de "GC Para Control", y visualiza exclusivamente aquellas novedades huérfanas (sin contrapartida en las liquidaciones mensuales).

#### Reemplazos
- Utiliza el filtro que extrae aquellos empleados cuya `PLANTA` corresponda a reemplazos (ej: `Reemplazante no permanente` o `Reemplazante no permanente-LD`), agrupándolos por Nivel Educativo y Organismo/Efector.

#### LD Liquidación
- Utiliza combinaciones configurables. El sistema intercepta las columnas contables específicas que el usuario ha mapeado a "Libre Disponibilidad" durante la pestaña de Configuración e integrarla al cálculo final.

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
- **Filtro:** Procesa y cruza los registros de planta `Residentes` y `Residentes Nacionales` con los datos mensuales de novedades de Residentes.
- **Generación y Lógica Auxiliar:**
  - El sistema verifica la existencia del archivo `AuxResidentesLiquidacion.csv` en la carpeta `csv-unidos`. Si no existe, lo crea de forma dinámica.
  - Cruza el `NRO_DOCUMENTO` de la liquidación con el `DNI` del archivo de novedades (`residentes.csv`).
  - Calcula las columnas:
    - **BRUTO 003-31:** Importe obtenido de la columna `ImporteEscalaCriticidad003_31` de novedades de Residentes.
    - **DIFERENCIAS 003-31:** Calculado como `LIB_003_31 - ((BRUTO 003-31 / 30) * (D_TRAB - DIAS_INASIST))`.
    - **BRUTO 003-91:** Importe obtenido de la columna `ImporteEscalaForta003_91` de novedades de Residentes.
    - **DIFERENCIAS 003-91:** Calculado como `LIB_003_91 - ((BRUTO 003-91 / 30) * (D_TRAB - DIAS_INASIST))`.
    - **OBSERVACION:** Un campo de entrada de texto editable que permite cargar y guardar observaciones de forma manual.
  - Las nuevas columnas se sitúan al inicio de la tabla (antes de `NIVEL`).
  - **Persistencia:** Al presionar el botón "Guardar Observaciones" situado en la barra de título, los cambios ingresados en la columna `OBSERVACION` se guardan directamente en el archivo `AuxResidentesLiquidacion.csv` en el servidor, manteniéndose para futuras cargas de la vista.


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
- Muestra específicamente el detalle de los datos correspondientes a la categoría de Criticidad, extrayendo los datos del archivo `criticidad.csv`.

#### Residentes - Fortalecimiento
- Muestra el detalle de los complementos de Fortalecimiento a partir del archivo `fortalecimiento.csv`.

---

## 3. Configuración del Sistema
Esta sección es el "cerebro" del sistema, donde se definen las reglas de negocio que transforman los datos crudos en información analítica.

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

---

## 4. Control de Acceso y Sistema de Permisos
El sistema cuenta con un esquema de seguridad basado en roles y control de accesos para garantizar que cada tipo de usuario visualice y opere exclusivamente sobre las secciones y funciones autorizadas.

### 4.1 Pantalla de Acceso (Login)
Al ingresar a la plataforma, se presenta un panel de autenticación que requiere un nombre de usuario y contraseña válidos. Esta pantalla cuenta con un fondo interactivo y dinámico, adornado con partículas de colores que siguen de forma reactiva los movimientos del cursor del mouse, proporcionando una interfaz moderna y fluida de bienvenida.
- **Persistencia de Sesión:** La sesión activa del usuario se almacena en el almacenamiento de sesión (`sessionStorage`) y expira automáticamente al cerrar la pestaña o el navegador.
- **Cierre de Sesión (Logout):** El usuario puede cerrar su sesión de forma manual mediante el botón de cierre de sesión provisto en la tarjeta de perfil de la barra lateral, ubicado de manera elegante directamente debajo del nombre de usuario y su rol. Este botón premium de ancho completo incluye un icono descriptivo y texto de acción. Al hacer clic, limpia de forma segura todos los datos cargados en la sesión activa y retorna inmediatamente a la pantalla de acceso para evitar fugas de información.

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
- **Persistencia de Datos:** Todos los usuarios, credenciales y permisos se guardan y persisten de manera estructurada en el archivo `usuarios.json` en la carpeta `configuracion_parametros/`.
