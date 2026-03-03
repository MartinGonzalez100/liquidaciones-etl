// server.js (Nuevo punto de entrada)

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
// server.js (Añade esta línea de importación al principio con las demás)
const csv = require('csv-parser');
// ...
const { ejecutarProcesoETL } = require('./etl_runner'); // El runner que creamos

//-------------
const app = express();
const PORT = 3000;
const EXCEL_DIR = path.join(__dirname, 'excel-a-convertir');
const CSV_UNIDOS_DIR = path.join(__dirname, 'csv-unidos'); // Directorio del CSV final
const FINAL_CSV_NAME = 'liquidaciones_unificadas.csv';

const CONFIG_DIR = path.join(__dirname, 'configuracion_parametros');
const LD_CONFIG_FILE = path.join(CONFIG_DIR, 'LD_config.csv');
const GC_CONFIG_FILE = path.join(CONFIG_DIR, 'GC_config.csv');

//-----------

// Crear carpetas necesarias
if (!fs.existsSync(EXCEL_DIR)) fs.mkdirSync(EXCEL_DIR);
if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR);

// Configuración de Multer: Al subir archivos, los guardamos temporalmente.
const upload = multer({ dest: 'uploads/' });

// Middleware para servir archivos estáticos (la interfaz HTML)
app.use(express.static('public'));
app.use(express.json());

//----------------

//---- 🚨 CAMPOS REQUERIDOS PARA LIQUIDACIÓN COMPLETA
const CAMPOS_LIQUIDACION = [
    'NIVEL',
    'DESCAGENTE',
    'NRO_DOCUMENTO',
    'HAB_C_AP',
    'HAB_S_AP',
    'ASIG_FAM',
    'TOT_HAB',
    'RETENCIONES',
    'DESCUENTOS',
    'LIQUIDO',
    'CARGA_PATRONAL',
    'BRUTO_LEY7991',
    'PENSION_229_92',
    'LIQUIDO_LEY7991',
    'COSTO_LABORAL_01',
    'COSTO_LABORAL_02',
    'SUELDO_MANO',
    'SUELDO',
    'MONTO_ANTIGUEDAD',
    'DIAS_INASIST',
    'D_TRAB',
    'ApJubPer',
    'PLANTA',
    'ORGANISMO',
    'FUNCION',
    'AGRUPAMIENTO',
    'PERIODO_IMPUTADO',
    'PERIODO_LIQUIDADO',
    'NUMERO_CARGO',
    'NRO_REC',
    'FECHA_NACIMIENTO',
    'ESTADO_LIQUIDACION',
    'Area2'
];

// NUEVO ENDPOINT: Con Filtros y Proyección de Columnas
app.get('/api/liquidacion-completa', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    let filteredCount = 0;

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Por favor, ejecute primero el proceso de Conversión.' });
    }

    // Leemos el archivo como un stream
    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            // [MODIFICACIÓN DE OPTIMIZACIÓN PENDIENTE]
            const periodoImputadoLimpio = row.PERIODO_IMPUTADO ? row.PERIODO_IMPUTADO.trim() : '';
            const periodoLiquidadoLimpio = row.PERIODO_LIQUIDADO ? row.PERIODO_LIQUIDADO.trim() : '';
            // 1. APLICAR FILTRO: PERIODO_IMPUTADO = PERIODO_LIQUIDADO
            if (periodoImputadoLimpio === periodoLiquidadoLimpio) {

                // 2. PROYECCIÓN DE COLUMNAS: Crear un nuevo objeto solo con los campos deseados
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    // Usamos el operador nullish ?? para manejar casos donde el campo podría no existir.
                    projectedRow[field] = row[field] ?? '';
                });

                results.push(projectedRow);
                filteredCount++;
            }
        })
        .on('end', () => {
            console.log(`[SERVER] ✅ Lectura y Conversión completa. Registros totales en el CSV: ${results.length}.`);
            console.log(`[SERVER] Registros filtrados y enviados: ${filteredCount}.`);

            // Enviamos el array JSON filtrado y proyectado
            res.json(results);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error leyendo o parseando el CSV:', error.message);
            res.status(500).json({ error: `Error interno al procesar el archivo CSV: ${error.message}` });
        });
});

// NUEVO ENDPOINT: Obtener datos filtrados para Residentes
app.get('/api/residentes', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    let filteredCount = 0;

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado para Residentes. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            // 1. APLICAR FILTRO: PLANTA = 'Residentes' O PLANTA = 'Residente Nacionales'
            const esResidente = row.PLANTA === 'Residentes' || row.PLANTA === 'Residentes Nacionales';

            // 2. Opcional: Aplicar filtro de Liquidación Completa si también se requiere (Revisar si es necesario)
            // const filtroLiquidacionCompleta = row.PERIODO_IMPUTADO === row.PERIODO_LIQUIDADO;

            // Si el registro pasa el filtro de Residente
            if (esResidente) { // && filtroLiquidacionCompleta, si el segundo filtro es necesario

                // 3. PROYECCIÓN DE COLUMNAS: Solo los campos esenciales (reutilizando CAMPOS_LIQUIDACION)
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });

                results.push(projectedRow);
                filteredCount++;
            }
        })
        .on('end', () => {
            console.log(`[SERVER] ✅ Proceso de Residentes completo. Registros filtrados y enviados: ${filteredCount}.`);
            res.json(results);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error en el proceso de Residentes:', error.message);
            res.status(500).json({ error: `Error interno al procesar el archivo CSV para Residentes: ${error.message}` });
        });
});

// NUEVO ENDPOINT: Obtener datos filtrados para Ley 100%
app.get('/api/ley100', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    let filteredCount = 0;

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado para Ley 100%. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {

            // 1. APLICAR FILTRO: AP100_090_54 > 0
            // Convertimos el valor a número flotante para la comparación.
            const valorCampo = parseFloat(row.AP100_090_54);

            // Verificamos si es un número válido y es mayor que cero.
            if (!isNaN(valorCampo) && valorCampo > 0) {

                // 2. PROYECCIÓN DE COLUMNAS: Solo los campos esenciales (reutilizando CAMPOS_LIQUIDACION)
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });

                results.push(projectedRow);
                filteredCount++;
            }
        })
        .on('end', () => {
            console.log(`[SERVER] ✅ Proceso de Ley 100% completo. Registros filtrados y enviados: ${filteredCount}.`);
            res.json(results);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error en el proceso de Ley 100%:', error.message);
            res.status(500).json({ error: `Error interno al procesar el archivo CSV para Ley 100%: ${error.message}` });
        });
});

// NUEVO ENDPOINT: Datos para el Dashboard (Informe de Liquidación)
app.get('/api/dashboard-info', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const tipoFiltro = req.query.tipo || 'todos'; // 'todos', 'mensual', 'retroactivo'

    // 1. Cargar configuraciones de LD y GC para aggregaciones dinámicas
    const ldCols = [];
    if (fs.existsSync(LD_CONFIG_FILE)) {
        const content = fs.readFileSync(LD_CONFIG_FILE, 'utf8').split('\n');
        content.shift();
        content.forEach(line => { if (line.trim()) ldCols.push(line.trim()); });
    }

    const gcCols = [];
    if (fs.existsSync(GC_CONFIG_FILE)) {
        const content = fs.readFileSync(GC_CONFIG_FILE, 'utf8').split('\n');
        content.shift();
        content.forEach(line => { if (line.trim()) gcCols.push(line.trim()); });
    }

    const dashboardData = {
        totales: { agentes: 0, importeBruto: 0, importeLiquido: 0, costoLaboral: 0 },
        porPlanta: {},
        porOrganismo: {},
        porArea: {},
        porReemplazoNivel: {},
        porReemplazoOrganismo: {},
        porLibresDisponibilidades: {},
        porGuardiasCriticas: {},
        porArea1: {},
        porArea3: {
            "LD Liquidacion": { nombre: "LD Liquidacion", cantidad: 0, importe: 0 },
            "GC Liquidacion": { nombre: "GC Liquidacion", cantidad: 0, importe: 0 },
            "Reemplazos": { nombre: "Reemplazos", cantidad: 0, importe: 0 }
        }
    };

    if (!fs.existsSync(csvPath)) {
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const periodoImputado = row.PERIODO_IMPUTADO ? row.PERIODO_IMPUTADO.trim() : '';
            const periodoLiquidado = row.PERIODO_LIQUIDADO ? row.PERIODO_LIQUIDADO.trim() : '';
            const esMensual = (periodoImputado === periodoLiquidado);

            // Aplicar filtro de tipo
            if (tipoFiltro === 'mensual' && !esMensual) return;
            if (tipoFiltro === 'retroactivo' && esMensual) return;

            const importeBruto = parseFloat(row.TOT_HAB) || 0;
            const importeLiquido = parseFloat(row.LIQUIDO) || 0;
            const costoLaboral = parseFloat(row.COSTO_LABORAL_02) || 0;
            const planta = row.PLANTA || 'SIN ESPECIFICAR';
            const organismo = row.ORGANISMO || 'SIN ESPECIFICAR';
            const area = row.Area2 || 'SIN ESPECIFICAR';

            // Acumular totales generales
            dashboardData.totales.agentes++;
            dashboardData.totales.importeBruto += importeBruto;
            dashboardData.totales.importeLiquido += importeLiquido;
            dashboardData.totales.costoLaboral += costoLaboral;

            // Acumular por Planta
            if (!dashboardData.porPlanta[planta]) {
                dashboardData.porPlanta[planta] = { nombre: planta, cantidad: 0, importe: 0 };
            }
            dashboardData.porPlanta[planta].cantidad++;
            dashboardData.porPlanta[planta].importe += importeBruto;

            // Acumular por Organismo
            if (!dashboardData.porOrganismo[organismo]) {
                dashboardData.porOrganismo[organismo] = { nombre: organismo, cantidad: 0, importe: 0 };
            }
            dashboardData.porOrganismo[organismo].cantidad++;
            dashboardData.porOrganismo[organismo].importe += importeBruto;

            // Acumular por Area2
            if (!dashboardData.porArea[area]) {
                dashboardData.porArea[area] = { nombre: area, cantidad: 0, importe: 0 };
            }
            dashboardData.porArea[area].cantidad++;
            dashboardData.porArea[area].importe += costoLaboral;

            // Acumular por Area1 (si Area2 == 'A1', agrupado por PLANTA)
            if (area === 'A1') {
                if (!dashboardData.porArea1[planta]) {
                    dashboardData.porArea1[planta] = { nombre: planta, cantidad: 0, importe: 0 };
                }
                dashboardData.porArea1[planta].cantidad++;
                dashboardData.porArea1[planta].importe += costoLaboral;
            }

            // --- AGREGACIONES DINÁMICAS (LD y GC) ---
            let hasLD = false;
            ldCols.forEach(col => {
                const val = parseFloat(row[col]) || 0;
                if (val !== 0) {
                    hasLD = true;
                    if (!dashboardData.porLibresDisponibilidades[col]) {
                        dashboardData.porLibresDisponibilidades[col] = { nombre: col, cantidad: 0, importe: 0 };
                    }
                    dashboardData.porLibresDisponibilidades[col].cantidad++;
                    dashboardData.porLibresDisponibilidades[col].importe += val;
                }
            });

            let hasGC = false;
            gcCols.forEach(col => {
                const val = parseFloat(row[col]) || 0;
                if (val !== 0) {
                    hasGC = true;
                    if (!dashboardData.porGuardiasCriticas[col]) {
                        dashboardData.porGuardiasCriticas[col] = { nombre: col, cantidad: 0, importe: 0 };
                    }
                    dashboardData.porGuardiasCriticas[col].cantidad++;
                    dashboardData.porGuardiasCriticas[col].importe += val;
                }
            });

            // --- AREA 3 (LD, GC, Reemplazos) ---
            if (hasLD) {
                dashboardData.porArea3["LD Liquidacion"].cantidad++;
                dashboardData.porArea3["LD Liquidacion"].importe += costoLaboral;
            }
            if (hasGC) {
                dashboardData.porArea3["GC Liquidacion"].cantidad++;
                dashboardData.porArea3["GC Liquidacion"].importe += costoLaboral;
            }
            if (planta === "Reemplazante no permanente") {
                dashboardData.porArea3["Reemplazos"].cantidad++;
                dashboardData.porArea3["Reemplazos"].importe += costoLaboral;
            }

            // --- LÓGICA ESPECÍFICA DE REEMPLAZOS ---
            if (planta === "Reemplazante no permanente") {
                const nivel = row.NIVEL || 'SIN ESPECIFICAR';
                const dias = parseFloat(row.D_TRAB) || 0;

                // 1. Por Nivel
                if (!dashboardData.porReemplazoNivel[nivel]) {
                    dashboardData.porReemplazoNivel[nivel] = { nombre: nivel, cantidad: 0, importe: 0, dias: 0 };
                }
                dashboardData.porReemplazoNivel[nivel].cantidad++;
                dashboardData.porReemplazoNivel[nivel].importe += costoLaboral;
                dashboardData.porReemplazoNivel[nivel].dias += dias;

                // 2. Por Organismo
                if (!dashboardData.porReemplazoOrganismo[organismo]) {
                    dashboardData.porReemplazoOrganismo[organismo] = { nombre: organismo, importe: 0, dias: 0 };
                }
                dashboardData.porReemplazoOrganismo[organismo].importe += costoLaboral;
                dashboardData.porReemplazoOrganismo[organismo].dias += dias;
            }
        })
        .on('end', () => {
            // Convertir objetos a arrays para facilitar el manejo en el frontend
            const response = {
                totales: dashboardData.totales,
                porPlanta: Object.values(dashboardData.porPlanta).sort((a, b) => b.importe - a.importe),
                porOrganismo: Object.values(dashboardData.porOrganismo).sort((a, b) => b.importe - a.importe),
                porArea: Object.values(dashboardData.porArea).sort((a, b) => b.importe - a.importe),
                porReemplazoNivel: Object.values(dashboardData.porReemplazoNivel).sort((a, b) => b.importe - a.importe),
                porReemplazoOrganismo: Object.values(dashboardData.porReemplazoOrganismo).sort((a, b) => b.importe - a.importe),
                porLibresDisponibilidades: Object.values(dashboardData.porLibresDisponibilidades).sort((a, b) => b.importe - a.importe),
                porGuardiasCriticas: Object.values(dashboardData.porGuardiasCriticas).sort((a, b) => b.importe - a.importe),
                porArea1: Object.values(dashboardData.porArea1).sort((a, b) => b.importe - a.importe),
                porArea3: Object.values(dashboardData.porArea3)
            };
            res.json(response);
        })
        .on('error', (error) => {
            res.status(500).json({ error: error.message });
        });
});

// --- ENDPOINTS DE CONFIGURACIÓN ---

// 1. Obtener columnas disponibles (LIB_0 o CG_0)
app.get('/api/config/available-columns', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    if (!fs.existsSync(csvPath)) return res.json([]);

    const results = [];
    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('headers', (headers) => {
            const prefixes = ['LIB_0', 'GC_0', 'LIB_D', 'LIB_N', 'LIB_SC', 'LIB_COB'];
            const filtered = headers.filter(h => prefixes.some(p => h.startsWith(p)));
            res.json(filtered);
            // Destruir el stream ya que solo necesitamos los headers
        })
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 2. Guardar configuración de Libres Disponibilidad
app.post('/api/config/save-ld', (req, res) => {
    const { columns } = req.body;
    if (!Array.isArray(columns)) return res.status(400).json({ error: 'Formato inválido' });

    try {
        const content = "columna\n" + columns.join('\n');
        fs.writeFileSync(LD_CONFIG_FILE, content, 'utf8');
        res.json({ success: true, message: 'Configuración guardada correctamente' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 3. Cargar configuración de Libres Disponibilidad
app.get('/api/config/load-ld', (req, res) => {
    if (!fs.existsSync(LD_CONFIG_FILE)) return res.json({ columns: [] });

    const results = [];
    fs.createReadStream(LD_CONFIG_FILE)
        .pipe(csv())
        .on('data', (data) => results.push(data.columna))
        .on('end', () => res.json({ columns: results }))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 4. Guardar configuración de Guardias Críticas
app.post('/api/config/save-gc', (req, res) => {
    const { columns } = req.body;
    if (!Array.isArray(columns)) return res.status(400).json({ error: 'Formato inválido' });

    try {
        const content = "columna\n" + columns.join('\n');
        fs.writeFileSync(GC_CONFIG_FILE, content, 'utf8');
        res.json({ success: true, message: 'Configuración de Guardias Críticas guardada' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 5. Cargar configuración de Guardias Críticas
app.get('/api/config/load-gc', (req, res) => {
    if (!fs.existsSync(GC_CONFIG_FILE)) return res.json({ columns: [] });

    const results = [];
    fs.createReadStream(GC_CONFIG_FILE)
        .pipe(csv())
        .on('data', (data) => results.push(data.columna))
        .on('end', () => res.json({ columns: results }))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// NUEVO ENDPOINT: Liquidaciones a observar por importes
app.get('/api/observar-importes', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    let filteredCount = 0;

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado para Observar Importes. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const totHab = parseFloat(row.TOT_HAB);
            const liquido = parseFloat(row.LIQUIDO);
            const sueldoMano = parseFloat(row.SUELDO_MANO);

            // FILTRO: TOT_HAB <=0 o LIQUIDO <= 0 o SUELDO_MANO <= 100000 o (SUELDO_MANO > 4000000.0 y PLANTA <> "Reemplazante no permanente-LD")
            const cumpleCriterio =
                (!isNaN(totHab) && totHab <= 0) ||
                (!isNaN(liquido) && liquido <= 0) ||
                (!isNaN(sueldoMano) && (
                    (sueldoMano <= 100000.0 && row.PLANTA !== "Reemplazante no permanente-LD") ||
                    (sueldoMano > 4000000.0 && row.PLANTA !== "Reemplazante no permanente-LD")
                ));

            if (cumpleCriterio) {
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });
                results.push(projectedRow);
                filteredCount++;
            }
        })
        .on('end', () => {
            console.log(`[SERVER] ✅ Proceso de Observar Importes completo. Registros filtrados y enviados: ${filteredCount}.`);
            res.json(results);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error en el proceso de Observar Importes:', error.message);
            res.status(500).json({ error: `Error interno al procesar el archivo CSV para Observar Importes: ${error.message}` });
        });
});

// 6. Reporte Reemplazos
app.get('/api/reemplazos', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];

    if (!fs.existsSync(csvPath)) return res.status(404).json({ error: 'Archivo no encontrado' });

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            if (row.PLANTA && row.PLANTA === "Reemplazante no permanente") {
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });
                results.push(projectedRow);
            }
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 7. Reporte LD Liquidacion (Filtrado por columnas configuradas en LD_config.csv)
app.get('/api/ld-liquidacion', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    if (!fs.existsSync(csvPath)) return res.status(404).json({ error: 'Archivo unificado no encontrado' });

    // 1. Cargar configuración de LD
    const configColumns = [];
    if (fs.existsSync(LD_CONFIG_FILE)) {
        const content = fs.readFileSync(LD_CONFIG_FILE, 'utf8').split('\n');
        content.shift(); // Quitar encabezado "columna"
        content.forEach(line => { if (line.trim()) configColumns.push(line.trim()); });
    }

    if (configColumns.length === 0) return res.json([]);

    const results = [];
    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            // Verificar si alguna de las columnas configuradas tiene valor != 0
            const hasValue = configColumns.some(col => {
                const val = parseFloat(row[col]);
                return !isNaN(val) && val !== 0;
            });

            if (hasValue) {
                const projectedRow = {};
                // Copiar campos estándar
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });
                // Inyectar columnas de configuración al final (después de Area2)
                configColumns.forEach(col => {
                    projectedRow[col] = row[col] ?? '0';
                });
                results.push(projectedRow);
            }
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 8. Reporte GC Liquidacion (Filtrado por columnas configuradas en GC_config.csv)
app.get('/api/gc-liquidacion', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    if (!fs.existsSync(csvPath)) return res.status(404).json({ error: 'Archivo unificado no encontrado' });

    // 1. Cargar configuración de GC
    const configColumns = [];
    if (fs.existsSync(GC_CONFIG_FILE)) {
        const content = fs.readFileSync(GC_CONFIG_FILE, 'utf8').split('\n');
        content.shift(); // Quitar encabezado "columna"
        content.forEach(line => { if (line.trim()) configColumns.push(line.trim()); });
    }

    if (configColumns.length === 0) return res.json([]);

    const results = [];
    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            // Verificar si alguna de las columnas configuradas tiene valor != 0
            const hasValue = configColumns.some(col => {
                const val = parseFloat(row[col]);
                return !isNaN(val) && val !== 0;
            });

            if (hasValue) {
                const projectedRow = {};
                // Copiar campos estándar
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });
                // Inyectar columnas de configuración al final (después de Area2)
                configColumns.forEach(col => {
                    projectedRow[col] = row[col] ?? '0';
                });
                results.push(projectedRow);
            }
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// server.js

app.get('/api/preparar-acumulado', (req, res) => {
    const inputPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const outputPath = path.join(CSV_UNIDOS_DIR, 'AcApJub.csv');
    const resultados = [];

    console.log("🛠️ Iniciando creación de AcApJub.csv...");

    if (!fs.existsSync(inputPath)) {
        return res.status(404).json({ success: false, message: "No existe el archivo unificado base." });
    }

    const stream = fs.createReadStream(inputPath).pipe(csv());

    stream.on('data', (row) => {
        // 1. Crear el nuevo objeto con la columna nueva al inicio
        const nuevoRegistro = {
            'Tope_Des_ap_jub': "0.0"
        };

        // 2. Agregar solo las columnas de CAMPOS_LIQUIDACION
        CAMPOS_LIQUIDACION.forEach(campo => {
            nuevoRegistro[campo] = row[campo] || "";
        });

        resultados.push(nuevoRegistro);
    });

    stream.on('end', () => {
        // 3. Crear el contenido CSV para guardar el archivo físicamente
        const encabezados = Object.keys(resultados[0]).join(',');
        const filas = resultados.map(r => Object.values(r).join(',')).join('\n');
        const contenidoCompleto = encabezados + '\n' + filas;

        try {
            fs.writeFileSync(outputPath, contenidoCompleto, 'utf8');
            console.log("✅ Archivo AcApJub.csv creado exitosamente en carpeta csv-unidos");

            // 4. Enviar los datos al frontend para visualización
            res.json(resultados);
        } catch (err) {
            console.error("❌ Error al escribir AcApJub.csv:", err);
            res.status(500).send("Error al guardar el archivo.");
        }
    });

    stream.on('error', (err) => {
        res.status(500).json({ success: false, message: err.message });
    });
});

// Endpoint para el procesamiento
app.post('/api/process', upload.array('excelFiles'), async (req, res) => {

    // El frontend nos dirá si debemos usar la carpeta por defecto o los archivos subidos
    const processMode = req.body.processMode;
    const files = req.files; // Archivos subidos por Multer

    let excelFilesToProcess = [];
    let filesToCleanup = [];

    try {
        if (processMode === 'default') {
            // Modo por defecto: Leer el contenido de la carpeta 'excel-a-convertir'
            excelFilesToProcess = fs.readdirSync(EXCEL_DIR)
                .filter(file => file.match(/\.(xlsx|xls)$/i));

            if (excelFilesToProcess.length === 0) {
                return res.status(400).json({ success: false, message: "No se encontraron archivos en la carpeta por defecto." });
            }
        } else if (processMode === 'upload') {
            // Modo Carga: Usar los archivos subidos
            if (!files || files.length === 0) {
                return res.status(400).json({ success: false, message: "No se cargó ningún archivo." });
            }

            // Mover los archivos cargados (desde 'uploads/') a la carpeta de entrada 'excel-a-convertir'
            files.forEach(file => {
                const tempPath = path.join(__dirname, file.path);
                const targetPath = path.join(EXCEL_DIR, file.originalname);

                // Mover el archivo subido a la carpeta de entrada del conversor
                fs.renameSync(tempPath, targetPath);

                excelFilesToProcess.push(file.originalname);
                filesToCleanup.push(targetPath); // Lista de archivos a borrar después del proceso si es necesario
            });
        }

        // Ejecutar la lógica ETL centralizada
        const result = await ejecutarProcesoETL(excelFilesToProcess);

        // Limpieza: Aunque los archivos ya están en EXCEL_DIR, para el modo 'upload' 
        // podríamos querer eliminarlos después de la unificación para evitar repetición.
        // MANTENDREMOS los archivos en la carpeta 'excel-a-convertir' para que la lógica 
        // de lotes no falle en la próxima corrida.

        res.json(result);

    } catch (error) {
        console.error('Error en el endpoint /api/process:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor: ' + error.message });
    }
});


app.listen(PORT, '0.0.0.0', () => {
    console.log(`Servidor de la Interfaz Simple ejecutándose en el puerto ${PORT}`);
    console.log(`Para acceder desde otro dispositivo en la red, utiliza la IP de esta PC.`);
});