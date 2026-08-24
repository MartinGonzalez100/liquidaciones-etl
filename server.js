// server.js (Nuevo punto de entrada)

const express = require('express');
const multer = require('multer');
const path = require('path');
const fs = require('fs');
// server.js (Añade esta línea de importación al principio con las demás)
const csv = require('csv-parser');
// ...
const { ejecutarProcesoETL } = require('./etl_runner'); // El runner que creamos
const { ejecutarProcesoNovedades } = require('./etl_novedades_runner');
//-------------
const app = express();
const PORT = 3000;
const EXCEL_DIR = path.join(process.cwd(), 'excel-a-convertir');
const CSV_UNIDOS_DIR = path.join(process.cwd(), 'csv-unidos'); // Directorio del CSV final
const FINAL_CSV_NAME = 'liquidaciones_unificadas.csv';

const EXCEL_NOVEDADES_DIR = path.join(process.cwd(), 'excel-a-convertir-novedades');
const CSV_UNIDOS_NOVEDADES_DIR = path.join(process.cwd(), 'csv-unidos-novedades');

const CONFIG_DIR = path.join(process.cwd(), 'configuracion_parametros');
const LD_CONFIG_FILE = path.join(CONFIG_DIR, 'LD_config.csv');
const GC_CONFIG_FILE = path.join(CONFIG_DIR, 'GC_config.csv');
const GC_EFECTORES_CONFIG_FILE = path.join(CONFIG_DIR, 'GC_config_efectores.csv');
const GC_CODIGOS_IMPORTES_CONFIG_FILE = path.join(CONFIG_DIR, 'GC_config_codigos_importes.csv');
const GC_SDYF_CONFIG_FILE = path.join(CONFIG_DIR, 'GC_config_sdyf.csv');
const ASIG_A3_CONFIG_FILE = path.join(CONFIG_DIR, 'Asig_A3_config_agentes_base.csv');

//-----------

// Crear carpetas necesarias
if (!fs.existsSync(EXCEL_DIR)) fs.mkdirSync(EXCEL_DIR);
if (!fs.existsSync(CSV_UNIDOS_DIR)) fs.mkdirSync(CSV_UNIDOS_DIR);
if (!fs.existsSync(CONFIG_DIR)) fs.mkdirSync(CONFIG_DIR);

if (!fs.existsSync(EXCEL_NOVEDADES_DIR)) fs.mkdirSync(EXCEL_NOVEDADES_DIR);
if (!fs.existsSync(CSV_UNIDOS_NOVEDADES_DIR)) fs.mkdirSync(CSV_UNIDOS_NOVEDADES_DIR);

// Configuración de Multer: Al subir archivos, los guardamos temporalmente.
const upload = multer({ dest: 'uploads/' });

// Middleware para servir archivos estáticos (la interfaz HTML)
app.use(express.static(path.join(process.cwd(), 'public')));
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
    'ANTIGUEDAD',
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
            // Ya no se filtra por PERIODO_IMPUTADO === PERIODO_LIQUIDADO
            // 2. PROYECCIÓN DE COLUMNAS: Crear un nuevo objeto solo con los campos deseados
            const projectedRow = {};
            CAMPOS_LIQUIDACION.forEach(field => {
                // Usamos el operador nullish ?? para manejar casos donde el campo podría no existir.
                projectedRow[field] = row[field] ?? '';
            });

            results.push(projectedRow);
            filteredCount++;
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

// NUEVO ENDPOINT: Unicos de Planta
app.get('/api/unicos-planta', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    const plantasValidas = [
        'Permanente Interino', 
        'Permanente Titular', 
        'Transitorios',
        'Residentes',
        'Residentes Nacionales',
        'RetVol2024-Permanente Titular',
        'RetVol2024-Permanente Interino'
    ];
    
    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const planta = row.PLANTA ? row.PLANTA.trim() : '';
            const dTrab = parseFloat(row.D_TRAB);
            const pImputado = row.PERIODO_IMPUTADO ? row.PERIODO_IMPUTADO.trim() : '';
            const pLiquidado = row.PERIODO_LIQUIDADO ? row.PERIODO_LIQUIDADO.trim() : '';

            // 1. (PLANTA= Permanente Interino O Permanente titular O Transitorios)
            // 2. (D_TRAB >0)
            // 3. (PERIODO_IMPUTADO=PERIODO_LIQUIDADO)
            if (plantasValidas.includes(planta) && !isNaN(dTrab) && dTrab > 0 && pImputado === pLiquidado) {
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    projectedRow[field] = row[field] ?? '';
                });
                results.push(projectedRow);
            }
        })
        .on('end', () => {
            // 4. Ordenar por NRO_DOCUMENTO ASC, y NUMERO_CARGO DESC
            results.sort((a, b) => {
                const docA = String(a.NRO_DOCUMENTO).padStart(15, '0');
                const docB = String(b.NRO_DOCUMENTO).padStart(15, '0');
                if (docA < docB) return -1;
                if (docA > docB) return 1;
                
                const cargoA = parseInt(a.NUMERO_CARGO) || 0;
                const cargoB = parseInt(b.NUMERO_CARGO) || 0;
                return cargoB - cargoA; // DESC
            });

            // 5. Dejar valores unicos tomando en cuenta NRO_DOCUMENTO (tomaremos el primero)
            const unicos = [];
            const dnisVistos = new Set();
            for (const row of results) {
                if (!dnisVistos.has(row.NRO_DOCUMENTO)) {
                    dnisVistos.add(row.NRO_DOCUMENTO);
                    unicos.push(row);
                }
            }

            console.log(`[SERVER] ✅ Proceso Unicos de Planta completo. Registros filtrados y enviados: ${unicos.length}.`);
            res.json(unicos);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error en el proceso de Unicos de Planta:', error.message);
            res.status(500).json({ error: `Error interno: ${error.message}` });
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
                projectedRow['LIB_003_31'] = row['LIB_003_31'] ?? '';
                projectedRow['LIB_003_91'] = row['LIB_003_91'] ?? '';

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
        });
});

// NUEVO ENDPOINT: Calculo de ley 100%
app.get('/api/calculo-ley100', (req, res) => {
    const { antiguedad, edad_f, edad_m, fecha_calculo } = req.query;
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    const groupedByDni = new Map();

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    if (!antiguedad || !edad_f || !edad_m || !fecha_calculo) {
        return res.status(400).json({ error: 'Faltan parámetros requeridos.' });
    }

    const [fcYear, fcMonth, fcDay] = fecha_calculo.includes('-')
        ? fecha_calculo.split('-')
        : fecha_calculo.split('/').reverse();
    const targetDate = new Date(fcYear, fcMonth - 1, fcDay);

    function calculateAge(birthDateStr, targetDateObj) {
        if (!birthDateStr) return -1;
        let parts = birthDateStr.split('/');
        if (parts.length !== 3) {
            if (birthDateStr.includes('-')) {
                parts = birthDateStr.split('-');
                const bDate = new Date(parts[0], parts[1] - 1, parts[2]);
                let age = targetDateObj.getFullYear() - bDate.getFullYear();
                let m = targetDateObj.getMonth() - bDate.getMonth();
                if (m < 0 || (m === 0 && targetDateObj.getDate() < bDate.getDate())) age--;
                return age;
            }
            return -1;
        }
        let bDate = new Date(parts[2], parts[1] - 1, parts[0]);
        let age = targetDateObj.getFullYear() - bDate.getFullYear();
        let m = targetDateObj.getMonth() - bDate.getMonth();
        if (m < 0 || (m === 0 && targetDateObj.getDate() < bDate.getDate())) age--;
        return age;
    }

    const minAntiguedad = parseInt(antiguedad, 10);
    const minEdadF = parseInt(edad_f, 10);
    const minEdadM = parseInt(edad_m, 10);

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const area2 = row.Area2 ? row.Area2.trim() : '';
            if (area2 !== 'A1' && area2 !== 'A26' && area2 !== 'A27') {
                return;
            }

            const pImputado = row.PERIODO_IMPUTADO ? row.PERIODO_IMPUTADO.trim() : '';
            const pLiquidado = row.PERIODO_LIQUIDADO ? row.PERIODO_LIQUIDADO.trim() : '';
            if (pImputado !== pLiquidado) return;

            const planta = row.PLANTA ? row.PLANTA.trim() : '';
            if (planta === 'Reemplazante no permanente-LD' || planta === 'Reemplazante no permanente') return;

            const ap100 = parseFloat(row.AP100_090_54);
            if (isNaN(ap100) || ap100 > 0) return;

            const antiguedadRow = parseInt(row.ANTIGUEDAD, 10) || 0;
            if (antiguedadRow < minAntiguedad) return;

            const sexo = row.SEXO ? row.SEXO.trim().toUpperCase() : '';
            const edad = calculateAge(row.FECHA_NACIMIENTO, targetDate);
            if (edad === -1) return;

            if (sexo === 'F' && edad < minEdadF) return;
            if (sexo === 'M' && edad < minEdadM) return;
            if (sexo !== 'F' && sexo !== 'M') return;

            const projectedRow = {};
            CAMPOS_LIQUIDACION.forEach(field => {
                projectedRow[field] = row[field] ?? '';
            });

            const dni = row.NRO_DOCUMENTO;
            const cargo = parseInt(row.NUMERO_CARGO, 10) || 0;

            if (!groupedByDni.has(dni)) {
                groupedByDni.set(dni, { cargo, record: projectedRow });
            } else {
                const existing = groupedByDni.get(dni);
                if (cargo > existing.cargo) {
                    groupedByDni.set(dni, { cargo, record: projectedRow });
                }
            }
        })
        .on('end', () => {
            for (const item of groupedByDni.values()) {
                results.push(item.record);
            }
            res.json(results);
        })
        .on('error', (error) => {
            res.status(500).json({ error: error.message });
        });
});

// NUEVO ENDPOINT: Asignaciones Familiares
app.get('/api/asignaciones-familiares', (req, res) => {
    const isReempOnly = req.query.tipo === 'reemp';
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado para Asignaciones Familiares. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado' });
    }

    const getPreviousMonth = (periodoStr) => {
        if (!periodoStr || typeof periodoStr !== 'string' || !periodoStr.includes('/')) return periodoStr;
        const parts = periodoStr.split('/');
        if (parts.length !== 3) return periodoStr;
        
        const day = parts[0];
        let month = parseInt(parts[1], 10);
        let year = parseInt(parts[2], 10);
        
        if (month === 1) {
            month = 12;
            year -= 1;
        } else {
            month -= 1;
        }
        
        return `${day}/${month.toString().padStart(2, '0')}/${year}`;
    };

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const asigFam = parseFloat(row[' ASIG_FAM '] || row['ASIG_FAM']) || 0;
            const dTrab = parseFloat(row.D_TRAB) || 0;
            const planta = (row.PLANTA || '').trim();
            const periodoImputado = (row.PERIODO_IMPUTADO || '').trim();
            const periodoLiquidado = (row.PERIODO_LIQUIDADO || '').trim();

            if (asigFam !== 0 && planta !== 'Reemplazante no permanente-LD' && dTrab > 0) {
                if (isReempOnly && planta !== 'Reemplazante no permanente') return;

                if (planta === 'Reemplazante no permanente') {
                    if (periodoImputado === periodoLiquidado || periodoImputado === getPreviousMonth(periodoLiquidado)) {
                        results.push(row);
                    }
                } else {
                    if (periodoImputado === periodoLiquidado) {
                        results.push(row);
                    }
                }
            }
        })
        .on('end', () => {
            res.json(results);
        })
        .on('error', (error) => {
            res.status(500).json({ error: error.message });
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
        porAreaPlanta: {},
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

            // Acumular por Area x Planta
            if (!dashboardData.porAreaPlanta[area]) {
                dashboardData.porAreaPlanta[area] = {};
            }
            if (!dashboardData.porAreaPlanta[area][planta]) {
                dashboardData.porAreaPlanta[area][planta] = { nombre: planta, cantidad: 0, importe: 0 };
            }
            dashboardData.porAreaPlanta[area][planta].cantidad++;
            dashboardData.porAreaPlanta[area][planta].importe += costoLaboral;

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
            // Aplanar y ordenar porAreaPlanta
            let porAreaPlantaFlat = [];
            for (let a in dashboardData.porAreaPlanta) {
                for (let p in dashboardData.porAreaPlanta[a]) {
                    porAreaPlantaFlat.push({ area: a, planta: p, ...dashboardData.porAreaPlanta[a][p] });
                }
            }
            porAreaPlantaFlat.sort((a, b) => b.importe - a.importe);

            // Convertir objetos a arrays para facilitar el manejo en el frontend
            const response = {
                totales: dashboardData.totales,
                porPlanta: Object.values(dashboardData.porPlanta).sort((a, b) => b.importe - a.importe),
                porOrganismo: Object.values(dashboardData.porOrganismo).sort((a, b) => b.importe - a.importe),
                porArea: Object.values(dashboardData.porArea).sort((a, b) => b.importe - a.importe),
                porAreaPlanta: porAreaPlantaFlat,
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
            const prefixes = ['LIB_0', 'GC_0', 'LIB_D', 'LIB_N', 'LIB_SC', 'LIB_COB', 'LIB_B'];
            const filtered = headers.filter(h => prefixes.some(p => h.startsWith(p)));
            res.json(filtered);
            // Destruir el stream ya que solo necesitamos los headers
        })
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// Función para limpiar todas las planillas auxiliares que dependen de configuración
function limpiarPlanillasAuxiliares() {
    try {
        const archivos = [
            path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxGcNovedades.csv'),
            path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv'),
            path.join(CSV_UNIDOS_DIR, 'AuxResidentesLiquidacion.csv')
        ];
        archivos.forEach(p => {
            if (fs.existsSync(p)) fs.unlinkSync(p);
        });
        console.log('[SERVER] 🧹 Planillas auxiliares borradas tras actualización de configuración.');
    } catch (e) {
        console.error('[SERVER] ⚠️ Error al borrar planillas auxiliares:', e.message);
    }
}

// 2. Guardar configuración de Libres Disponibilidad
app.post('/api/config/save-ld', (req, res) => {
    const { columns } = req.body;
    if (!Array.isArray(columns)) return res.status(400).json({ error: 'Formato inválido' });

    try {
        const content = "columna\n" + columns.join('\n');
        fs.writeFileSync(LD_CONFIG_FILE, content, 'utf8');
        limpiarPlanillasAuxiliares();
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
        limpiarPlanillasAuxiliares();
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

// 6. Cargar configuración de Guardias Críticas Efectores
app.get('/api/config/load-gc-efectores', (req, res) => {
    if (!fs.existsSync(GC_EFECTORES_CONFIG_FILE)) return res.json([]);

    const results = [];
    fs.createReadStream(GC_EFECTORES_CONFIG_FILE, { encoding: 'utf8' })
        .pipe(csv())
        .on('data', (data) => {
            const efectorKey = data['EFECTORES'] !== undefined ? 'EFECTORES' : '\uFEFFEFECTORES';
            
            if (data[efectorKey] || data['NUEVOS EFECTORES']) {
                results.push({
                    efector: data[efectorKey] || '',
                    nuevoEfector: data['NUEVOS EFECTORES'] || ''
                });
            }
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 7. Guardar configuración de Guardias Críticas Efectores (ABM)
app.post('/api/config/save-gc-efectores', (req, res) => {
    const data = req.body;
    if (!Array.isArray(data)) return res.status(400).json({ error: 'Formato inválido. Debe ser un array.' });

    try {
        let content = 'EFECTORES,NUEVOS EFECTORES\n';
        data.forEach(row => {
            const efector = (row.efector || '').replace(/,/g, ' '); // Evitar romper el CSV
            const nuevoEfector = (row.nuevoEfector || '').replace(/,/g, ' ');
            content += `${efector},${nuevoEfector}\n`;
        });
        fs.writeFileSync(GC_EFECTORES_CONFIG_FILE, content, 'utf8');
        limpiarPlanillasAuxiliares();
        res.json({ success: true, message: 'Configuración de Efectores guardada correctamente' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 8. Cargar configuración de Códigos Guardias Críticas Importes
app.get('/api/config/load-gc-codigos-efectores', (req, res) => {
    if (!fs.existsSync(GC_CODIGOS_IMPORTES_CONFIG_FILE)) return res.json([]);

    const results = [];
    fs.createReadStream(GC_CODIGOS_IMPORTES_CONFIG_FILE, { encoding: 'latin1' })
        .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
        .on('data', (data) => {
             results.push(data);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

function parseDateForSort(dateStr) {
    if (!dateStr) return new Date(8640000000000000); // max date if empty
    const parts = dateStr.split('/');
    if (parts.length === 3) return new Date(`${parts[2]}-${parts[1]}-${parts[0]}T00:00:00Z`);
    return new Date(8640000000000000);
}

function formatDateToDDMMYYYY(dateObj) {
    if (!dateObj || isNaN(dateObj)) return '';
    const d = String(dateObj.getUTCDate()).padStart(2, '0');
    const m = String(dateObj.getUTCMonth() + 1).padStart(2, '0');
    const y = dateObj.getUTCFullYear();
    return `${d}/${m}/${y}`;
}

// 9. Guardar configuración de Códigos Guardias Críticas Importes (ABM)
app.post('/api/config/save-gc-codigos-efectores', (req, res) => {
    let data = req.body;
    if (!Array.isArray(data)) return res.status(400).json({ error: 'Formato inválido. Debe ser un array.' });

    try {
        // Validación de Duplicados Exactos
        const seen = new Set();
        for (const row of data) {
            const hash = `${row.CLAVEUNICA}|${row.DIAS}|${row.TIPODEGUARDIA}|${row.NIVEL}|${row.CODIGO}|${row.IMPORTE}|${row.ACTIVO_DESDE}|${row.ACTIVO_HASTA}`;
            if (seen.has(hash)) {
                return res.status(400).json({ error: 'Existe un registro igual, cambiar algun parametro' });
            }
            seen.add(hash);
        }

        // Cierre Automático de Vigencias Anteriores
        const groups = {};
        data.forEach(row => {
            const key = row.CLAVEUNICA || '';
            if (!groups[key]) groups[key] = [];
            groups[key].push(row);
        });

        for (const key in groups) {
            const group = groups[key];
            // Ordenar por ACTIVO_DESDE ascendente para ajustar ACTIVO_HASTA
            group.sort((a, b) => parseDateForSort(a.ACTIVO_DESDE) - parseDateForSort(b.ACTIVO_DESDE));
            
            for (let i = 0; i < group.length - 1; i++) {
                const current = group[i];
                const next = group[i+1];
                const nextDate = parseDateForSort(next.ACTIVO_DESDE);
                
                // Restar 1 día
                const hastaDate = new Date(nextDate.getTime() - 24 * 60 * 60 * 1000);
                current.ACTIVO_HASTA = formatDateToDDMMYYYY(hastaDate);
            }
        }

        data = [];
        for (const key in groups) {
            data.push(...groups[key]);
        }

        // Ordenamiento Final: ACTIVO_DESDE (desc) y CODIGO (asc)
        data.sort((a, b) => {
            const dateDiff = parseDateForSort(b.ACTIVO_DESDE) - parseDateForSort(a.ACTIVO_DESDE);
            if (dateDiff !== 0) return dateDiff;
            const codeA = (a.CODIGO || '').toString();
            const codeB = (b.CODIGO || '').toString();
            return codeA.localeCompare(codeB);
        });

        let content = 'CLAVEUNICA;DIAS;TIPO DE GUARDIA;NIVEL;CODIGO;IMPORTE;ACTIVO_DESDE;ACTIVO_HASTA\n';
        data.forEach(row => {
            const claveUnica = (row.CLAVEUNICA || '').replace(/;/g, ' ');
            const dias = (row.DIAS || '').replace(/;/g, ' ');
            const tipo = (row.TIPODEGUARDIA || '').replace(/;/g, ' ');
            const nivel = (row.NIVEL || '').replace(/;/g, ' ');
            const codigo = (row.CODIGO || '').replace(/;/g, ' ');
            const importe = (row.IMPORTE || '').replace(/;/g, ' ');
            const activoDesde = (row.ACTIVO_DESDE || '01/07/2026').replace(/;/g, ' ');
            const activoHasta = (row.ACTIVO_HASTA || '').replace(/;/g, ' ');
            
            content += `${claveUnica};${dias};${tipo};${nivel};${codigo};${importe};${activoDesde};${activoHasta}\n`;
        });
        
        fs.writeFileSync(GC_CODIGOS_IMPORTES_CONFIG_FILE, content, 'latin1');
        limpiarPlanillasAuxiliares();
        res.json({ success: true, message: 'Configuración de Códigos guardada correctamente' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 10. Cargar configuración de Guardias Críticas Fines de Semanas y Feriados (SDYF)
app.get('/api/config/load-gc-sdyf', (req, res) => {
    if (!fs.existsSync(GC_SDYF_CONFIG_FILE)) return res.json([]);

    const results = [];
    fs.createReadStream(GC_SDYF_CONFIG_FILE, { encoding: 'utf8' })
        .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
        .on('data', (data) => {
             results.push(data);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 11. Guardar configuración de SDYF
app.post('/api/config/save-gc-sdyf', (req, res) => {
    const data = req.body;
    if (!Array.isArray(data)) return res.status(400).json({ error: 'Formato inválido. Debe ser un array.' });

    try {
        let content = 'DIA;SDYF\n';
        data.forEach(row => {
            const dia = (row.DIA || '').replace(/;/g, ' ');
            const sdyf = (row.SDYF || '').replace(/;/g, ' ');
            content += `${dia};${sdyf}\n`;
        });
        fs.writeFileSync(GC_SDYF_CONFIG_FILE, content, 'utf8');
        limpiarPlanillasAuxiliares();
        res.json({ success: true, message: 'Configuración de Fines de Semanas y Feriados guardada' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
});

// 12. Cargar configuración de Asignaciones A3
app.get('/api/config/load-asig-a3', (req, res) => {
    if (!fs.existsSync(ASIG_A3_CONFIG_FILE)) return res.json([]);

    const results = [];
    fs.createReadStream(ASIG_A3_CONFIG_FILE, { encoding: 'latin1' })
        .pipe(csv({ mapHeaders: ({ header }) => header.trim() }))
        .on('data', (data) => {
             results.push(data);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// 13. Guardar configuración de Asignaciones A3
app.post('/api/config/save-asig-a3', (req, res) => {
    const data = req.body;
    if (!Array.isArray(data)) return res.status(400).json({ error: 'Formato inválido. Debe ser un array.' });

    try {
        let content = 'DESCAGENTE,NRO_DOCUMENTO, ASIG_FAM ,PLANTA,ORGANISMO\n';
        data.forEach(row => {
            const desc = (row.DESCAGENTE || '').replace(/,/g, ' ');
            const nro = (row.NRO_DOCUMENTO || '').replace(/,/g, ' ');
            const asig = (row[' ASIG_FAM '] || row.ASIG_FAM || '').replace(/,/g, ' ');
            const planta = (row.PLANTA || '').replace(/,/g, ' ');
            const org = (row.ORGANISMO || '').replace(/,/g, ' ');
            content += `${desc},${nro},${asig},${planta},${org}\n`;
        });
        fs.writeFileSync(ASIG_A3_CONFIG_FILE, content, 'latin1');
        limpiarPlanillasAuxiliares();
        res.json({ success: true, message: 'Configuración de Asignaciones A3 guardada correctamente' });
    } catch (err) {
        res.status(500).json({ error: err.message });
    }
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
            const liquidoLey7991 = parseFloat(row.LIQUIDO_LEY7991);
            const apJubPer = parseFloat(row.ApJubPer);
            const obSocPer = parseFloat(row.ObSocPer);

            // CONDICIONES DE ANOMALIAS PARA OBSERVAR (SÓLO IMPORTES)
            const dTrab = parseFloat(row.D_TRAB);
            const condicionSueldoMano = !isNaN(sueldoMano) && !isNaN(dTrab) && dTrab > 15 && (sueldoMano < 50000.00 || sueldoMano > 5000000.00);
            const condicionLey7991 = !isNaN(liquidoLey7991) && liquidoLey7991 < 0;
            const condicionApJubObSoc = (!isNaN(apJubPer) && apJubPer < 0) || (!isNaN(obSocPer) && obSocPer < 0);

            const cumpleCriterio =
                (!isNaN(totHab) && totHab <= 0) ||
                (!isNaN(liquido) && liquido <= 0) ||
                condicionSueldoMano ||
                condicionLey7991 ||
                condicionApJubObSoc;

            if (cumpleCriterio) {
                const projectedRow = {};
                // Agregamos primero los campos normales menos ApJubPer
                CAMPOS_LIQUIDACION.forEach(field => {
                    if (field !== 'ApJubPer') {
                        projectedRow[field] = row[field] ?? '';
                    }
                });
                // Inyectamos las columnas al final, luego de Area2
                projectedRow['ApJubPer'] = row['ApJubPer'] ?? '';
                projectedRow['ObSocPer'] = row['ObSocPer'] ?? '';

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

// NUEVO ENDPOINT: Liquidaciones a observar por Planta
app.get('/api/observar-planta', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const results = [];
    let filteredCount = 0;

    if (!fs.existsSync(csvPath)) {
        console.error('[SERVER] ❌ Archivo CSV unificado no encontrado para Observar Planta. Enviando 404.');
        return res.status(404).json({ error: 'Archivo CSV unificado no encontrado. Ejecute primero la Conversión.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const area2 = row.Area2 ? row.Area2.trim() : '';
            const planta = row.PLANTA ? row.PLANTA.trim() : '';

            // CONDICIONES DE ANOMALIAS PARA OBSERVAR (SÓLO PLANTA)
            const condicionAreaA1_Reemp = area2 === 'A1' && planta === 'Reemplazante no permanente';
            const condicionAreaA1_ReempLD = area2 === 'A1' && planta === 'Reemplazante no permanente-LD';
            const plantasA3 = [
                'Transitorios',
                'Permanente Interino',
                'Permanente Titular',
                'Residentes',
                'Residentes Nacionales'
            ];
            const condicionAreaA3 = area2 === 'A3' && plantasA3.includes(planta);

            const cumpleCriterio = condicionAreaA1_Reemp || condicionAreaA1_ReempLD || condicionAreaA3;

            if (cumpleCriterio) {
                const projectedRow = {};
                CAMPOS_LIQUIDACION.forEach(field => {
                    if (field !== 'ApJubPer') {
                        projectedRow[field] = row[field] ?? '';
                    }
                });
                projectedRow['ApJubPer'] = row['ApJubPer'] ?? '';
                projectedRow['ObSocPer'] = row['ObSocPer'] ?? '';

                results.push(projectedRow);
                filteredCount++;
            }
        })
        .on('end', () => {
            console.log(`[SERVER] ✅ Proceso de Observar Planta completo. Registros filtrados y enviados: ${filteredCount}.`);
            res.json(results);
        })
        .on('error', (error) => {
            console.error('[SERVER] ❌ Error en el proceso de Observar Planta:', error.message);
            res.status(500).json({ error: `Error interno al procesar el archivo CSV para Observar Planta: ${error.message}` });
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
// Funciones auxiliares para LD Liquidacion
function writeCsvFile(filePath, headers, data) {
    return new Promise((resolve, reject) => {
        try {
            const stream = fs.createWriteStream(filePath);
            stream.write(headers.map(h => `"${h}"`).join(',') + '\n');
            for (const row of data) {
                const line = headers.map(h => {
                    let val = row[h] !== undefined && row[h] !== null ? String(row[h]) : '';
                    if (val.includes('"') || val.includes(',')) {
                        val = `"${val.replace(/"/g, '""')}"`;
                    }
                    return val;
                }).join(',');
                stream.write(line + '\n');
            }
            stream.end();
            stream.on('finish', resolve);
            stream.on('error', reject);
        } catch(e) {
            reject(e);
        }
    });
}

async function ensureAuxLDLiquidacionFiles() {
    const auxLDPath = path.join(CSV_UNIDOS_DIR, 'AuxLDLiquidacion.csv');
    const auxLDCodigosPath = path.join(CSV_UNIDOS_DIR, 'AuxLDLiquidacionCodigos.csv');
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    
    if (fs.existsSync(auxLDPath) && fs.existsSync(auxLDCodigosPath)) {
        return true;
    }
    if (!fs.existsSync(csvPath)) return false;

    // Load LD config
    const configColumns = [];
    if (fs.existsSync(LD_CONFIG_FILE)) {
        const content = fs.readFileSync(LD_CONFIG_FILE, 'utf8').split('\n');
        content.shift(); // Quitar encabezado "columna"
        content.forEach(line => { if (line.trim()) configColumns.push(line.trim()); });
    }
    if (configColumns.length === 0) return false;

    const results = [];
    await new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                const hasValue = configColumns.some(col => {
                    const val = parseFloat(row[col]);
                    return !isNaN(val) && val !== 0;
                });

                if (hasValue) {
                    const projectedRow = {};
                    CAMPOS_LIQUIDACION.forEach(field => {
                        projectedRow[field] = row[field] ?? '';
                    });
                    configColumns.forEach(col => {
                        projectedRow[col] = row[col] ?? '0';
                    });
                    results.push(projectedRow);
                }
            })
            .on('end', resolve)
            .on('error', reject);
    });

    // Ordenar results
    // NRO_DOCUMENTO ascendente (Numérico) y PERIODO_IMPUTADO mas reciente a menos reciente (descendente)
    results.sort((a, b) => {
        const docA = parseInt(a.NRO_DOCUMENTO) || 0;
        const docB = parseInt(b.NRO_DOCUMENTO) || 0;
        if (docA !== docB) {
            return docA - docB; // Ascendente numérico
        }
        
        const perA = a.PERIODO_IMPUTADO || '';
        const perB = b.PERIODO_IMPUTADO || '';
        if (perA !== perB) {
            return perB.localeCompare(perA); // Descendente
        }
        return 0;
    });

    // Escribir AuxLDLiquidacion.csv
    const headersAuxLD = results.length > 0 ? Object.keys(results[0]) : [];
    if (headersAuxLD.length > 0) {
        await writeCsvFile(auxLDPath, headersAuxLD, results);
    } else {
        fs.writeFileSync(auxLDPath, "");
    }

    // Generar codigos
    const codigosResults = [];
    let currentDoc = null;
    let numero_ld_agente = 1;

    for (const row of results) {
        if (row.NRO_DOCUMENTO !== currentDoc) {
            currentDoc = row.NRO_DOCUMENTO;
            numero_ld_agente = 1;
        }

        let numero_Copia = 1;
        for (const col of Object.keys(row)) {
            if (col.includes('003_') || col.includes('031') || col.includes('032') || col.includes('033')) {
                const cleanCol = col.replace(/\s+/g, '');
                
                // Excepciones: estas columnas no generan registro por sí solas
                if (cleanCol === 'LIB_032_34' || cleanCol === 'LIB_033_34') {
                    continue;
                }

                let val = parseFloat(row[col]) || 0;

                // Sumar importes según excepciones
                if (cleanCol === 'LIB_003_34') {
                    const col32 = Object.keys(row).find(k => k.replace(/\s+/g, '') === 'LIB_032_34');
                    if (col32) {
                        val += (parseFloat(row[col32]) || 0);
                    }
                } else if (cleanCol === 'LIB_031_34') {
                    const col33 = Object.keys(row).find(k => k.replace(/\s+/g, '') === 'LIB_033_34');
                    if (col33) {
                        val += (parseFloat(row[col33]) || 0);
                    }
                }

                if (val > 0) {
                    const trimmedCol = col.replace(/\s+/g, '');
                    const last6 = trimmedCol.slice(-6).replace(/_/g, '-');
                    
                    const docStr = String(row.NRO_DOCUMENTO || '');
                    const orgStr = String(row.ORGANISMO || '');
                    const clave = `${docStr}${last6}${orgStr}${numero_ld_agente}`;

                    const newRow = {
                        clave: clave,
                        Codigo_Optimo: last6,
                        Importe_Optimo: Number(val.toFixed(2)),
                        numero_Copia: numero_Copia,
                        numero_ld_agente: numero_ld_agente,
                        ...row
                    };
                    codigosResults.push(newRow);
                    numero_Copia++;
                    numero_ld_agente++;
                }
            }
        }
    }
    
    const headersCodigos = ['clave', 'Codigo_Optimo', 'Importe_Optimo', 'numero_Copia', 'numero_ld_agente', ...headersAuxLD];
    if (codigosResults.length > 0 || headersCodigos.length > 5) {
        await writeCsvFile(auxLDCodigosPath, headersCodigos, codigosResults);
    } else {
        fs.writeFileSync(auxLDCodigosPath, "");
    }
    
    return true;
}

// 7. Reporte LD Liquidacion (Filtrado por columnas configuradas en LD_config.csv)
app.get('/api/ld-liquidacion', async (req, res) => {
    try {
        const success = await ensureAuxLDLiquidacionFiles();
        if (!success) {
            return res.json([]);
        }

        const auxLDPath = path.join(CSV_UNIDOS_DIR, 'AuxLDLiquidacion.csv');
        const results = [];
        if (!fs.existsSync(auxLDPath)) return res.json([]);

        fs.createReadStream(auxLDPath)
            .pipe(csv())
            .on('data', (row) => results.push(row))
            .on('end', () => res.json(results))
            .on('error', (err) => res.status(500).json({ error: err.message }));
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});

// 7.1 Reporte LD Codigos Separados
app.get('/api/ld-codigos-separados', async (req, res) => {
    try {
        const success = await ensureAuxLDLiquidacionFiles();
        if (!success) {
            return res.json([]);
        }

        const auxLDCodigosPath = path.join(CSV_UNIDOS_DIR, 'AuxLDLiquidacionCodigos.csv');
        const results = [];
        if (!fs.existsSync(auxLDCodigosPath)) return res.json([]);

        fs.createReadStream(auxLDCodigosPath)
            .pipe(csv())
            .on('data', (row) => {
                if (row.Importe_Optimo !== undefined) {
                    row.Importe_Optimo = parseFloat(row.Importe_Optimo);
                }
                results.push(row);
            })
            .on('end', () => res.json(results))
            .on('error', (err) => res.status(500).json({ error: err.message }));
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
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
// NUEVOS ENDPOINTS PARA GC CONTROL

/**
 * Función auxiliar para generar AuxGCLiquidacion.csv
 * Se basa en las columnas configuradas en GC_config.csv y los datos de liquidaciones_unificadas.csv
 */
async function generateAuxGCLiquidacion() {
    const csvPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const auxPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');

    if (!fs.existsSync(csvPath)) throw new Error('Archivo unificado no encontrado');

    // 1. Cargar configuración de GC
    const configColumns = [];
    if (fs.existsSync(GC_CONFIG_FILE)) {
        const content = fs.readFileSync(GC_CONFIG_FILE, 'utf8').split('\n');
        content.shift();
        content.forEach(line => { if (line.trim()) configColumns.push(line.trim()); });
    }

    if (configColumns.length === 0) return false;

    const results = [];
    return new Promise((resolve, reject) => {
        fs.createReadStream(csvPath)
            .pipe(csv())
            .on('data', (row) => {
                // Reutilizamos la lógica de GC Liquidación para filtrar
                const hasValue = configColumns.some(col => {
                    const val = parseFloat(row[col]);
                    return !isNaN(val) && val !== 0;
                });

                if (hasValue) {
                    const newRow = {};
                    
                    // CLAVE_AGRUPACION: NRO_DOCUMENTO + ORGANISMO (sin espacios a la derecha)
                    const nroDoc = (row.NRO_DOCUMENTO || '').toString().trimEnd();
                    const organismo = (row.ORGANISMO || '').toString().trimEnd();
                    newRow['CLAVE_AGRUPACION'] = nroDoc + organismo;
                    newRow['OBSERVACION_EPS'] = '';

                    // SUMA_GC: Suma de las columnas de GC
                    let sumaGC = 0;
                    configColumns.forEach(col => {
                        const val = parseFloat(row[col]) || 0;
                        sumaGC += val;
                    });
                    newRow['SUMA_GC'] = sumaGC.toFixed(2);

                    // Copiamos los campos estándar de CAMPOS_LIQUIDACION
                    CAMPOS_LIQUIDACION.forEach(field => {
                        newRow[field] = row[field] ?? '';
                    });

                    // Agregamos las columnas de GC
                    configColumns.forEach(col => {
                        newRow[col] = row[col] ?? '0';
                    });

                    results.push(newRow);
                }
            })
            .on('end', () => {
                if (results.length === 0) {
                    resolve(false);
                    return;
                }
                // Escribir el nuevo CSV
                const headers = Object.keys(results[0]);
                const csvContent = [
                    headers.join(','),
                    ...results.map(r => headers.map(h => {
                        let val = (r[h] !== undefined && r[h] !== null) ? r[h].toString() : '';
                        if (val.includes(',') || val.includes('"')) val = `"${val.replace(/"/g, '""')}"`;
                        return val;
                    }).join(','))
                ].join('\n');

                fs.writeFileSync(auxPath, csvContent, 'utf8');
                console.log(`[SERVER] ✅ AuxGCLiquidacion.csv generado con ${results.length} registros.`);
                resolve(true);
            })
            .on('error', reject);
    });
}

function processGCControl(csvFile, filterFn, res) {
    const auxPath = path.join(CSV_UNIDOS_DIR, csvFile);
    if (!fs.existsSync(auxPath)) return res.status(404).json({ error: 'Archivo auxiliar no encontrado' });

    const groupings = new Map();

    fs.createReadStream(auxPath)
        .pipe(csv())
        .on('data', (row) => {
            if (filterFn(row)) {
                const clave = row.CLAVE_AGRUPACION;
                if (!groupings.has(clave)) {
                    groupings.set(clave, {
                        ORGANISMO: row.ORGANISMO,
                        NRO_DOCUMENTO: row.NRO_DOCUMENTO,
                        NIVEL: row.NIVEL,
                        NUMERO_LIQ: row.NUMERO_LIQ || '',
                        DESCAGENTE: row.DESCAGENTE,
                        PERIODO_IMPUTADO: row.PERIODO_IMPUTADO,
                        NUMERO_CARGO: row.NUMERO_CARGO,
                        PERIODO_LIQUIDADO: row.PERIODO_LIQUIDADO,
                        SUMA: parseFloat(row.SUMA_GC) || 0,
                        GC: '',
                        CLAVE: clave
                    });
                } else {
                    const existing = groupings.get(clave);
                    existing.SUMA += parseFloat(row.SUMA_GC) || 0;
                }
            }
        })
        .on('end', () => {
            const results = Array.from(groupings.values()).map(r => ({
                ...r,
                SUMA: r.SUMA.toFixed(2)
            }));
            res.json(results);
        })
        .on('error', (err) => res.status(500).json({ error: err.message }));
}

app.get('/api/gc-control', async (req, res) => {
    try {
        const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
        if (!fs.existsSync(auxLiqPath)) {
            await generateAuxGCLiquidacion();
        }
        processGCControl('AuxGCLiquidacion.csv', (row) => {
            const pImp = (row.PERIODO_IMPUTADO || '').trim();
            const pLiq = (row.PERIODO_LIQUIDADO || '').trim();
            return pImp === pLiq;
        }, res);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/gc-control-retro', async (req, res) => {
    try {
        const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
        if (!fs.existsSync(auxLiqPath)) {
            await generateAuxGCLiquidacion();
        }
        processGCControl('AuxGCLiquidacion.csv', (row) => {
            const pImp = (row.PERIODO_IMPUTADO || '').trim();
            const pLiq = (row.PERIODO_LIQUIDADO || '').trim();
            return pImp < pLiq;
        }, res);
    } catch (error) {
        res.status(500).json({ error: error.message });
    }
});

// server.js

app.get('/api/preparar-acumulado', (req, res) => {
    const inputPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const outputPathDetalle = path.join(CSV_UNIDOS_DIR, 'AcApJub.csv');
    const outputPathTope = path.join(CSV_UNIDOS_DIR, 'AcApJub_con_tope.csv');
    const resultados = [];
    const agrupadoPorDni = new Map();
    const dniOccurrences = new Map(); // Mapa para contar duplicados por DNI
    const tope = parseFloat(req.query.tope) || 0.0;

    console.log(`🛠️ Iniciando creación de reportes AcApJub con tope: $${tope}...`);

    if (!fs.existsSync(inputPath)) {
        return res.status(404).json({ success: false, message: "No existe el archivo unificado base." });
    }

    const stream = fs.createReadStream(inputPath).pipe(csv());

    stream.on('data', (row) => {
        let dni = row.NRO_DOCUMENTO ? row.NRO_DOCUMENTO.trim() : "";
        const count = (dniOccurrences.get(dni) || 0) + 1;
        dniOccurrences.set(dni, count);

        // --- 1. Preparar el detalle general (AcApJub.csv) ---
        const nuevoRegistro = {
            'Tope_Des_ap_jub': tope.toFixed(2),
            'AcumuladoApJub': '',
            'Dif_ApJub': '',
            'Enviar_A_Descontar_automatico': '',
            'Dif_Sobrante_Descuento': '',
            'duplicado': count,
            'ApJubPer': row['ApJubPer'] || "",
            'PLANTASS': row['PLANTA'] || "",
            'PERIODO_IMPUTADO': row['PERIODO_IMPUTADO'] || "",
            'PERIODO_LIQUIDADO': row['PERIODO_LIQUIDADO'] || "",
            'NUMERO_CARGO': row['NUMERO_CARGO'] || ""
        };
        const camposExtraList = ['ApJubPer', 'PLANTA', 'PERIODO_IMPUTADO', 'PERIODO_LIQUIDADO', 'NUMERO_CARGO'];
        CAMPOS_LIQUIDACION.forEach(campo => {
            if (!camposExtraList.includes(campo)) {
                nuevoRegistro[campo] = row[campo] || "";
            }
        });
        resultados.push(nuevoRegistro);

        // --- 2. Agrupar sumas para (AcApJub_con_tope.csv) ---
        dni = row.NRO_DOCUMENTO ? row.NRO_DOCUMENTO.trim() : "";
        const apJub = parseFloat(row.ApJubPer) || 0;
        
        if (dni) {
            if (!agrupadoPorDni.has(dni)) {
                agrupadoPorDni.set(dni, 0);
            }
            agrupadoPorDni.set(dni, agrupadoPorDni.get(dni) + apJub);
        }
    });

    stream.on('end', () => {
        try {
            // --- 3. Crear el primer archivo físico: AcApJub_con_tope.csv ---
            const filtrados = [];
            for (const [dni, suma] of agrupadoPorDni.entries()) {
                if (suma > tope) {
                    filtrados.push({
                        'NRO_DOCUMENTO': dni,
                        'Acumulado_Ap_Jub_169_00': suma.toFixed(2)
                    });
                }
            }

            let encabezadosTope = 'NRO_DOCUMENTO,Acumulado_Ap_Jub_169_00';
            let filasTope = filtrados.map(r => `${r.NRO_DOCUMENTO},${r.Acumulado_Ap_Jub_169_00}`).join('\n');
            let contenidoTope = filtrados.length > 0 ? encabezadosTope + '\n' + filasTope : encabezadosTope;
            
            fs.writeFileSync(outputPathTope, contenidoTope, 'utf8');
            console.log("✅ Archivo AcApJub_con_tope.csv creado exitosamente.");

            // --- 4. Crear el segundo archivo físico: AcApJub.csv ---
            if (resultados.length > 0) {
                // Rellenar AcumuladoApJub y Dif_ApJub
                resultados.forEach(r => {
                    const doc = r.NRO_DOCUMENTO ? r.NRO_DOCUMENTO.trim() : "";
                    const suma = agrupadoPorDni.get(doc) || 0;
                    if (suma > tope) {
                        r['AcumuladoApJub'] = suma.toFixed(2);
                        r['Dif_ApJub'] = (tope - suma).toFixed(2);
                    }
                });

                // ORDENAMIENTO DE LAS FILAS
                resultados.sort((a, b) => {
                    // 1. NRO_DOCUMENTO (ascendente)
                    const docA = (a.NRO_DOCUMENTO || "").trim();
                    const docB = (b.NRO_DOCUMENTO || "").trim();
                    if (docA !== docB) return docA.localeCompare(docB, undefined, { numeric: true });

                    // 2. ESTADO_LIQUIDACION (ascendente)
                    const estA = (a.ESTADO_LIQUIDACION || "").trim();
                    const estB = (b.ESTADO_LIQUIDACION || "").trim();
                    if (estA !== estB) return estA.localeCompare(estB);

                    // 3. PLANTASS (descendente)
                    const plantaA = (a.PLANTASS || "").trim();
                    const plantaB = (b.PLANTASS || "").trim();
                    if (plantaA !== plantaB) return plantaB.localeCompare(plantaA);

                    // 4. ApJubPer (descendente)
                    const apA = parseFloat(a.ApJubPer) || 0;
                    const apB = parseFloat(b.ApJubPer) || 0;
                    return apB - apA;
                });

                const encabezados = Object.keys(resultados[0]).join(',');
                const filas = resultados.map(r => Object.values(r).join(',')).join('\n');
                const contenidoCompleto = encabezados + '\n' + filas;
                fs.writeFileSync(outputPathDetalle, contenidoCompleto, 'utf8');
                console.log("✅ Archivo AcApJub.csv creado exitosamente en carpeta csv-unidos");
            } else {
                fs.writeFileSync(outputPathDetalle, "", 'utf8');
            }

            // --- 5. Enviar los datos detallados al frontend para visualización ---
            res.json(resultados);
            
        } catch (err) {
            console.error("❌ Error al escribir los archivos:", err);
            res.status(500).send("Error al guardar los archivos.");
        }
    });

    stream.on('error', (err) => {
        res.status(500).json({ success: false, message: err.message });
    });
});

// NUEVO ENDPOINT: Topes de Ap. Jubilatorios
app.get('/api/topes-jubilatorios', (req, res) => {
    const csvPath = path.join(CSV_UNIDOS_DIR, 'AcApJub.csv');
    const results = [];

    if (!fs.existsSync(csvPath)) {
        return res.status(404).json({ error: 'Archivo AcApJub.csv no encontrado. Genere el acumulado primero.' });
    }

    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            const acumulado = parseFloat(row.AcumuladoApJub);
            // Filtro solicitado: AcumuladoApJub > 0.10
            if (!isNaN(acumulado) && acumulado > 0.10) {
                results.push(row);
            }
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// Endpoint para el procesamiento
// API para obtener lotes trabajados
app.get('/api/lotes-trabajados', (req, res) => {
    const filePath = path.join(CONFIG_DIR, 'excel-convertidos.csv');
    if (!fs.existsSync(filePath)) return res.json([]);
    const results = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            const key = data['excel-convertidos'] !== undefined ? 'excel-convertidos' : '\uFEFFexcel-convertidos';
            if (data[key]) results.push(data[key]);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

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
                const tempPath = path.join(process.cwd(), file.path);
                const targetPath = path.join(EXCEL_DIR, file.originalname);

                // Mover el archivo subido a la carpeta de entrada del conversor
                fs.renameSync(tempPath, targetPath);

                excelFilesToProcess.push(file.originalname);
                filesToCleanup.push(targetPath); // Lista de archivos a borrar después del proceso si es necesario
            });
        }

        // Ejecutar la lógica ETL centralizada
        const result = await ejecutarProcesoETL(excelFilesToProcess);

        if (result.success) {
            try {
                const excelConvertidosCsvPath = path.join(CONFIG_DIR, 'excel-convertidos.csv');
                let contentConvertidos = 'excel-convertidos\n';
                excelFilesToProcess.forEach(fileName => {
                    const nameWithoutExt = fileName.replace(/\.(xlsx|xls)$/i, '');
                    contentConvertidos += `${nameWithoutExt}\n`;
                });
                fs.writeFileSync(excelConvertidosCsvPath, contentConvertidos, 'utf8');
            } catch (err) {
                console.error("Error al actualizar excel-convertidos.csv:", err);
            }
        }

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

// API para obtener lotes de novedades trabajados
app.get('/api/lotes-novedades-trabajados', (req, res) => {
    const filePath = path.join(CONFIG_DIR, 'excel-convertidos-novedades.csv');
    if (!fs.existsSync(filePath)) return res.json([]);
    const results = [];
    fs.createReadStream(filePath)
        .pipe(csv())
        .on('data', (data) => {
            const key = data['excel-convertidos-novedades'] !== undefined ? 'excel-convertidos-novedades' : '\uFEFFexcel-convertidos-novedades';
            if (data[key]) results.push(data[key]);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// Endpoint para procesamiento de Novedades
app.post('/api/process-novedades', upload.array('excelFilesNovedades'), async (req, res) => {
    const processMode = req.body.processMode;
    const files = req.files;

    let excelFilesToProcess = [];

    try {
        if (processMode === 'default') {
            excelFilesToProcess = fs.readdirSync(EXCEL_NOVEDADES_DIR)
                .filter(file => file.match(/\.(xlsx|xls)$/i));

            if (excelFilesToProcess.length === 0) {
                return res.status(400).json({ success: false, message: "No se encontraron archivos en la carpeta excel-a-convertir-novedades." });
            }
        } else if (processMode === 'upload') {
            if (!files || files.length === 0) {
                return res.status(400).json({ success: false, message: "No se cargó ningún archivo." });
            }

            files.forEach(file => {
                const tempPath = path.join(process.cwd(), file.path);
                const targetPath = path.join(EXCEL_NOVEDADES_DIR, file.originalname);
                fs.renameSync(tempPath, targetPath);
                excelFilesToProcess.push(file.originalname);
            });
        }

        const result = await ejecutarProcesoNovedades(excelFilesToProcess);

        if (result.success) {
            try {
                const excelConvertidosCsvPath = path.join(CONFIG_DIR, 'excel-convertidos-novedades.csv');
                let contentConvertidos = 'excel-convertidos-novedades\n';
                excelFilesToProcess.forEach(fileName => {
                    const nameWithoutExt = fileName.replace(/\.(xlsx|xls)$/i, '');
                    contentConvertidos += `${nameWithoutExt}\n`;
                });
                fs.writeFileSync(excelConvertidosCsvPath, contentConvertidos, 'utf8');
            } catch (err) {
                console.error("Error al actualizar excel-convertidos-novedades.csv:", err);
            }
        }

        res.json(result);

    } catch (error) {
        console.error('Error en el endpoint /api/process-novedades:', error);
        res.status(500).json({ success: false, message: 'Error interno del servidor: ' + error.message });
    }
});

// ENDPOINTS PARA NOVEDADES REPORTERIA
app.get('/api/novedades/resumen', (req, res) => {
    try {
        if (!fs.existsSync(CSV_UNIDOS_NOVEDADES_DIR)) return res.json([]);
        const files = fs.readdirSync(CSV_UNIDOS_NOVEDADES_DIR).filter(f => f.endsWith('.csv'));
        const resumeDetails = [];

        for (const file of files) {
            const filePath = path.join(CSV_UNIDOS_NOVEDADES_DIR, file);
            const fileContent = fs.readFileSync(filePath, 'utf8');
            const nonBlankLines = fileContent.split('\n').filter(l => l.trim().length > 0);

            let count = 0;
            if (nonBlankLines.length > 0) count = nonBlankLines.length - 1; // restamos header

            resumeDetails.push({ nombre: file, registros: count });
        }
        res.json(resumeDetails);
    } catch (e) {
        res.status(500).json({ error: e.message });
    }
});
// FUNCIÓN PARA NORMALIZAR CADENAS (Quitar acentos y estandarizar)
function normalizeStr(str) {
    if (!str) return '';
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
}

// NUEVA FUNCIONALIDAD: NOVEDADES GC PARA CONTROL
async function generateAuxGcNovedades() {
    const gcCsvPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'gc.csv');
    const auxPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxGcNovedades.csv');

    // Al generar de nuevo las Novedades, forzamos que se regeneren las Liquidaciones 
    // para que se borren las observaciones previas.
    const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
    if (fs.existsSync(auxLiqPath)) {
        fs.unlinkSync(auxLiqPath);
        console.log(`[SERVER] 🗑️ AuxGCLiquidacion.csv borrado para forzar regeneración y limpieza de observaciones.`);
    }

    if (!fs.existsSync(gcCsvPath)) throw new Error('Archivo origen de Novedades GC no encontrado');

    const efectoresMap = new Map();
    if (fs.existsSync(GC_EFECTORES_CONFIG_FILE)) {
        await new Promise((resolve, reject) => {
            fs.createReadStream(GC_EFECTORES_CONFIG_FILE, { encoding: 'utf8' })
                .pipe(csv())
                .on('data', (data) => {
                    const efectorKey = data['EFECTORES'] !== undefined ? 'EFECTORES' : '\uFEFFEFECTORES';
                    const key = (data[efectorKey] || '').trim().toUpperCase();
                    const val = (data['NUEVOS EFECTORES'] || '').trim();
                    if (key) efectoresMap.set(key, val);
                })
                .on('end', resolve)
                .on('error', reject);
        });
    }

    const sdyfMap = new Map();
    if (fs.existsSync(GC_SDYF_CONFIG_FILE)) {
        await new Promise((resolve, reject) => {
            fs.createReadStream(GC_SDYF_CONFIG_FILE, { encoding: 'utf8' })
                .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
                .on('data', (data) => {
                    const dia = (data['DIA'] || '').trim();
                    const sdyf = (data['SDYF'] || '').trim().toUpperCase();
                    if (dia) sdyfMap.set(dia, sdyf);
                })
                .on('end', resolve)
                .on('error', reject);
        });
    }

    const importesMap = new Map();
    if (fs.existsSync(GC_CODIGOS_IMPORTES_CONFIG_FILE)) {
        await new Promise((resolve, reject) => {
            fs.createReadStream(GC_CODIGOS_IMPORTES_CONFIG_FILE, { encoding: 'latin1' })
                .pipe(csv({ separator: ';', mapHeaders: ({ header }) => header.trim() }))
                .on('data', (data) => {
                    const claveUnicaKey = data['CLAVEUNICA'] !== undefined ? 'CLAVEUNICA' : '\uFEFFCLAVEUNICA';
                    const key = (data[claveUnicaKey] || '').trim();
                    const rawImporte = (data['IMPORTE'] || '').trim();
                    
                    // Procesar importe (ej: " 230.000,00 ")
                    let val = rawImporte.replace(/\s/g, '').replace(/\./g, '').replace(',', '.');
                    let importe = parseFloat(val);
                    
                    if (key) {
                        importesMap.set(normalizeStr(key), isNaN(importe) ? 0 : importe);
                    }
                })
                .on('end', resolve)
                .on('error', reject);
        });
    }

    const results = [];
    return new Promise((resolve, reject) => {
        fs.createReadStream(gcCsvPath)
            .pipe(csv())
            .on('data', (row) => {
                const efectorRaw = (row['Efector'] || '').trim().toUpperCase();
                const transformado = efectoresMap.get(efectorRaw) || '';
                
                const doc = (row['Documento'] || '').trim();
                const clave = doc + transformado; 
                
                let sdyfVal = '';
                const fechaFormatMatch = (row['Fecha'] || '').match(/^(\d{4})-(\d{2})-(\d{2})/);
                if (fechaFormatMatch) {
                    const diaStr = `${fechaFormatMatch[3]}/${fechaFormatMatch[2]}/${fechaFormatMatch[1]}`;
                    const dictVal = sdyfMap.get(diaStr);
                    if (dictVal === 'SI') sdyfVal = 'SI';
                } else if ((row['Fecha'] || '').includes('/')) {
                    // Por si la fecha ya vino formateada DD/MM/YYYY o similar
                    let parts = row['Fecha'].split('/');
                    if(parts[0].length === 4) { // YYYY/MM/DD
                        const diaStr = `${parts[2]}/${parts[1]}/${parts[0]}`;
                        if (sdyfMap.get(diaStr) === 'SI') sdyfVal = 'SI';
                    } else if (parts[2].length === 4) { // DD/MM/YYYY
                        let d = parts[0].padStart(2, '0');
                        let m = parts[1].padStart(2, '0');
                        const diaStr = `${d}/${m}/${parts[2]}`;
                        if (sdyfMap.get(diaStr) === 'SI') sdyfVal = 'SI';
                    }
                }

                const prefijo = (sdyfVal === 'SI') ? 'S,DYF' : 'LAV';
                const tipoGuardia = (row['Tipo Guardia'] || '').trim();
                const tipoNivel = (row['Tipo Nivel'] || '').trim();
                const claveImporte = prefijo + tipoGuardia + tipoNivel.charAt(0);
                const claveImporteNorm = normalizeStr(claveImporte);

                // Nuevo cálculo de importe_guardia
                const horas = parseFloat(row['Horas']) || 0;
                const importeBase = importesMap.get(claveImporteNorm) || 0;
                
                let importeGuardiaFinal = 1; // Valor por defecto si no se encuentra o es cero
                if (importeBase > 0) {
                    importeGuardiaFinal = importeBase * horas;
                }

                results.push({
                    ...row,
                    EfectorTransformado: transformado,
                    clave: clave,
                    SDYF: sdyfVal,
                    clave_importe: claveImporte,
                    importe_guardia: importeGuardiaFinal
                });
            })
            .on('end', () => {
                if (results.length === 0) {
                    resolve(false);
                    return;
                }
                const headers = Object.keys(results[0]);
                const csvContent = [
                    headers.join(','),
                    ...results.map(r => headers.map(h => {
                        let val = (r[h] !== undefined && r[h] !== null) ? r[h].toString() : '';
                        if (val.includes(',') || val.includes('"') || val.includes('\n')) {
                            val = `"${val.replace(/"/g, '""')}"`;
                        }
                        return val;
                    }).join(','))
                ].join('\n');

                fs.writeFileSync(auxPath, csvContent, 'utf8');
                console.log(`[SERVER] ✅ AuxGcNovedades.csv generado.`);
                resolve(true);
            })
            .on('error', reject);
    });
}

app.get('/api/novedades/gc-para-control', async (req, res) => {
    try {
        const auxPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxGcNovedades.csv');
        if (!fs.existsSync(auxPath)) {
            await generateAuxGcNovedades();
        }

        if (!fs.existsSync(auxPath)) {
            return res.json([]);
        }

        const groupings = new Map();
        fs.createReadStream(auxPath)
            .pipe(csv())
            .on('data', (row) => {
                const clave = row.clave;
                const importe = parseFloat(row.importe_guardia) || 0;
                
                if (!groupings.has(clave)) {
                    // Keep the first row's data and initialize the sum
                    groupings.set(clave, {
                        ...row,
                        importe_guardia: importe
                    });
                } else {
                    // Just add to the sum
                    const existing = groupings.get(clave);
                    existing.importe_guardia += importe;
                }
            })
            .on('end', () => {
                // Convert Map values to array
                const results = Array.from(groupings.values());
                res.json(results);
            })
            .on('error', (err) => res.status(500).json({ error: err.message }));
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/novedades/gc-para-control:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/gc-liquidacion-eps', async (req, res) => {
    try {
        // 1. Asegurar que AuxGcNovedades y AuxGCLiquidacion existen
        const auxNovedadesPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxGcNovedades.csv');
        if (!fs.existsSync(auxNovedadesPath)) {
            await generateAuxGcNovedades();
        }

        const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
        if (!fs.existsSync(auxLiqPath)) {
            await generateAuxGCLiquidacion();
        }

        // 2. Leer y agrupar Novedades por 'clave'
        const novedadesMap = new Map();
        if (fs.existsSync(auxNovedadesPath)) {
            await new Promise((resolve, reject) => {
                fs.createReadStream(auxNovedadesPath)
                    .pipe(csv())
                    .on('data', (row) => {
                        const clave = row.clave;
                        const importe = parseFloat(row.importe_guardia) || 0;
                        novedadesMap.set(clave, (novedadesMap.get(clave) || 0) + importe);
                    })
                    .on('end', resolve)
                    .on('error', reject);
            });
        }

        // 3. Procesar Liquidacion (similar a processGCControl) y hacer el cruce
        if (!fs.existsSync(auxLiqPath)) return res.status(404).json({ error: 'Archivo auxiliar de liquidacion no encontrado' });

        const groupings = new Map();

        fs.createReadStream(auxLiqPath)
            .pipe(csv())
            .on('data', (row) => {
                const pImp = (row.PERIODO_IMPUTADO || '').trim();
                const pLiq = (row.PERIODO_LIQUIDADO || '').trim();
                
                // Filtro: Solo periodo imputado == liquidado (Igual que GC Para Control normal)
                if (pImp === pLiq) {
                    const clave = row.CLAVE_AGRUPACION;
                    if (!groupings.has(clave)) {
                        groupings.set(clave, {
                            ORGANISMO: row.ORGANISMO,
                            NRO_DOCUMENTO: row.NRO_DOCUMENTO,
                            NIVEL: row.NIVEL,
                            NUMERO_LIQ: row.NUMERO_LIQ || '',
                            DESCAGENTE: row.DESCAGENTE,
                            PERIODO_IMPUTADO: row.PERIODO_IMPUTADO,
                            NUMERO_CARGO: row.NUMERO_CARGO,
                            PERIODO_LIQUIDADO: row.PERIODO_LIQUIDADO,
                            SUMA: parseFloat(row.SUMA_GC) || 0,
                            GC: '',
                            OBSERVACION_EPS: row.OBSERVACION_EPS || '',
                            CLAVE: clave
                        });
                    } else {
                        const existing = groupings.get(clave);
                        existing.SUMA += parseFloat(row.SUMA_GC) || 0;
                        if (row.OBSERVACION_EPS) existing.OBSERVACION_EPS = row.OBSERVACION_EPS;
                    }
                }
            })
            .on('end', () => {
                const results = Array.from(groupings.values()).map(r => {
                    const sumaLiq = r.SUMA;
                    const importeEps = novedadesMap.get(r.CLAVE) || 0;
                    const control = sumaLiq - importeEps;
                    
                    let obs = r.OBSERVACION_EPS;
                    if (!obs || obs.trim() === '') {
                        if (control === 0) obs = "Verificado";
                        else if (control > 0) obs = "Verificar Liquidado de Mas";
                        else obs = "Verificar Liquidado de Menos";
                    }
                    
                    return {
                        OBSERVACION: obs,
                        Importes_Eps: importeEps.toFixed(2),
                        Control: control.toFixed(2),
                        ...r,
                        SUMA: sumaLiq.toFixed(2)
                    };
                });
                res.json(results);
            })
            .on('error', (err) => res.status(500).json({ error: err.message }));

    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/gc-liquidacion-eps:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// NUEVO ENDPOINT: Guardar Observaciones en Control GC Liquidacion a Eps
app.post('/api/gc-liquidacion-eps/save-observaciones', async (req, res) => {
    try {
        const { changes } = req.body;
        if (!changes || !Array.isArray(changes)) {
            return res.status(400).json({ success: false, error: 'Cambios no válidos.' });
        }

        const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
        if (!fs.existsSync(auxLiqPath)) {
            return res.status(404).json({ success: false, error: 'El archivo AuxGCLiquidacion.csv no existe.' });
        }

        const rows = await readCsvFile(auxLiqPath);
        const changeMap = new Map();
        changes.forEach(c => {
            changeMap.set(c.clave, c.observacion);
        });

        const updatedRows = rows.map(row => {
            if (changeMap.has(row.CLAVE_AGRUPACION)) {
                row.OBSERVACION_EPS = changeMap.get(row.CLAVE_AGRUPACION);
            }
            return row;
        });

        if (updatedRows.length > 0) {
            const headers = Object.keys(updatedRows[0]);
            const csvContent = [
                headers.join(','),
                ...updatedRows.map(r => headers.map(h => {
                    let val = (r[h] !== undefined && r[h] !== null) ? r[h].toString() : '';
                    if (val.includes(',') || val.includes('"')) val = `"${val.replace(/"/g, '""')}"`;
                    return val;
                }).join(','))
            ].join('\n');
            fs.writeFileSync(auxLiqPath, csvContent, 'utf8');
        }

        res.json({ success: true, message: 'Observaciones guardadas correctamente' });
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/gc-liquidacion-eps/save-observaciones:', error.message);
        res.status(500).json({ success: false, error: error.message });
    }
});

app.get('/api/gc-no-liquidados', async (req, res) => {
    try {
        // 1. Asegurar que AuxGcNovedades y AuxGCLiquidacion existen
        const auxNovedadesPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxGcNovedades.csv');
        if (!fs.existsSync(auxNovedadesPath)) {
            await generateAuxGcNovedades();
        }

        const auxLiqPath = path.join(CSV_UNIDOS_DIR, 'AuxGCLiquidacion.csv');
        if (!fs.existsSync(auxLiqPath)) {
            await generateAuxGCLiquidacion();
        }

        // 2. Leer "GC Para Control" y guardar sus CLAVEs
        if (!fs.existsSync(auxLiqPath)) return res.status(404).json({ error: 'Archivo auxiliar de liquidacion no encontrado' });

        const clavesLiquidadas = new Set();
        await new Promise((resolve, reject) => {
            fs.createReadStream(auxLiqPath)
                .pipe(csv())
                .on('data', (row) => {
                    const pImp = (row.PERIODO_IMPUTADO || '').trim();
                    const pLiq = (row.PERIODO_LIQUIDADO || '').trim();
                    if (pImp === pLiq) {
                        clavesLiquidadas.add(row.CLAVE_AGRUPACION);
                    }
                })
                .on('end', resolve)
                .on('error', reject);
        });

        // 3. Leer Novedades y filtrar las que NO están en clavesLiquidadas
        if (!fs.existsSync(auxNovedadesPath)) {
            return res.json([]);
        }

        const groupings = new Map();
        fs.createReadStream(auxNovedadesPath)
            .pipe(csv())
            .on('data', (row) => {
                const clave = row.clave;
                if (!clavesLiquidadas.has(clave)) {
                    const importe = parseFloat(row.importe_guardia) || 0;
                    if (!groupings.has(clave)) {
                        groupings.set(clave, {
                            ...row,
                            importe_guardia: importe
                        });
                    } else {
                        const existing = groupings.get(clave);
                        existing.importe_guardia += importe;
                    }
                }
            })
            .on('end', () => {
                const results = Array.from(groupings.values());
                res.json(results);
            })
            .on('error', (err) => res.status(500).json({ error: err.message }));

    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/gc-no-liquidados:', error.message);
        res.status(500).json({ error: error.message });
    }
});

// Helper to read CSV file as Promise
function readCsvFile(filePath) {
    return new Promise((resolve, reject) => {
        if (!fs.existsSync(filePath)) {
            return resolve([]);
        }
        const results = [];
        fs.createReadStream(filePath)
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => resolve(results))
            .on('error', (err) => reject(err));
    });
}

// Helper to parse floats safely from CSV cells
function parseCsvFloat(val) {
    if (!val) return 0;
    const cleaned = val.toString().replace(/"/g, '').replace(/,/g, '').trim();
    const parsed = parseFloat(cleaned);
    return isNaN(parsed) ? 0 : parsed;
}

// Generate AuxResidentesLiquidacion.csv
async function generateAuxResidentesLiquidacion() {
    const unificadasPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
    const novedadesPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'residentes.csv');
    const auxPath = path.join(CSV_UNIDOS_DIR, 'AuxResidentesLiquidacion.csv');

    if (!fs.existsSync(unificadasPath)) throw new Error('Archivo liquidaciones_unificadas.csv no encontrado.');

    // 1. Load Novedades Residentes mapping: DNI -> row
    const novedadesMap = new Map();
    if (fs.existsSync(novedadesPath)) {
        const novedadesList = await readCsvFile(novedadesPath);
        novedadesList.forEach(row => {
            const dni = (row.DNI || '').toString().trim();
            const estado = (row.estado || '').toString().trim();
            const nivelNovedad = (row.NIVEL || '').toString().trim().toUpperCase();
            if (dni && estado === 'Activo') {
                novedadesMap.set(dni + nivelNovedad, row);
            }
        });
    }

    // 2. Load existing observations if any
    const existingObservations = new Map();
    if (fs.existsSync(auxPath)) {
        const existingRows = await readCsvFile(auxPath);
        existingRows.forEach(row => {
            const doc = (row.NRO_DOCUMENTO || '').toString().trim();
            const cargo = (row.NUMERO_CARGO || '').toString().trim();
            if (doc) {
                existingObservations.set(doc + '_' + cargo, row.OBSERVACION || '');
            }
        });
    }

    // 3. Read liquidaciones_unificadas.csv and build output
    const unificadasRows = await readCsvFile(unificadasPath);
    const results = [];

    unificadasRows.forEach(row => {
        const esResidente = row.PLANTA === 'Residentes' || row.PLANTA === 'Residentes Nacionales';
        const dTrab = parseCsvFloat(row.D_TRAB);
        const pImp = (row.PERIODO_IMPUTADO || '').toString().trim();
        const pLiq = (row.PERIODO_LIQUIDADO || '').toString().trim();
        const cumpleCondicion = esResidente && pImp === pLiq && dTrab !== 0;

        if (cumpleCondicion) {
            const doc = (row.NRO_DOCUMENTO || '').toString().trim();
            const cargo = (row.NUMERO_CARGO || '').toString().trim();
            const key = doc + '_' + cargo;

            const nivelOriginal = (row.NIVEL || '').toString();
            const char7 = nivelOriginal.charAt(6) ? nivelOriginal.charAt(6).toUpperCase() : '';
            const char8 = nivelOriginal.charAt(7) || '';
            const lookupKey = doc + char7 + char8;

            const novedad = novedadesMap.get(lookupKey);
            const bruto31 = parseCsvFloat(novedad ? novedad.ImporteEscalaCriticidad003_31 : 0);
            const bruto91 = parseCsvFloat(novedad ? novedad.ImporteEscalaForta003_91 : 0);

            const lib31 = parseCsvFloat(row.LIB_003_31);
            const lib91 = parseCsvFloat(row.LIB_003_91);

            const diasInasist = parseCsvFloat(row.DIAS_INASIST);

            const dif31 = lib31 - ((bruto31 / 30) * (dTrab - diasInasist));
            const dif91 = lib91 - ((bruto91 / 30) * (dTrab - diasInasist));

            const newRow = {};
            if (!novedad) {
                newRow['OBSERVACION'] = (existingObservations.has(key) && existingObservations.get(key) !== 'Obs-')
                    ? existingObservations.get(key)
                    : '01-No Encontrado en Novedades de Residentes';
            } else {
                newRow['OBSERVACION'] = existingObservations.has(key) ? existingObservations.get(key) : 'Obs-';
            }
            newRow['BRUTO 003-31'] = bruto31.toFixed(2);
            newRow['DIFERENCIAS 003-31'] = dif31.toFixed(2);
            newRow['BRUTO 003-91'] = bruto91.toFixed(2);
            newRow['DIFERENCIAS 003-91'] = dif91.toFixed(2);

            const cols = [
                'NIVEL', 'DESCAGENTE', 'NRO_DOCUMENTO', 'DIAS_INASIST', 'D_TRAB',
                'PLANTA', 'ORGANISMO', 'FUNCION', 'PERIODO_IMPUTADO', 'PERIODO_LIQUIDADO',
                'NUMERO_CARGO', 'Area2', 'LIB_003_31', 'LIB_003_91'
            ];

            cols.forEach(col => {
                newRow[col] = row[col] ?? '';
            });

            results.push(newRow);
        }
    });

    const headers = [
        'OBSERVACION', 'BRUTO 003-31', 'DIFERENCIAS 003-31', 'BRUTO 003-91', 'DIFERENCIAS 003-91',
        'NIVEL', 'DESCAGENTE', 'NRO_DOCUMENTO', 'DIAS_INASIST', 'D_TRAB',
        'PLANTA', 'ORGANISMO', 'FUNCION', 'PERIODO_IMPUTADO', 'PERIODO_LIQUIDADO',
        'NUMERO_CARGO', 'Area2', 'LIB_003_31', 'LIB_003_91'
    ];

    const csvContent = [
        headers.join(','),
        ...results.map(r => headers.map(h => {
            let val = (r[h] !== undefined && r[h] !== null) ? r[h].toString() : '';
            if (val.includes(',') || val.includes('"') || val.includes('\n')) {
                val = `"${val.replace(/"/g, '""')}"`;
            }
            return val;
        }).join(','))
    ].join('\n');

    fs.writeFileSync(auxPath, csvContent, 'utf8');
    console.log(`[SERVER] ✅ AuxResidentesLiquidacion.csv generado con ${results.length} registros.`);
    return true;
}

app.get('/api/residentes/control', async (req, res) => {
    try {
        const auxPath = path.join(CSV_UNIDOS_DIR, 'AuxResidentesLiquidacion.csv');
        if (!fs.existsSync(auxPath)) {
            await generateAuxResidentesLiquidacion();
        }

        if (!fs.existsSync(auxPath)) {
            return res.json([]);
        }

        const data = await readCsvFile(auxPath);
        res.json(data);
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/residentes/control:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.post('/api/residentes/save-observaciones', async (req, res) => {
    try {
        const { changes } = req.body;
        if (!changes || !Array.isArray(changes)) {
            return res.status(400).json({ success: false, error: 'Cambios no válidos.' });
        }

        const auxPath = path.join(CSV_UNIDOS_DIR, 'AuxResidentesLiquidacion.csv');
        if (!fs.existsSync(auxPath)) {
            return res.status(404).json({ success: false, error: 'El archivo AuxResidentesLiquidacion.csv no existe.' });
        }

        const rows = await readCsvFile(auxPath);
        const changeMap = new Map();
        changes.forEach(c => {
            const doc = (c.nro_documento || '').toString().trim();
            const cargo = (c.numero_cargo || '').toString().trim();
            const key = doc + '_' + cargo;
            changeMap.set(key, c.observacion);
        });

        const updatedRows = rows.map(row => {
            const doc = (row.NRO_DOCUMENTO || '').toString().trim();
            const cargo = (row.NUMERO_CARGO || '').toString().trim();
            const key = doc + '_' + cargo;

            if (changeMap.has(key)) {
                row.OBSERVACION = changeMap.get(key);
            }
            return row;
        });

        const headers = [
            'OBSERVACION', 'BRUTO 003-31', 'DIFERENCIAS 003-31', 'BRUTO 003-91', 'DIFERENCIAS 003-91',
            'NIVEL', 'DESCAGENTE', 'NRO_DOCUMENTO', 'DIAS_INASIST', 'D_TRAB',
            'PLANTA', 'ORGANISMO', 'FUNCION', 'PERIODO_IMPUTADO', 'PERIODO_LIQUIDADO',
            'NUMERO_CARGO', 'Area2', 'LIB_003_31', 'LIB_003_91'
        ];

        const csvContent = [
            headers.join(','),
            ...updatedRows.map(r => headers.map(h => {
                let val = (r[h] !== undefined && r[h] !== null) ? r[h].toString() : '';
                if (val.includes(',') || val.includes('"') || val.includes('\n')) {
                    val = `"${val.replace(/"/g, '""')}"`;
                }
                return val;
            }).join(','))
        ].join('\n');

        fs.writeFileSync(auxPath, csvContent, 'utf8');
        res.json({ success: true });
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/residentes/save-observaciones:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/novedades/residentes-no-liquidados', async (req, res) => {
    try {
        const unificadasPath = path.join(CSV_UNIDOS_DIR, FINAL_CSV_NAME);
        const novedadesPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'residentes.csv');

        if (!fs.existsSync(unificadasPath) || !fs.existsSync(novedadesPath)) {
            return res.json([]);
        }

        // 1. Build Set of keys from Liquidaciones
        const liquidacionesKeys = new Set();
        const unificadasRows = await readCsvFile(unificadasPath);
        
        unificadasRows.forEach(row => {
            const esResidente = row.PLANTA === 'Residentes' || row.PLANTA === 'Residentes Nacionales';
            const dTrab = parseCsvFloat(row.D_TRAB);
            const pImp = (row.PERIODO_IMPUTADO || '').toString().trim();
            const pLiq = (row.PERIODO_LIQUIDADO || '').toString().trim();
            
            if (esResidente && pImp === pLiq && dTrab !== 0) {
                const doc = (row.NRO_DOCUMENTO || '').toString().trim();
                const nivelOriginal = (row.NIVEL || '').toString();
                const char7 = nivelOriginal.charAt(6) ? nivelOriginal.charAt(6).toUpperCase() : '';
                const char8 = nivelOriginal.charAt(7) || '';
                const lookupKey = doc + char7 + char8;
                liquidacionesKeys.add(lookupKey);
            }
        });

        // 2. Cross with Novedades
        const novedadesRows = await readCsvFile(novedadesPath);
        const results = [];
        
        novedadesRows.forEach(row => {
            const estado = (row.estado || '').toString().trim();
            if (estado === 'Activo') {
                const dni = (row.DNI || '').toString().trim();
                const nivelNovedad = (row.NIVEL || '').toString().trim().toUpperCase();
                const keyNovedad = dni + nivelNovedad;
                
                if (!liquidacionesKeys.has(keyNovedad)) {
                    results.push(row);
                }
            }
        });

        res.json(results);
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/novedades/residentes-no-liquidados:', error.message);
        res.status(500).json({ error: error.message });
    }
});

app.get('/api/novedades/:tipo', (req, res) => {
    const { tipo } = req.params;
    const csvPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, `${tipo}.csv`);

    if (!fs.existsSync(csvPath)) {
        // En lugar de enviar un JSON con error que revienta Datatable, enviamos data vacía
        return res.json([]);
    }

    const results = [];
    fs.createReadStream(csvPath)
        .pipe(csv())
        .on('data', (row) => {
            if (tipo === 'ld') {
                if (row.IMPORTE_AUTORIZADO !== undefined) {
                    row.IMPORTE_AUTORIZADO = parseFloat(String(row.IMPORTE_AUTORIZADO).replace(',', '.')) || 0;
                }
                if (row.IMPORTE_LIQUIDADO !== undefined) {
                    row.IMPORTE_LIQUIDADO = parseFloat(String(row.IMPORTE_LIQUIDADO).replace(',', '.')) || 0;
                }
            }
            results.push(row);
        })
        .on('end', () => res.json(results))
        .on('error', (err) => res.status(500).json({ error: err.message }));
});

// ENDPOINTS PARA BORRADO DE ARCHIVOS
app.delete('/api/borrar-liquidacion', (req, res) => {
    try {
        const keepExcel = req.query.keepExcel === 'true';
        const dirs = [
            path.join(process.cwd(), 'csv-convertido'),
            path.join(process.cwd(), 'csv-unidos')
        ];
        
        if (!keepExcel) {
            dirs.push(path.join(process.cwd(), 'excel-a-convertir'));
        }
        dirs.forEach(dir => {
            if (fs.existsSync(dir)) {
                const files = fs.readdirSync(dir);
                for (const file of files) {
                    const filePath = path.join(dir, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                    }
                }
            }
        });

        // También borrar el seguimiento de lotes trabajados
        const trackingFile = path.join(CONFIG_DIR, 'excel-convertidos.csv');
        if (fs.existsSync(trackingFile)) {
            fs.unlinkSync(trackingFile);
        }

        res.json({ success: true, message: 'Archivos de liquidación borrados correctamente.' });
    } catch (e) {
        console.error('Error al borrar archivos de liquidación:', e);
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/borrar-novedad', (req, res) => {
    try {
        const dirs = [
            path.join(process.cwd(), 'csv-unidos-novedades'),
            path.join(process.cwd(), 'excel-a-convertir-novedades')
        ];
        dirs.forEach(dir => {
            if (fs.existsSync(dir)) {
                const files = fs.readdirSync(dir);
                for (const file of files) {
                    const filePath = path.join(dir, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                    }
                }
            }
        });

        // También borrar el seguimiento de lotes de novedades trabajados
        const trackingFile = path.join(CONFIG_DIR, 'excel-convertidos-novedades.csv');
        if (fs.existsSync(trackingFile)) {
            fs.unlinkSync(trackingFile);
        }

        res.json({ success: true, message: 'Archivos de novedad borrados correctamente.' });
    } catch (e) {
        console.error('Error al borrar archivos de novedad:', e);
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/borrar-novedades-guardias', (req, res) => {
    try {
        let deletedExcelCount = 0;
        let deletedCsvCount = 0;

        // 1. Eliminar de excel-a-convertir-novedades/ el/los archivos que inicien con "gc"
        if (fs.existsSync(EXCEL_NOVEDADES_DIR)) {
            const files = fs.readdirSync(EXCEL_NOVEDADES_DIR);
            for (const file of files) {
                if (file.toLowerCase().startsWith('gc')) {
                    const filePath = path.join(EXCEL_NOVEDADES_DIR, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                        deletedExcelCount++;
                    }
                }
            }
        }

        // 2. Eliminar de csv-unidos-novedades/ el archivo gc.csv y el archivo AuxGCNovedades.csv
        if (fs.existsSync(CSV_UNIDOS_NOVEDADES_DIR)) {
            const files = fs.readdirSync(CSV_UNIDOS_NOVEDADES_DIR);
            for (const file of files) {
                const lowerFile = file.toLowerCase();
                if (lowerFile === 'gc.csv' || lowerFile === 'auxgcnovedades.csv') {
                    const filePath = path.join(CSV_UNIDOS_NOVEDADES_DIR, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                        deletedCsvCount++;
                    }
                }
            }
        }

        // 3. Actualizar el archivo excel-convertidos-novedades.csv
        const trackingFile = path.join(CONFIG_DIR, 'excel-convertidos-novedades.csv');
        if (fs.existsSync(trackingFile)) {
            const content = fs.readFileSync(trackingFile, 'utf8');
            const lines = content.split(/\r?\n/);
            const remainingLines = [];
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (line && !line.toLowerCase().startsWith('gc')) {
                    remainingLines.push(line);
                }
            }
            if (remainingLines.length === 0) {
                fs.unlinkSync(trackingFile);
            } else {
                const newContent = 'excel-convertidos-novedades\n' + remainingLines.join('\n') + '\n';
                fs.writeFileSync(trackingFile, newContent, 'utf8');
            }
        }

        res.json({ 
            success: true, 
            message: 'Novedades de Guardias borradas correctamente.',
            details: { deletedExcelCount, deletedCsvCount }
        });
    } catch (e) {
        console.error('Error al borrar novedades de guardias:', e);
        res.status(500).json({ error: e.message });
    }
});

app.delete('/api/borrar-novedades-residentes', (req, res) => {
    try {
        let deletedExcelCount = 0;
        let deletedCsvCount = 0;

        // 1. Eliminar de excel-a-convertir-novedades/ el/los archivos que inicien con "residentes"
        if (fs.existsSync(EXCEL_NOVEDADES_DIR)) {
            const files = fs.readdirSync(EXCEL_NOVEDADES_DIR);
            for (const file of files) {
                if (file.toLowerCase().startsWith('residentes')) {
                    const filePath = path.join(EXCEL_NOVEDADES_DIR, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                        deletedExcelCount++;
                    }
                }
            }
        }

        // 2. Eliminar de csv-unidos-novedades/ el archivo residentes.csv y AuxResidentesNovedades.csv/AuxResidentesNovedades
        if (fs.existsSync(CSV_UNIDOS_NOVEDADES_DIR)) {
            const files = fs.readdirSync(CSV_UNIDOS_NOVEDADES_DIR);
            for (const file of files) {
                const lowerFile = file.toLowerCase();
                if (lowerFile === 'residentes.csv' || lowerFile === 'auxresidentesnovedades.csv' || lowerFile === 'auxresidentesnovedades') {
                    const filePath = path.join(CSV_UNIDOS_NOVEDADES_DIR, file);
                    if (fs.lstatSync(filePath).isFile()) {
                        fs.unlinkSync(filePath);
                        deletedCsvCount++;
                    }
                }
            }
        }

        // 2.5 Eliminar de csv-unidos/ el archivo AuxResidentesLiquidacion.csv
        const auxLiquidacionPath = path.join(CSV_UNIDOS_DIR, 'AuxResidentesLiquidacion.csv');
        if (fs.existsSync(auxLiquidacionPath)) {
            fs.unlinkSync(auxLiquidacionPath);
            deletedCsvCount++;
        }

        // 3. Actualizar el archivo excel-convertidos-novedades.csv
        const trackingFile = path.join(CONFIG_DIR, 'excel-convertidos-novedades.csv');
        if (fs.existsSync(trackingFile)) {
            const content = fs.readFileSync(trackingFile, 'utf8');
            const lines = content.split(/\r?\n/);
            const remainingLines = [];
            for (let i = 1; i < lines.length; i++) {
                const line = lines[i].trim();
                if (line && !line.toLowerCase().startsWith('residentes')) {
                    remainingLines.push(line);
                }
            }
            if (remainingLines.length === 0) {
                fs.unlinkSync(trackingFile);
            } else {
                const newContent = 'excel-convertidos-novedades\n' + remainingLines.join('\n') + '\n';
                fs.writeFileSync(trackingFile, newContent, 'utf8');
            }
        }

        res.json({ 
            success: true, 
            message: 'Novedades de Residentes borradas correctamente.',
            details: { deletedExcelCount, deletedCsvCount }
        });
    } catch (e) {
        console.error('Error al borrar novedades de residentes:', e);
        res.status(500).json({ error: e.message });
    }
});

// ==========================================
// SISTEMA DE LOGINS Y PERMISOS
// ==========================================
const USUARIOS_FILE = path.join(CONFIG_DIR, 'usuarios.json');

// Inicializar archivo de usuarios si no existe
if (!fs.existsSync(USUARIOS_FILE)) {
    const defaultUsers = [
        {
            username: 'progra',
            password: '1234',
            role: 'programador',
            permissions: ['all']
        },
        {
            username: 'Admin',
            password: '1234',
            role: 'Administrador',
            permissions: [
                'conversion-form',
                'menu-controles-manuales',
                'menu-dashboard-informe',
                'menu-liquidacion-completa'
            ]
        },
        {
            username: 'Perso',
            password: '1234',
            role: 'Personal',
            permissions: [
                'conversion-form',
                'menu-dashboard-informe'
            ]
        }
    ];
    fs.writeFileSync(USUARIOS_FILE, JSON.stringify(defaultUsers, null, 2), 'utf8');
}

// Función auxiliar para leer usuarios
function leerUsuarios() {
    try {
        if (fs.existsSync(USUARIOS_FILE)) {
            const data = fs.readFileSync(USUARIOS_FILE, 'utf8');
            return JSON.parse(data);
        }
    } catch (e) {
        console.error("Error al leer usuarios.json:", e);
    }
    return [];
}

// Función auxiliar para escribir usuarios
function guardarUsuarios(usuarios) {
    try {
        fs.writeFileSync(USUARIOS_FILE, JSON.stringify(usuarios, null, 2), 'utf8');
        return true;
    } catch (e) {
        console.error("Error al guardar usuarios.json:", e);
        return false;
    }
}

// 1. Endpoint de Login
app.post('/api/login', (req, res) => {
    const { username, password } = req.body;
    if (!username || !password) {
        return res.status(400).json({ error: 'Usuario y contraseña son requeridos' });
    }

    const usuarios = leerUsuarios();
    const usuario = usuarios.find(u => u.username.toLowerCase() === username.toLowerCase() && u.password === password);

    if (!usuario) {
        return res.status(401).json({ error: 'Credenciales incorrectas' });
    }

    // Retornamos la sesión sin la contraseña por seguridad
    res.json({
        success: true,
        username: usuario.username,
        role: usuario.role,
        permissions: usuario.permissions
    });
});

// Middleware simple para requerir rol 'programador' en endpoints de gestión
function requireProgramador(req, res, next) {
    const userRole = req.headers['x-user-role'];
    if (userRole === 'programador') {
        return next();
    }
    res.status(403).json({ error: 'Acceso denegado. Se requieren permisos de programador.' });
}

// 2. Obtener lista de usuarios (solo programador)
app.get('/api/users', requireProgramador, (req, res) => {
    const usuarios = leerUsuarios();
    res.json(usuarios);
});

// 3. Crear nuevo usuario (solo programador)
app.post('/api/users', requireProgramador, (req, res) => {
    const { username, password, role, permissions } = req.body;
    if (!username || !password || !role || !permissions) {
        return res.status(400).json({ error: 'Todos los campos son requeridos' });
    }

    const usuarios = leerUsuarios();
    if (usuarios.some(u => u.username.toLowerCase() === username.toLowerCase())) {
        return res.status(400).json({ error: 'El nombre de usuario ya está registrado' });
    }

    const nuevoUsuario = {
        username,
        password,
        role,
        permissions
    };

    usuarios.push(nuevoUsuario);
    if (guardarUsuarios(usuarios)) {
        res.json({ success: true, user: nuevoUsuario });
    } else {
        res.status(500).json({ error: 'Error al escribir en la base de datos' });
    }
});

// 4. Editar usuario existente (solo programador)
app.put('/api/users/:username', requireProgramador, (req, res) => {
    const targetUsername = req.params.username;
    const { password, role, permissions } = req.body;

    const usuarios = leerUsuarios();
    const index = usuarios.findIndex(u => u.username.toLowerCase() === targetUsername.toLowerCase());

    if (index === -1) {
        return res.status(404).json({ error: 'Usuario no encontrado' });
    }

    // Si editan a progra, prevenimos quitarle el rol de programador o sus accesos para no dejar huérfano el sistema
    if (targetUsername.toLowerCase() === 'progra') {
        usuarios[index].password = password || usuarios[index].password;
    } else {
        usuarios[index].password = password || usuarios[index].password;
        usuarios[index].role = role || usuarios[index].role;
        usuarios[index].permissions = permissions || usuarios[index].permissions;
    }

    if (guardarUsuarios(usuarios)) {
        res.json({ success: true, user: usuarios[index] });
    } else {
        res.status(500).json({ error: 'Error al guardar los cambios' });
    }
});

// 5. Eliminar usuario (solo programador)
app.delete('/api/users/:username', requireProgramador, (req, res) => {
    const targetUsername = req.params.username;

    if (targetUsername.toLowerCase() === 'progra') {
        return res.status(400).json({ error: 'No es posible eliminar el usuario administrador principal (progra)' });
    }

    const usuarios = leerUsuarios();
    const filtrados = usuarios.filter(u => u.username.toLowerCase() !== targetUsername.toLowerCase());

    if (usuarios.length === filtrados.length) {
        return res.status(404).json({ error: 'Usuario no encontrado' });
    }

    if (guardarUsuarios(filtrados)) {
        res.json({ success: true, message: 'Usuario eliminado correctamente' });
    } else {
        res.status(500).json({ error: 'Error al escribir en la base de datos' });
    }
});

// ENDPOINT DE APAGADO (Para cerrar el servidor cuando se cierra la aplicación de escritorio)
app.post('/api/shutdown', (req, res) => {
    console.log('[SERVER] 🛑 Solicitud de apagado recibida. Cerrando servidor...');
    res.json({ success: true, message: 'Apagando el servidor local...' });
    setTimeout(() => {
        process.exit(0);
    }, 1000);
});

// ==========================================
// CONFIGURACIÓN DE RED DEL SERVIDOR
// ==========================================

// OPCIÓN 1: Ejecutar solo localmente (Más seguro, nadie en tu red puede entrar)
// Descomenta las siguientes 4 líneas para usar esta opción, y comenta la Opción 2.
app.listen(PORT, '127.0.0.1', () => {
   console.log(`Servidor de la Interfaz Simple ejecutándose en modo LOCAL (Solo esta PC) en el puerto ${PORT}`);
   console.log(`Accede desde tu navegador en: http://localhost:${PORT}`);
});

// OPCIÓN 2: Ejecutar en la red local (Para ver muestras desde otros dispositivos)
// Descomenta las siguientes 4 líneas para usar esta opción, y comenta la Opción 1.
//app.listen(PORT, '0.0.0.0', () => {
  //  console.log(`Servidor de la Interfaz Simple ejecutándose en modo RED (Accesible por otros) en el puerto ${PORT}`);
  //  console.log(`Para acceder desde otro dispositivo, ingresa la IP de esta PC seguida de :${PORT}`);
//});