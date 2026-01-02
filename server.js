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

//-----------

// Crear la carpeta 'excel-a-convertir' si no existe
if (!fs.existsSync(EXCEL_DIR)) {
    fs.mkdirSync(EXCEL_DIR);
}

// Configuración de Multer: Al subir archivos, los guardamos temporalmente.
const upload = multer({ dest: 'uploads/' }); 

// Middleware para servir archivos estáticos (la interfaz HTML)
app.use(express.static('public'));

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
    'ESTADO_LIQUIDACION'
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
// ... (El resto de tu código server.js) ...

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


app.listen(PORT, () => {
    console.log(`Servidor de la Interfaz Simple ejecutándose en http://localhost:${PORT}`);
});