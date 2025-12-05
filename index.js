// index.js (Punto de entrada: Procesamiento de Lotes)

const fs = require('fs');
const path = require('path');
const { convertirExcelACsv } = require('./services/conversor');
const { limpiarColumnasCsv } = require('./services/limpiador');
const { unirCsv } = require('./services/unificador');              // <-- NUEVA IMPORTACIÓN
//const { cargarCsvAPostgres } = require('./services/cargador'); 

// --- CONFIGURACIÓN PRINCIPAL ---

// 🚨 INTERRUPTOR DE SEGURIDAD
const CARGA_ACTIVADA = false; 

// Columnas de TEXTO con espacios (base 1)
const COLUMNAS_A_LIMPIAR = [
    1,2,193,194,195,202,209,205,212,213,214,216
    // ... AJUSTA ESTA LISTA
];

const EXCEL_DIR = path.join(__dirname, 'excel-a-convertir');
// ------------------------------

async function ejecutarProcesoETL() {
    try {
        console.log(`\n==========================================`);
        console.log(`  INICIO DEL PROCESO DE LOTES ETL`);
        console.log(`==========================================`);

        // 1. OBTENER ARCHIVOS DE EXCEL
        const excelArchivos = fs.readdirSync(EXCEL_DIR)
            .filter(file => file.match(/\.(xlsx|xls)$/i));

        if (excelArchivos.length === 0) {
            console.warn("⚠️ No se encontraron archivos .xlsx en la carpeta 'excel-a-convertir'.");
            return;
        }

        console.log(`[ORQUESTADOR] Se encontraron ${excelArchivos.length} archivos para procesar.`);

        // 2. CONVERSIÓN Y LIMPIEZA INDIVIDUAL (por cada archivo)
        for (const excelFile of excelArchivos) {
            console.log(`\n[PROCESANDO] Archivo: ${excelFile}`);
            
            // a. Conversión (genera un .csv en csv-convertido/)
            const rawCsvFile = convertirExcelACsv(excelFile);

            // b. Limpieza (genera un _limpio.csv en csv-convertido/)
            await limpiarColumnasCsv(rawCsvFile, COLUMNAS_A_LIMPIAR);
        }

        // 3. UNIFICACIÓN DE CSVs
        const finalCsvFile = unirCsv();

        // 4. CARGA (Si está activada)
        if (CARGA_ACTIVADA && finalCsvFile) {
            console.log(`\n[CARGA] ⚠️ La carga a PostgreSQL está activada.`);
            // La función cargarCsvAPostgres debe ajustarse para buscar en 'csv-unidos'
            // Por simplicidad, asumiremos que cargador.js busca en el directorio final.
            await cargarCsvAPostgres(finalCsvFile); 
            console.log(`\n==========================================`);
            console.log(`  ✅ PROCESO COMPLETO FINALIZADO CON ÉXITO`);
            console.log(`  ¡Datos cargados en PostgreSQL!`);
            console.log(`==========================================`);
        } else {
            console.log(`\n[CARGA] 🛑 Carga a PostgreSQL DESACTIVADA.`);
            console.log(`[AUDITORÍA] Revisa el archivo final '${finalCsvFile}' en la carpeta 'csv-unidos'.`);
            console.log(`\n==========================================`);
            console.log(`  ✅ PROCESO DE GENERACIÓN FINALIZADO`);
            console.log(`==========================================`);
        }

    } catch (error) {
        console.error(`\n==========================================`);
        console.error(`  ❌ EL PROCESO ETL FALLÓ:`);
        console.error(`==========================================`);
        console.error(error.message);
    }
}

ejecutarProcesoETL();