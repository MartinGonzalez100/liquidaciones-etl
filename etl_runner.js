// etl_runner.js

const fs = require('fs');
const path = require('path');
const { convertirExcelACsv } = require('./services/conversor');
const { limpiarColumnasCsv } = require('./services/limpiador');
const { unirCsv } = require('./services/unificador');
const { cargarCsvAPostgres } = require('./services/cargador'); 

// --- CONFIGURACIÓN PRINCIPAL ---
const CARGA_ACTIVADA = false; 

// Columnas de TEXTO con espacios (base 1) que necesitan limpieza
/*const COLUMNAS_A_LIMPIAR = [
    1,2,193,194,195,202,205,209,212,213,214,216
    // ... AJUSTA ESTA LISTA
];*/

const NOMBRES_COLUMNAS_A_LIMPIAR = [
    // Campos de texto que pueden tener espacios (Ej: Planilla y Descripciones)
    'NIVEL', 'DESCAGENTE', 'PLANTA', 'ORGANISMO', 'FUNCION', 'AGRUPAMIENTO', 'OB_ALTA', 'OB_BAJA', 'AREA'
, 'SEXO', 'TIT_EDUCATIVO', 'AREA_TEMATICA', 'DURACION', 'ESTADO_LIQUIDACION'




 


    /*
    // Campos numéricos/de monto que deben limpiarse de comas/comillas
    'HAB_C_AP', 'HAB_S_AP', 'ASIG_FAM', 'TOT_HAB', 'RETENCIONES', 'DESCUENTOS', 'LIQUIDO', 
    'BRUTO_LEY7991', 'PENSION_229_92', 'LIQUIDO_LEY7991', 'COSTO_LABORAL_01', 
    'COSTO_LABORAL_02', 'SUELDO_MANO', 'SUELDO', 'MONTO_ANTIGUEDAD', 'AP100_090_54' 
    // ... Incluye aquí todos los encabezados relevantes.
    */
];




// ------------------------------

async function ejecutarProcesoETL(excelFilesToProcess) {
    try {
        console.log(`\n==========================================`);
        console.log(`  INICIO DEL PROCESO ETL A POSTGRESQL`);
        console.log(`  Archivos a procesar: ${excelFilesToProcess.length}`);
        console.log(`==========================================`);

        if (excelFilesToProcess.length === 0) {
            return { success: false, message: "No se encontraron archivos de Excel válidos para procesar." };
        }

        // 1. CONVERSIÓN Y LIMPIEZA INDIVIDUAL (por cada archivo)
        for (const excelFile of excelFilesToProcess) {
            console.log(`\n[PROCESANDO] Archivo: ${excelFile}`);
            
            // a. Conversión (asume que el conversor encuentra el archivo en excel-a-convertir/)
            const rawCsvFile = convertirExcelACsv(excelFile);

            // b. Limpieza
            await limpiarColumnasCsv(rawCsvFile, NOMBRES_COLUMNAS_A_LIMPIAR);
        }

        // 2. UNIFICACIÓN DE CSVs
        const finalCsvFile = unirCsv();

        if (!finalCsvFile) {
             return { success: false, message: "El proceso se detuvo porque no se pudo unificar ningún CSV." };
        }

        // 3. CARGA (Condicional de seguridad)
        if (CARGA_ACTIVADA) {
            console.log(`\n[CARGA] ⚠️ La carga a PostgreSQL está activada.`);
            await cargarCsvAPostgres(finalCsvFile);
            return { success: true, message: `Proceso completado. Datos cargados en PostgreSQL.` };
        } else {
            console.log(`\n[CARGA] 🛑 La carga a PostgreSQL está DESACTIVADA (Modo Auditoría).`);
            return { 
                success: true, 
                message: `Proceso completado en Modo Auditoría. Revisa el archivo '${finalCsvFile}' en la carpeta 'csv-unidos'.`,
                file: finalCsvFile
            };
        }

    } catch (error) {
        console.error(`\n==========================================`);
        console.error(`  ❌ EL PROCESO ETL FALLÓ:`);
        console.error(`==========================================`);
        console.error(error.message);
        return { success: false, message: `El proceso falló: ${error.message}` };
    }
}

module.exports = { ejecutarProcesoETL };