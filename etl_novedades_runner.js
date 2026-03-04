// etl_novedades_runner.js

const fs = require('fs');
const path = require('path');
const { convertirExcelACsvNovedades } = require('./services/conversor_novedades');
const { limpiarYNombrarNovedadesCsv } = require('./services/limpiador_novedades');

const PREFIJOS_VALIDOS = ['ld', 'reem', 'gc', 'residentes', 'criticidad', 'fortalecimiento'];

async function ejecutarProcesoNovedades(excelFilesToProcess) {
    let unprocessedFiles = [];
    let filesByPrefix = {};

    try {
        console.log(`\n==========================================`);
        console.log(`  INICIO DEL PROCESO ETL NOVEDADES`);
        console.log(`  Archivos a procesar: ${excelFilesToProcess.length}`);
        console.log(`==========================================`);

        if (excelFilesToProcess.length === 0) {
            return { success: false, message: "No se encontraron archivos de Excel válidos para procesar." };
        }

        // 1. CLASIFICACIÓN DE ARCHIVOS
        for (const file of excelFilesToProcess) {
            const lowerFile = file.toLowerCase();
            let matchedPrefix = null;

            for (const prefix of PREFIJOS_VALIDOS) {
                if (lowerFile.startsWith(prefix)) {
                    matchedPrefix = prefix;
                    break;
                }
            }

            if (!matchedPrefix) {
                unprocessedFiles.push(`${file} (Prefijo inválido)`);
            } else {
                if (!filesByPrefix[matchedPrefix]) {
                    filesByPrefix[matchedPrefix] = [];
                }
                filesByPrefix[matchedPrefix].push(file);
            }
        }

        // 2. VALIDACIÓN DE DUPLICADOS
        const validFilesToProcess = [];
        for (const prefix in filesByPrefix) {
            const files = filesByPrefix[prefix];
            if (files.length > 1) {
                // Hay múltiples archivos con el mismo prefijo
                files.forEach(f => unprocessedFiles.push(`${f} (Múltiples archivos para el prefijo '${prefix}')`));
            } else {
                validFilesToProcess.push({ file: files[0], prefix: prefix });
            }
        }

        // 3. PROCESAMIENTO
        let filesProcessedCount = 0;
        for (const item of validFilesToProcess) {
            console.log(`\n[PROCESANDO] Archivo Novedades: ${item.file} -> ${item.prefix}.csv`);

            // a. Conversión a CSV temporal
            const rawCsvPath = convertirExcelACsvNovedades(item.file);

            // b. Limpieza (trim espacios) y guardado en csv-unidos-novedades como <prefix>.csv
            await limpiarYNombrarNovedadesCsv(rawCsvPath, item.prefix);

            filesProcessedCount++;
        }

        return {
            success: true,
            message: `Proceso completado. ${filesProcessedCount} archivos procesados.`,
            unprocessedFiles: unprocessedFiles.length > 0 ? unprocessedFiles : null
        };

    } catch (error) {
        console.error(`\n==========================================`);
        console.error(`  ❌ EL PROCESO ETL FEN NOVEDADES FALLÓ:`);
        console.error(`==========================================`);
        console.error(error.message);
        return { success: false, message: `El proceso falló: ${error.message}` };
    }
}

module.exports = { ejecutarProcesoNovedades };
