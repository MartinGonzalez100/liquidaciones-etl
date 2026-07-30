// services/conversor_novedades.js

const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');

/**
 * Convierte un archivo Excel de Novedades a CSV y lo guarda temporalmente.
 * @param {string} excelFileName - Nombre del archivo de Excel.
 * @returns {string} El path absoluto del archivo CSV temporal creado.
 */
function convertirExcelACsvNovedades(excelFileName) {
    const INPUT_DIR = path.join(process.cwd(), 'excel-a-convertir-novedades');
    const OUTPUT_DIR = path.join(process.cwd(), 'csv-convertido');

    if (!fs.existsSync(OUTPUT_DIR)) {
        fs.mkdirSync(OUTPUT_DIR);
    }

    const inputPath = path.join(INPUT_DIR, excelFileName);
    const csvFileName = `temp_nov_${Date.now()}_${excelFileName.replace(/\.(xlsx|xls)$/i, '.csv')}`;
    const outputPath = path.join(OUTPUT_DIR, csvFileName);

    console.log(`[CONVERSIÓN NOVEDADES] Iniciando conversión de: ${excelFileName}`);

    try {
        const workbook = XLSX.readFile(inputPath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];

        // --- LIMPIEZA DE CELDAS: Reemplazar saltos de línea para evitar romper el CSV ---
        Object.keys(worksheet).forEach(cellId => {
            if (cellId[0] === '!') return; // Ignorar metadatos
            const cell = worksheet[cellId];
            if (cell.v && typeof cell.v === 'string') {
                cell.v = cell.v.replace(/[\r\n]+/g, ' ');
                // Actualizar el valor formateado si existe
                if (cell.w) {
                    cell.w = cell.w.replace(/[\r\n]+/g, ' ');
                }
            }
        });

        const csv = XLSX.utils.sheet_to_csv(worksheet, { FS: ',' });

        fs.writeFileSync(outputPath, csv, 'utf8');

        console.log(`[CONVERSIÓN NOVEDADES] ✅ Archivo temporal CSV creado.`);
        return outputPath;

    } catch (error) {
        console.error(`[CONVERSIÓN NOVEDADES] ❌ ERROR convirtiendo ${excelFileName}`);
        throw new Error(`Error en la conversión temporal: ${error.message}`);
    }
}

module.exports = { convertirExcelACsvNovedades };
