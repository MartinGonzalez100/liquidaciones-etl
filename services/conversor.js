// services/conversor.js

const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');

/**
 * Convierte un archivo Excel a CSV y lo guarda.
 * @param {string} excelFileName - Nombre del archivo de Excel (ej: 'datos.xlsx').
 * @returns {string} El nombre del archivo CSV creado (ej: 'datos.csv').
 */
function convertirExcelACsv(excelFileName) {
    // Rutas de archivos y carpetas
    const INPUT_DIR = path.join(__dirname, '..', 'excel-a-convertir');
    const OUTPUT_DIR = path.join(__dirname, '..', 'csv-convertido');
    
    // Asegurarse de que el directorio de salida exista
    if (!fs.existsSync(OUTPUT_DIR)) {
        fs.mkdirSync(OUTPUT_DIR);
    }

    const inputPath = path.join(INPUT_DIR, excelFileName);
    const csvFileName = excelFileName.replace(/\.(xlsx|xls)$/i, '.csv');
    const outputPath = path.join(OUTPUT_DIR, csvFileName);

    console.log(`[CONVERSIÓN] Iniciando conversión de: ${excelFileName}`);

    try {
        const workbook = XLSX.readFile(inputPath);
        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];

        // Convertir la hoja de cálculo a formato CSV con delimitador coma
        const csv = XLSX.utils.sheet_to_csv(worksheet, { FS: ',' });

        // Guardar la cadena CSV en el nuevo archivo
        fs.writeFileSync(outputPath, csv, 'utf8');

        console.log(`[CONVERSIÓN] ✅ Archivo CSV creado exitosamente: ${csvFileName}`);
        return csvFileName;

    } catch (error) {
        console.error(`[CONVERSIÓN] ❌ ERROR: Asegúrate de que el archivo existe en ${INPUT_DIR}`);
        throw new Error(`Error en la conversión: ${error.message}`);
    }
}

module.exports = { convertirExcelACsv };