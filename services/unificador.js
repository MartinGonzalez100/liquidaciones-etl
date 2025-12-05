// services/unificador.js

const fs = require('fs');
const path = require('path');

const CSV_CONVERTIDO_DIR = path.join(__dirname, '..', 'csv-convertido');
const CSV_UNIDOS_DIR = path.join(__dirname, '..', 'csv-unidos');

/**
 * Une todos los archivos CSV de una carpeta en un único archivo, manteniendo solo el primer encabezado.
 * @returns {string} El nombre del archivo CSV final unido.
 */
function unirCsv() {
    console.log(`\n[UNIFICADOR] Iniciando unificación de archivos CSV limpios...`);
    
    if (!fs.existsSync(CSV_UNIDOS_DIR)) {
        fs.mkdirSync(CSV_UNIDOS_DIR);
    }

    const archivosCsv = fs.readdirSync(CSV_CONVERTIDO_DIR)
        .filter(file => file.endsWith('_limpio.csv')); // Solo archivos ya limpios

    if (archivosCsv.length === 0) {
        console.warn(`[UNIFICADOR] ⚠️ No se encontraron archivos CSV limpios para unificar.`);
        return null;
    }

    const outputFileName = 'liquidaciones_unificadas.csv';
    const outputPath = path.join(CSV_UNIDOS_DIR, outputFileName);
    
    let contenidoUnificado = '';
    let encabezadoGuardado = false;

    archivosCsv.forEach((archivo) => {
        const filePath = path.join(CSV_CONVERTIDO_DIR, archivo);
        const contenido = fs.readFileSync(filePath, 'utf8');
        const lineas = contenido.split('\n').filter(line => line.trim() !== ''); // Eliminar líneas vacías

        if (lineas.length === 0) return; // Si el archivo está vacío, saltar

        if (!encabezadoGuardado) {
            // Guardar el primer encabezado y marcar como guardado
            contenidoUnificado += lineas[0] + '\n';
            encabezadoGuardado = true;
        }

        // Añadir las líneas de datos (desde la segunda línea)
        const datos = lineas.slice(encabezadoGuardado ? 1 : 0).join('\n');
        contenidoUnificado += datos + '\n';
    });

    // Guardar el archivo final
    fs.writeFileSync(outputPath, contenidoUnificado.trim(), 'utf8');

    console.log(`[UNIFICADOR] ✅ Unificación completada. Archivo final: ${outputFileName}`);
    return outputFileName;
}

module.exports = { unirCsv };