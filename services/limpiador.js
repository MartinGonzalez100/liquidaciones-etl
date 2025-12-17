// services/limpiador.js

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const FILAS_PARA_ANALIZAR = 10; 

/**
 * Limpia los espacios y formatos numéricos por nombre de columna.
 * @param {string} csvFileName - Nombre del archivo CSV.
 * @param {string[]} columnNamesToClean - Lista de nombres de encabezados a limpiar.
 */
function limpiarColumnasCsv(csvFileName, columnNamesToClean) {
    const INPUT_DIR = path.join(__dirname, '..', 'csv-convertido');
    
    // Definimos las rutas aquí para que estén disponibles en toda la función
    const inputPath = path.join(INPUT_DIR, csvFileName);
    const outputFileName = csvFileName.replace('.csv', '_limpio.csv');
    const outputPath = path.join(INPUT_DIR, outputFileName);
    
    console.log(`[LIMPIEZA] Iniciando limpieza por nombre en: ${csvFileName}`);

    const input = fs.readFileSync(inputPath, 'utf8');
    const columnasNumericas = new Set(); // Índices detectados automáticamente
    const nombresParaLimpiar = new Set(columnNamesToClean);

    return new Promise((resolve, reject) => {
        // --- 1. PRIMER PARSEO: Detección automática de columnas numéricas ---
        parse(input, { delimiter: ',' }, (err, records) => {
            if (err) return reject(new Error(`Error de parseo (Detección): ${err.message}`));
            if (records.length === 0) return resolve(csvFileName);

            const filasATestear = records.slice(0, FILAS_PARA_ANALIZAR);
            const numColumnas = records[0].length;

            for (let i = 0; i < numColumnas; i++) {
                let esProbableNumerica = false;
                for (let j = 1; j < filasATestear.length; j++) {
                    let valor = filasATestear[j][i] ? filasATestear[j][i].trim() : "";
                    // Detecta números, incluyendo negativos y formatos con comas
                    if (valor !== "" && /^-?[\d,.]+$/.test(valor)) {
                        esProbableNumerica = true;
                        break;
                    }
                }
                if (esProbableNumerica) columnasNumericas.add(i);
            }

            // --- 2. SEGUNDO PARSEO: Limpieza real usando nombres de columnas ---
            const registrosLimpios = [];

            parse(input, {
                delimiter: ',',
                columns: true, // Esto nos permite usar row['NOMBRE_COLUMNA']
                skip_empty_lines: true
            }, (err, records) => {
                if (err) return reject(new Error(`Error de parseo (Limpieza): ${err.message}`));

                records.forEach(row => {
                    const headers = Object.keys(row);
                    const registroModificado = headers.map((header, index) => {
                        let valor = row[header] || '';

                        // A. Limpieza por NOMBRE (especificado en etl_runner.js)
                        if (nombresParaLimpiar.has(header)) {
                            valor = valor.trim();
                        }

                        // B. Limpieza por ÍNDICE (Detección automática de montos)
                        if (columnasNumericas.has(index)) {
                            valor = valor.replace(/"/g, '').replace(/,/g, '').trim();
                        }

                        return valor;
                    });
                    registrosLimpios.push(registroModificado);
                });

                // Volvemos a insertar los encabezados al principio para el archivo final
                const encabezadosFinales = Object.keys(records[0]);
                registrosLimpios.unshift(encabezadosFinales);

                // --- 3. ESCRITURA DEL ARCHIVO ---
                stringify(registrosLimpios, { delimiter: ',' }, (err, output) => {
                    if (err) return reject(new Error(`Error de stringify: ${err.message}`));
                    
                    fs.writeFileSync(outputPath, output, 'utf8');
                    console.log(`[LIMPIEZA] ✅ Finalizado por nombre: ${outputFileName}`);
                    resolve(outputFileName);
                });
            });
        });
    });
}

module.exports = { limpiarColumnasCsv };