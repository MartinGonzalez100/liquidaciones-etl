// services/limpiador_novedades.js

const fs = require('fs');
const path = require('path');
const csv = require('csv-parser');

async function limpiarYNombrarNovedadesCsv(inputCsvPath, prefix) {
    return new Promise((resolve, reject) => {
        const OUTPUT_DIR = path.join(process.cwd(), 'csv-unidos-novedades');
        if (!fs.existsSync(OUTPUT_DIR)) {
            fs.mkdirSync(OUTPUT_DIR);
        }

        const finalCsvName = `${prefix}.csv`;
        const outputPath = path.join(OUTPUT_DIR, finalCsvName);

        console.log(`[LIMPIEZA NOVEDADES] Iniciando limpieza en ${inputCsvPath}`);

        const results = [];
        let headersSet = false;
        let headers = [];

        fs.createReadStream(inputCsvPath)
            .pipe(csv())
            .on('headers', (h) => {
                // Quitamos espacios en los headers también por seguridad
                headers = h.map(header => header.trim());
            })
            .on('data', (row) => {
                const cleanedRow = {};
                for (const key in row) {
                    if (Object.hasOwnProperty.call(row, key)) {
                        // Limpiar la clave por si venia mal del stream, usar nuestro header
                        const cleanKey = key.trim();
                        // Trimear el contenido de la celda si es string
                        let val = row[key];
                        if (typeof val === 'string') {
                            val = val.trimRight(); // Quitar los espacios en blanco de la derecha específicamente, o trim total. 
                            // Uso trim() que es mas seguro, pero cumplimos quitando los blancos a la derecha.
                            val = val.trim();
                        }
                        cleanedRow[cleanKey] = val;
                    }
                }
                results.push(cleanedRow);
            })
            .on('end', () => {
                try {
                    console.log(`[LIMPIEZA NOVEDADES] Escritura del archivo final: ${finalCsvName}`);

                    if (results.length === 0 && headers.length === 0) {
                        // Archivo vacío
                        fs.writeFileSync(outputPath, '', 'utf8');
                    } else if (results.length === 0 && headers.length > 0) {
                        fs.writeFileSync(outputPath, headers.join(',') + '\n', 'utf8');
                    } else {
                        // Recuperamos los headers del primer objeto ya limpio, en caso que no haya match
                        const finalHeaders = Object.keys(results[0]);

                        const csvLines = [];
                        csvLines.push(finalHeaders.join(','));

                        for (const r of results) {
                            const line = finalHeaders.map(h => {
                                let val = r[h] !== null && r[h] !== undefined ? String(r[h]) : '';
                                // Escapar comas
                                if (val.includes(',') || val.includes('"')) {
                                    val = `"${val.replace(/"/g, '""')}"`;
                                }
                                return val;
                            });
                            csvLines.push(line.join(','));
                        }

                        fs.writeFileSync(outputPath, csvLines.join('\n'), 'utf8');
                    }

                    // Intentar borrar el temporal si se limpio correctamente
                    if (fs.existsSync(inputCsvPath)) {
                        fs.unlinkSync(inputCsvPath);
                    }

                    resolve(outputPath);
                } catch (e) {
                    reject(e);
                }
            })
            .on('error', (error) => {
                console.error(`[LIMPIEZA NOVEDADES] ❌ Error limpiando el csv temporal`);
                reject(error);
            });
    });
}

module.exports = { limpiarYNombrarNovedadesCsv };
