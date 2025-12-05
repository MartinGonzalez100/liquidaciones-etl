// services/limpiador.js

const fs = require('fs');
const path = require('path');
const { parse } = require('csv-parse');
const { stringify } = require('csv-stringify');

const FILAS_PARA_ANALIZAR = 10; 

/**
 * Limpia los espacios finales y los formatos numéricos (positivos y negativos) de forma selectiva.
 */
function limpiarColumnasCsv(csvFileName, columnsToCleanSpaces) {
    const INPUT_DIR = path.join(__dirname, '..', 'csv-convertido');
    
    const inputPath = path.join(INPUT_DIR, csvFileName);
    const outputFileName = csvFileName.replace('.csv', '_limpio.csv');
    const outputPath = path.join(INPUT_DIR, outputFileName);
    
    console.log(`[LIMPIEZA] Iniciando limpieza inteligente de: ${csvFileName} (incluyendo negativos)`);

    const input = fs.readFileSync(inputPath, 'utf8');
    const columnasNumericas = new Set(); // Almacena los índices (base 0)

    return new Promise((resolve, reject) => {
        // --- 1. PRIMER PARSEO: Detección de columnas ---
        parse(input, {
            delimiter: ',',
        }, (err, records) => {
            if (err) return reject(new Error(`Error de parseo (Detección): ${err.message}`));
            if (records.length === 0) {
                 console.log("[LIMPIEZA] El archivo CSV está vacío.");
                 return resolve(csvFileName);
            }
            
            const filasDeDatos = records.slice(1, FILAS_PARA_ANALIZAR + 1);
            
            for (let c = 0; c < records[0].length; c++) {
                let esNumerica = false;
                
                for (let r = 0; r < filasDeDatos.length; r++) {
                    const valor = filasDeDatos[r][c];
                    if (!valor) continue; 

                    // CLAVE: Ajuste de RegEx para incluir signo negativo (-) opcional
                    // Patrón: opcionalmente espacios/comillas, opcionalmente '-', números, opcionalmente comas, opcionalmente decimales.
                    if (valor.match(/^\s*"?-?\d{1,3}(?:,\d{3})*(?:\.\d+)?"?\s*$/)) {
                        esNumerica = true;
                        break; 
                    }
                }
                
                if (esNumerica) {
                    columnasNumericas.add(c);
                }
            }

            console.log(`[LIMPIEZA] Columnas numéricas detectadas (Base 1): ${Array.from(columnasNumericas).map(i => i + 1).join(', ')}`);
            
            // --- 2. SEGUNDO PARSEO: Aplicar limpieza ---
            const inputParaLimpieza = fs.readFileSync(inputPath, 'utf8');
            const registrosLimpios = [];
            
            parse(inputParaLimpieza, {
                delimiter: ',',
                on_record: (record) => {
                    const registroModificado = record.map((valor, indice) => {
                        if (typeof valor !== 'string' || !valor) return valor;

                        const numeroColumnaBase1 = indice + 1; 

                        // 1. Limpieza de Espacios (según lista manual)
                        if (columnsToCleanSpaces.includes(numeroColumnaBase1)) {
                            valor = valor.replace(/\s+$/, '');
                        }

                        // 2. Limpieza de Formato Numérico (detección automática)
                        if (columnasNumericas.has(indice)) {
                            
                            // ELIMINACIÓN DE FORMATO: Quitamos comillas y comas (separador de miles)
                            // Esto transforma " -382,372.17" en -382372.17, que es lo que espera PostgreSQL.
                            valor = valor.replace(/"/g, ''); 
                            valor = valor.replace(/,/g, ''); 
                            
                            // Usamos trim() para eliminar cualquier espacio residual antes/después del número
                            return valor.trim();
                        }

                        return valor;
                    });

                    registrosLimpios.push(registroModificado);
                    return null;
                }
            }, (err) => {
                if (err) return reject(new Error(`Error de parseo (Limpieza): ${err.message}`));

                stringify(registrosLimpios, { delimiter: ',' }, (err, output) => {
                    if (err) return reject(new Error(`Error de stringify CSV: ${err.message}`));
                    
                    fs.writeFileSync(outputPath, output, 'utf8');
                    console.log(`[LIMPIEZA] ✅ Limpieza finalizada. Archivo final: ${outputFileName}`);
                    resolve(outputFileName);
                });
            });
        });
    });
}

module.exports = { limpiarColumnasCsv };