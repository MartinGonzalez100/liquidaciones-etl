// services/conversor.js

const XLSX = require('xlsx');
const path = require('path');
const fs = require('fs');

/**
 * Verifica si un workbook de Excel contiene filtros activos (!autofilter),
 * filas/columnas ocultas (!rows / !cols con hidden: true) o nombres definidos de filtro (_FilterDatabase).
 * @param {object} workbook - Objeto workbook de XLSX.
 * @returns {boolean} true si detecta algún filtro o fila/columna oculta.
 */
function detectarFiltrosEnWorkbook(workbook) {
    if (!workbook || !Array.isArray(workbook.SheetNames)) return false;

    for (const sheetName of workbook.SheetNames) {
        const worksheet = workbook.Sheets[sheetName];
        if (!worksheet) continue;

        // 1. Verificar autofiltro en la hoja
        if (worksheet['!autofilter']) {
            return true;
        }

        // 2. Verificar filas ocultas (ej. filas ocultadas por un filtro activo)
        if (Array.isArray(worksheet['!rows']) && worksheet['!rows'].some(row => row && row.hidden)) {
            return true;
        }

        // 3. Verificar columnas ocultas
        if (Array.isArray(worksheet['!cols']) && worksheet['!cols'].some(col => col && col.hidden)) {
            return true;
        }
    }

    // 4. Verificar nombre definido de base de datos de filtro en el libro
    if (
        workbook.Workbook &&
        Array.isArray(workbook.Workbook.Names) &&
        workbook.Workbook.Names.some(n => n && n.Name && String(n.Name).includes('_FilterDatabase'))
    ) {
        return true;
    }

    return false;
}

/**
 * Limpia en memoria todos los filtros (!autofilter), filas/columnas ocultas y _FilterDatabase de un workbook.
 * @param {object} workbook - Objeto workbook de XLSX.
 * @returns {boolean} true si se realizó alguna modificación.
 */
function limpiarFiltrosDeWorkbook(workbook) {
    let modificado = false;

    if (!workbook || !Array.isArray(workbook.SheetNames)) return false;

    for (const sheetName of workbook.SheetNames) {
        const worksheet = workbook.Sheets[sheetName];
        if (!worksheet) continue;

        if (worksheet['!autofilter']) {
            delete worksheet['!autofilter'];
            modificado = true;
        }

        if (Array.isArray(worksheet['!rows'])) {
            worksheet['!rows'].forEach(row => {
                if (row && row.hidden) {
                    delete row.hidden;
                    modificado = true;
                }
            });
        }

        if (Array.isArray(worksheet['!cols'])) {
            worksheet['!cols'].forEach(col => {
                if (col && col.hidden) {
                    delete col.hidden;
                    modificado = true;
                }
            });
        }
    }

    if (workbook.Workbook && Array.isArray(workbook.Workbook.Names)) {
        const originalLength = workbook.Workbook.Names.length;
        workbook.Workbook.Names = workbook.Workbook.Names.filter(
            n => !(n && n.Name && String(n.Name).includes('_FilterDatabase'))
        );
        if (workbook.Workbook.Names.length !== originalLength) {
            modificado = true;
        }
    }

    return modificado;
}

/**
 * Recorre todos los archivos Excel en 'excel-a-convertir/' y les quita cualquier filtro activo o filas ocultas,
 * guardando los archivos actualizados en disco.
 * @param {string[]} [excelFiles] - Lista opcional de nombres de archivos Excel. Si se omite, revisa todos los de la carpeta.
 * @returns {{ procesados: number, desfiltrados: string[] }}
 */
function quitarFiltrosExcel(excelFiles) {
    const INPUT_DIR = path.join(process.cwd(), 'excel-a-convertir');
    if (!fs.existsSync(INPUT_DIR)) {
        return { procesados: 0, desfiltrados: [] };
    }

    const archivosEnCarpeta = fs.readdirSync(INPUT_DIR)
        .filter(file => file.match(/\.(xlsx|xls)$/i) && !file.startsWith('~$'));

    const archivos = Array.isArray(excelFiles) && excelFiles.length > 0
        ? Array.from(new Set([...archivosEnCarpeta, ...excelFiles.filter(f => !f.startsWith('~$'))]))
        : archivosEnCarpeta;

    const desfiltrados = [];

    console.log(`\n[FILTROS EXCEL] Analizando ${archivos.length} archivo(s) en 'excel-a-convertir/' para remover filtros...`);

    for (const excelFileName of archivos) {
        const inputPath = path.join(INPUT_DIR, excelFileName);
        if (!fs.existsSync(inputPath)) continue;

        try {
            const workbook = XLSX.readFile(inputPath, { cellNF: true });
            if (detectarFiltrosEnWorkbook(workbook)) {
                console.log(`[FILTROS EXCEL] ⚠️ Filtro detectado en '${excelFileName}'. Removiendo filtros y filas ocultas...`);
                limpiarFiltrosDeWorkbook(workbook);
                XLSX.writeFile(workbook, inputPath, { compression: true });
                desfiltrados.push(excelFileName);
                console.log(`[FILTROS EXCEL] ✅ Filtros removidos correctamente en '${excelFileName}'.`);
            } else {
                console.log(`[FILTROS EXCEL] ✔️ '${excelFileName}' no posee filtros activos.`);
            }
        } catch (error) {
            if (error.code === 'EBUSY' || error.code === 'EPERM') {
                throw new Error(`No se pudo quitar el filtro del archivo '${excelFileName}' porque se encuentra abierto en Excel u otro programa. Por favor, ciérrelo e intente nuevamente.`);
            }
            throw new Error(`Error al remover filtros en '${excelFileName}': ${error.message}`);
        }
    }

    return { procesados: archivos.length, desfiltrados };
}

/**
 * Comprueba que ningún archivo Excel en 'excel-a-convertir/' tenga filtros activos antes de iniciar el proceso.
 * @param {string[]} [excelFiles] - Lista opcional de nombres de archivos Excel. Si se omite, revisa todos los de la carpeta.
 * @returns {{ ok: boolean, archivosConFiltro: string[] }}
 */
function comprobarSinFiltrosExcel(excelFiles) {
    const INPUT_DIR = path.join(process.cwd(), 'excel-a-convertir');
    if (!fs.existsSync(INPUT_DIR)) {
        return { ok: true, archivosConFiltro: [] };
    }

    const archivosEnCarpeta = fs.readdirSync(INPUT_DIR)
        .filter(file => file.match(/\.(xlsx|xls)$/i) && !file.startsWith('~$'));

    const archivos = Array.isArray(excelFiles) && excelFiles.length > 0
        ? Array.from(new Set([...archivosEnCarpeta, ...excelFiles.filter(f => !f.startsWith('~$'))]))
        : archivosEnCarpeta;

    const archivosConFiltro = [];

    console.log(`[FILTROS EXCEL] 🔍 Comprobando ausencia de filtros en ${archivos.length} archivo(s) antes de iniciar el proceso...`);

    for (const excelFileName of archivos) {
        const inputPath = path.join(INPUT_DIR, excelFileName);
        if (!fs.existsSync(inputPath)) continue;

        const workbook = XLSX.readFile(inputPath, { cellNF: true });
        if (detectarFiltrosEnWorkbook(workbook)) {
            archivosConFiltro.push(excelFileName);
        }
    }

    if (archivosConFiltro.length > 0) {
        console.error(`[FILTROS EXCEL] ❌ Comprobación fallida. Archivos que aún contienen filtros: ${archivosConFiltro.join(', ')}`);
        return { ok: false, archivosConFiltro };
    }

    console.log(`[FILTROS EXCEL] ✅ Comprobación exitosa: Todos los archivos Excel están libres de filtros.`);
    return { ok: true, archivosConFiltro: [] };
}

/**
 * Convierte un archivo Excel a CSV y lo guarda.
 * @param {string} excelFileName - Nombre del archivo de Excel (ej: 'datos.xlsx').
 * @returns {string} El nombre del archivo CSV creado (ej: 'datos.csv').
 */
function convertirExcelACsv(excelFileName) {
    // Rutas de archivos y carpetas
    const INPUT_DIR = path.join(process.cwd(), 'excel-a-convertir');
    const OUTPUT_DIR = path.join(process.cwd(), 'csv-convertido');
    
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
        // Garantizar en memoria que no queden filtros ni filas ocultas
        limpiarFiltrosDeWorkbook(workbook);

        const sheetName = workbook.SheetNames[0];
        const worksheet = workbook.Sheets[sheetName];

        // Convertir la hoja de cálculo a formato CSV con delimitador coma y sin omitir filas ocultas
        const csv = XLSX.utils.sheet_to_csv(worksheet, { FS: ',', skipHidden: false });

        // Guardar la cadena CSV en el nuevo archivo
        fs.writeFileSync(outputPath, csv, 'utf8');

        console.log(`[CONVERSIÓN] ✅ Archivo CSV creado exitosamente: ${csvFileName}`);
        return csvFileName;

    } catch (error) {
        console.error(`[CONVERSIÓN] ❌ ERROR: Asegúrate de que el archivo existe en ${INPUT_DIR}`);
        throw new Error(`Error en la conversión: ${error.message}`);
    }
}

module.exports = {
    convertirExcelACsv,
    quitarFiltrosExcel,
    comprobarSinFiltrosExcel
};