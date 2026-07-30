const fs = require('fs');
const path = require('path');

const SOURCE_DIR = process.cwd();
const TARGET_DIR = path.join(SOURCE_DIR, 'dist-portable');

// Función recursiva para copiar directorios
function copyDirSync(src, dest) {
    if (!fs.existsSync(dest)) {
        fs.mkdirSync(dest, { recursive: true });
    }
    const entries = fs.readdirSync(src, { withFileTypes: true });

    for (const entry of entries) {
        const srcPath = path.join(src, entry.name);
        const destPath = path.join(dest, entry.name);

        if (entry.isDirectory()) {
            copyDirSync(srcPath, destPath);
        } else {
            fs.copyFileSync(srcPath, destPath);
        }
    }
}

// Limpiar carpeta destino si existe
if (fs.existsSync(TARGET_DIR)) {
    console.log('[PORTABLE] 🧹 Limpiando carpeta portable anterior...');
    fs.rmSync(TARGET_DIR, { recursive: true, force: true });
}

console.log('[PORTABLE] 📁 Creando estructura de carpeta portable...');
fs.mkdirSync(TARGET_DIR, { recursive: true });

// 1. Copiar ejecutables y scripts principales
const filesToCopy = ['lanzador.vbs'];
filesToCopy.forEach(file => {
    const srcPath = path.join(SOURCE_DIR, file);
    const destPath = path.join(TARGET_DIR, file);
    if (fs.existsSync(srcPath)) {
        fs.copyFileSync(srcPath, destPath);
    }
});

// Copiar aud-server.exe desde dist/
const exeSrc = path.join(SOURCE_DIR, 'dist', 'aud-server.exe');
const exeDest = path.join(TARGET_DIR, 'aud-server.exe');
if (fs.existsSync(exeSrc)) {
    fs.copyFileSync(exeSrc, exeDest);
} else {
    console.error('[PORTABLE] ❌ ERROR: No se encontró dist/aud-server.exe. Ejecuta "npm run build:exe" primero.');
    process.exit(1);
}

// 2. Copiar carpetas de recursos (public y configuracion_parametros)
console.log('[PORTABLE] 🚀 Copiando recursos de interfaz y configuración...');
copyDirSync(path.join(SOURCE_DIR, 'public'), path.join(TARGET_DIR, 'public'));
copyDirSync(path.join(SOURCE_DIR, 'configuracion_parametros'), path.join(TARGET_DIR, 'configuracion_parametros'));

// 3. Crear carpetas de trabajo vacías indispensables
console.log('[PORTABLE] 🛠️ Creando carpetas de trabajo vacías...');
const emptyDirs = [
    'excel-a-convertir',
    'excel-a-convertir-novedades',
    'csv-convertido',
    'csv-unidos',
    'csv-unidos-novedades',
    'uploads'
];

emptyDirs.forEach(dir => {
    fs.mkdirSync(path.join(TARGET_DIR, dir), { recursive: true });
});

console.log('\n[PORTABLE]  ¡Carpeta portable creada exitosamente en "dist-portable"!');
console.log('[PORTABLE] 💡 Ahora puedes comprimir "dist-portable" en un archivo ZIP y compartirlo.');
