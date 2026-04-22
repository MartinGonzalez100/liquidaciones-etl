const fs = require('fs');

function normalizeStr(str) {
    if (!str) return '';
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
}

const testVal = "LAVCRÍTICA MÉDICOSA";
const normalizedTestVal = normalizeStr(testVal);

console.log(`Original Test: "${testVal}"`);
console.log(`Normalized Test: "${normalizedTestVal}"`);

const configPath = 'c:\\proyecto-aud\\configuracion_parametros\\GC_config_codigos_importes.csv';
const content = fs.readFileSync(configPath, 'latin1');
const lines = content.split('\n');

console.log('\n--- Buscando en CSV (Latin1) ---');
lines.forEach((line, index) => {
    if (index === 0) return;
    const parts = line.split(';');
    const claveUnica = (parts[0] || '').trim();
    const normalizedClave = normalizeStr(claveUnica);
    
    if (index === 1) { // Línea 2
        console.log(`Línea ${index + 1}:`);
        console.log(`  Original CSV: "${claveUnica}"`);
        console.log(`  Normalized CSV: "${normalizedClave}"`);
        if (normalizedClave === normalizedTestVal) {
            console.log('  >>> ¡COINCIDENCIA ENCONTRADA! <<<');
        } else {
            console.log('  No coincide.');
        }
    }
});
