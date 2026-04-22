const fs = require('fs');
const path = require('path');

function normalizeStr(str) {
    if (!str) return '';
    return str.normalize("NFD").replace(/[\u0300-\u036f]/g, "").toUpperCase().trim();
}

const testVal = "LAVCRÍTICA MÉDICOSA";
const normalizedTestVal = normalizeStr(testVal);

console.log(`Original: "${testVal}"`);
console.log(`Normalized: "${normalizedTestVal}"`);

const configPath = 'c:\\proyecto-aud\\configuracion_parametros\\GC_config_codigos_importes.csv';
const content = fs.readFileSync(configPath, 'utf8');
const lines = content.split('\n');

console.log('\n--- Buscando en CSV ---');
lines.forEach((line, index) => {
    if (index === 0) return; // Skip header
    const parts = line.split(';');
    const claveUnica = (parts[0] || '').trim();
    const normalizedClave = normalizeStr(claveUnica);
    
    if (normalizedClave.includes('CRITICA') || index < 5) {
        console.log(`Línea ${index + 1}:`);
        console.log(`  Original CSV: "${claveUnica}"`);
        console.log(`  Normalized CSV: "${normalizedClave}"`);
        if (normalizedClave === normalizedTestVal) {
            console.log('  >>> ¡COINCIDENCIA ENCONTRADA! <<<');
        }
    }
});
