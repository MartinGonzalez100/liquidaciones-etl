const fs = require('fs');
const path = require('path');

let content = fs.readFileSync('c:/proyecto-aud/server.js', 'utf8');

const injection = \
async function generateAuxLDNovedadesPorImporte() {
    const ldCsvPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'ld.csv');
    const auxPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxLDNovedadesPorImporte.csv');

    if (!fs.existsSync(ldCsvPath)) throw new Error('Archivo origen de Novedades LD no encontrado');

    const groupedData = new Map();

    await new Promise((resolve, reject) => {
        fs.createReadStream(ldCsvPath, { encoding: 'utf8' })
            .pipe(csv({ separator: ',', mapHeaders: ({ header }) => header.trim() }))
            .on('data', (row) => {
                const estado = (row['ESTADO'] || '').trim().toLowerCase();
                const diasEpsStr = (row['DIAS EPS'] || '0').trim().replace(/,/g, '.');
                const diasEps = parseFloat(diasEpsStr);
                
                if (estado === 'aprobado' && !isNaN(diasEps) && diasEps > 0) {
                    const dni = (row['DNI'] || '').trim();
                    const nombre = (row['APELLIDO Y NOMBRE'] || '').trim();
                    const importeStr = (row['IMPORTE LIQUIDADO'] || '0').trim().replace(/,/g, '.');
                    const importe = parseFloat(importeStr) || 0;
                    
                    if (dni) {
                        if (!groupedData.has(dni)) {
                            groupedData.set(dni, { dni, nombre, suma: 0 });
                        }
                        groupedData.get(dni).suma += importe;
                    }
                }
            })
            .on('end', resolve)
            .on('error', reject);
    });

    const headers = ['DNI', 'APELLIDO Y NOMBRE', 'SUMA_IMPORTE'];
    const results = Array.from(groupedData.values()).map(r => ({
        DNI: r.dni,
        'APELLIDO Y NOMBRE': r.nombre,
        SUMA_IMPORTE: r.suma.toFixed(2)
    }));

    const csvContent = [
        headers.join(','),
        ...results.map(row => headers.map(h => {
            const str = String(row[h] !== undefined && row[h] !== null ? row[h] : '');
            return str.includes(',') ? \"\"\ : str;
        }).join(','))
    ].join('\\n');

    fs.writeFileSync(auxPath, csvContent, 'utf8');
    console.log('[SERVER] 📊 AuxLDNovedadesPorImporte.csv generado con ' + results.length + ' registros.');
}

app.get('/api/novedades/ld-importes-dni', async (req, res) => {
    try {
        const auxPath = path.join(CSV_UNIDOS_NOVEDADES_DIR, 'AuxLDNovedadesPorImporte.csv');
        if (!fs.existsSync(auxPath)) {
            await generateAuxLDNovedadesPorImporte();
        }
        
        const results = [];
        fs.createReadStream(auxPath, { encoding: 'utf8' })
            .pipe(csv())
            .on('data', (data) => results.push(data))
            .on('end', () => res.json(results))
            .on('error', (err) => res.status(500).json({ error: err.message }));
    } catch (error) {
        console.error('[SERVER] ❌ Error en /api/novedades/ld-importes-dni:', error.message);
        res.status(500).json({ error: error.message });
    }
});

\;

const targetStr = "app.get('/api/novedades/gc-para-control'";
content = content.replace(targetStr, injection + targetStr);
fs.writeFileSync('c:/proyecto-aud/server.js', content, 'utf8');
console.log('Update complete');
