const http = require('http');

const url = 'http://localhost:3000/api/preparar-acumulado?tope=0.10';

http.get(url, (res) => {
    let data = '';
    res.on('data', (chunk) => { data += chunk; });
    res.on('end', () => {
        const results = JSON.parse(data);
        if (results.length > 0) {
            const first = results[0];
            console.log('--- Primers registros ---');
            console.log(JSON.stringify(first, null, 2));
            
            // Buscar un DNI que aparezca más de una vez
            const counts = {};
            results.forEach(r => {
                const dni = r.NRO_DOCUMENTO;
                counts[dni] = (counts[dni] || 0) + 1;
            });
            
            const duplicates = Object.keys(counts).filter(k => counts[k] > 1);
            if (duplicates.length > 0) {
                console.log('\n--- Ejemplo de duplicado ---');
                const exampleDni = duplicates[0];
                const rows = results.filter(r => r.NRO_DOCUMENTO === exampleDni);
                rows.forEach((r, i) => {
                    console.log(`Fila ${i+1}: DNI=${r.NRO_DOCUMENTO}, duplicado=${r.duplicado}`);
                });
            } else {
                console.log('\nNo se encontraron DNIs duplicados en estos datos.');
            }
        } else {
            console.log('No se devolvieron resultados.');
        }
    });
}).on('error', (err) => {
    console.error('Error:', err.message);
});
