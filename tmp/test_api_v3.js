const http = require('http');

const url = 'http://localhost:3000/api/preparar-acumulado?tope=0.10';

http.get(url, (res) => {
    let data = '';
    res.on('data', (chunk) => { data += chunk; });
    res.on('end', () => {
        const results = JSON.parse(data);
        if (results.length > 0) {
            console.log('--- Verificando ordenamiento ---');
            
            let ordered = true;
            for (let i = 0; i < results.length - 1; i++) {
                const a = results[i];
                const b = results[i+1];
                
                // 1. NRO_DOCUMENTO asc
                const docA = (a.NRO_DOCUMENTO || "").trim();
                const docB = (b.NRO_DOCUMENTO || "").trim();
                if (docA !== docB) {
                    if (docA.localeCompare(docB, undefined, { numeric: true }) > 0) {
                        console.error(`Error en NRO_DOCUMENTO (asc): Fila ${i} > Fila ${i+1} (${docA} > ${docB})`);
                        ordered = false;
                        break;
                    }
                    continue;
                }
                
                // 2. ESTADO_LIQUIDACION asc
                const estA = (a.ESTADO_LIQUIDACION || "").trim();
                const estB = (b.ESTADO_LIQUIDACION || "").trim();
                if (estA !== estB) {
                    if (estA.localeCompare(estB) > 0) {
                        console.error(`Error en ESTADO_LIQUIDACION (asc): Fila ${i} > Fila ${i+1} (${estA} > ${estB})`);
                        ordered = false;
                        break;
                    }
                    continue;
                }
                
                // 3. PLANTASS desc
                const plantaA = (a.PLANTASS || "").trim();
                const plantaB = (b.PLANTASS || "").trim();
                if (plantaA !== plantaB) {
                    if (plantaA.localeCompare(plantaB) < 0) {
                        console.error(`Error en PLANTASS (desc): Fila ${i} < Fila ${i+1} (${plantaA} < ${plantaB})`);
                        ordered = false;
                        break;
                    }
                    continue;
                }
                
                // 4. ApJubPer desc
                const apA = parseFloat(a.ApJubPer) || 0;
                const apB = parseFloat(b.ApJubPer) || 0;
                if (apA !== apB) {
                    if (apA < apB) {
                        console.error(`Error en ApJubPer (desc): Fila ${i} < Fila ${i+1} (${apA} < ${apB})`);
                        ordered = false;
                        break;
                    }
                }
            }
            
            if (ordered) {
                console.log('✅ El ordenamiento es CORRECTO.');
            } else {
                console.log('❌ El ordenamiento tiene errores.');
            }
            
            // Mostrar los primeros registros para verificar visualmente
            console.log('\n--- Primeros 5 registros ---');
            results.slice(0, 5).forEach(r => {
                console.log(`DNI: ${r.NRO_DOCUMENTO}, ESTADO: ${r.ESTADO_LIQUIDACION}, PLANTASS: ${r.PLANTASS}, ApJubPer: ${r.ApJubPer}`);
            });
            
        } else {
            console.log('No se devolvieron resultados.');
        }
    });
}).on('error', (err) => {
    console.error('Error:', err.message);
});
