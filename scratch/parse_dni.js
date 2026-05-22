const fs = require('fs');
const csv = require('csv-parser');
const path = require('path');

const csvPath = 'c:\\proyecto-aud\\csv-unidos\\liquidaciones_unificadas.csv';

fs.createReadStream(csvPath)
    .pipe(csv())
    .on('data', (row) => {
        if (row.NRO_DOCUMENTO === '27141416') {
            console.log(JSON.stringify(row, null, 2));
        }
    })
    .on('end', () => {
        console.log('Finished search.');
    });
