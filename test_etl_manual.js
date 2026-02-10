
const { ejecutarProcesoETL } = require('./etl_runner');
const fs = require('fs');
const path = require('path');

const EXCEL_DIR = path.join(__dirname, 'excel-a-convertir');

async function test() {
    console.log("Starting Manual ETL Test...");
    try {
        const files = fs.readdirSync(EXCEL_DIR).filter(file => file.match(/\.(xlsx|xls)$/i));
        console.log("Files found:", files);
        
        if (files.length === 0) {
            console.log("No files to process.");
            return;
        }

        const result = await ejecutarProcesoETL(files);
        console.log("ETL Result:", result);
    } catch (e) {
        console.error("Test failed:", e);
    }
}

test();
