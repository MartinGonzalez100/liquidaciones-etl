const fs = require('fs');

const fpath = 'c:\\proyecto-aud\\public\\index.html';
let content = fs.readFileSync(fpath, 'utf8');

// 1. Add Menu Item
const menuMatchStr = '<a href="#" id="menu-detalle-vs-detalle" style="padding-left: 50px; font-size: 16px; color: #a29bfe;">└─ Detalle vs Detalle</a>';
content = content.replace(
    menuMatchStr,
    menuMatchStr + '\n                    <a href="#" id="menu-detalle-vs-detalle-importes" style="padding-left: 50px; font-size: 16px; color: #1abc9c;">└─ Detalle Importes (Solo Montos)</a>'
);

// 2. Clone View
const startIndex = content.indexOf('<!-- VISTA DETALLE VS DETALLE -->');
if (startIndex === -1) throw new Error("View comment missing!");
const nextViewIndex = content.indexOf('<script> //'); // The main script tag happens after all views.
const viewStr = content.substring(startIndex, nextViewIndex);
// But wait, the script tag is way too far, let's find the closing tag of the main container view
// We can find the very last </div> before <script>
const endOfView = content.lastIndexOf('</div>', nextViewIndex);
const oldView = content.substring(startIndex, endOfView + 6);

// Create new view by replacing IDs and removing code columns
let newView = oldView.replace(/detalle-vs-detalle-view/g, 'detalle-vs-detalle-importes-view');
newView = newView.replace(/VISTA: DETALLE VS DETALLE/g, 'VISTA: DETALLE VS DETALLE IMPORTES');
newView = newView.replace(/Detalle vs Detalle \(OCR\)/g, 'Detalle Vs Detalle (Solo Importes)');
newView = newView.replace(/img-anterior-container/g, 'img-anterior-container-importes');
newView = newView.replace(/img-actual-container/g, 'img-actual-container-importes');
newView = newView.replace(/preview-anterior/g, 'preview-anterior-importes');
newView = newView.replace(/preview-actual/g, 'preview-actual-importes');
newView = newView.replace(/btn-procesar-ocr/g, 'btn-procesar-ocr-importes');
newView = newView.replace(/ocr-status/g, 'ocr-status-importes');
newView = newView.replace(/ocr-results-container/g, 'ocr-results-container-importes');
newView = newView.replace(/ocr-table-sec1/g, 'ocr-table-sec1-importes');
newView = newView.replace(/ocr-table-sec2/g, 'ocr-table-sec2-importes');
newView = newView.replace(/input-umbral-porcentaje/g, 'input-umbral-porcentaje-importes');

// Remove <th>Código</th> from sec1 and sec2
newView = newView.replace(/<th style="padding: 10px;text-align: left;">Código<\/th>/g, '');

// Remove sec3 completely
const sec3Start = newView.indexOf('<h3 style="color: #e67e22; margin-top: 0;">3. Códigos No Coincidentes');
if(sec3Start !== -1) {
    const parentDivStart = newView.lastIndexOf('<div', sec3Start);
    const parentDivEnd = newView.indexOf('</div>', sec3Start) + 6;
    newView = newView.substring(0, parentDivStart) + newView.substring(parentDivEnd);
}

// Inject new view right after the old view.
content = content.substring(0, endOfView + 6) + '\n' + newView + '\n' + content.substring(endOfView + 6);

// 3. Add JS for Menu logic
content = content.replace(
    /showSection\('detalle-vs-detalle-view'\);\s*\}\);/g,
    `showSection('detalle-vs-detalle-view');\n        });\n\n        document.getElementById('menu-detalle-vs-detalle-importes').addEventListener('click', function (e) {\n            e.preventDefault();\n            showSection('detalle-vs-detalle-importes-view');\n        });`
);

// 4. Update the OCR Logic by adding the new functions at the end of the script tag.
const newLogic = `
        // --- LOGICA DE PEGAR IMAGENES Y OCR (DETALLE VS DETALLE IMPORTES) ---
        let imgBase64AnteriorImportes = null;
        let imgBase64ActualImportes = null;

        document.getElementById('img-anterior-container-importes').addEventListener('paste', function(e) {
            handlePaste(e, 'img-anterior-container-importes', 'preview-anterior-importes', v => imgBase64AnteriorImportes = v);
        });
        document.getElementById('img-actual-container-importes').addEventListener('paste', function(e) {
            handlePaste(e, 'img-actual-container-importes', 'preview-actual-importes', v => imgBase64ActualImportes = v);
        });

        document.getElementById('btn-procesar-ocr-importes').addEventListener('click', async function() {
            if (!imgBase64AnteriorImportes || !imgBase64ActualImportes) {
                alert('Debe pegar ambas imágenes de la columna Monto antes de procesar.');
                return;
            }

            const status = document.getElementById('ocr-status-importes');
            const btn = this;
            const resultsContainer = document.getElementById('ocr-results-container-importes');
            
            status.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Iniciando motor OCR...';
            btn.disabled = true;
            btn.style.opacity = '0.6';
            resultsContainer.style.display = 'none';

            try {
                status.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Pre-procesando imágenes (Filtro B/N y recorte)...';
                const processedAnt = await preprocessImageForOCR(imgBase64AnteriorImportes);
                const processedAct = await preprocessImageForOCR(imgBase64ActualImportes);

                status.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Analizando imagen Mes Anterior...';
                const workerAnt = await Tesseract.createWorker('spa');
                await workerAnt.setParameters({ tessedit_pageseg_mode: '6' });
                const retAnt = await workerAnt.recognize(processedAnt);
                const textAnt = retAnt.data.text;
                await workerAnt.terminate();

                status.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Analizando imagen Mes Actual...';
                const workerAct = await Tesseract.createWorker('spa');
                await workerAct.setParameters({ tessedit_pageseg_mode: '6' });
                const retAct = await workerAct.recognize(processedAct);
                const textAct = retAct.data.text;
                await workerAct.terminate();

                status.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Procesando datos extraídos...';
                
                processOcrDataImportes(textAnt, textAct);

                status.innerHTML = '<span style="color: #27ae60;">¡Proceso completo! Resultados listos.</span>';
                resultsContainer.style.display = 'block';
                
            } catch (err) {
                console.error(err);
                status.innerHTML = '<span style="color:#e74c3c;"><i class="fas fa-exclamation-triangle"></i> Error procesando imágenes. Revise la consola.</span>';
            } finally {
                btn.disabled = false;
                btn.style.opacity = '1';
            }
        });

        function parseOcrLineImportes(line) {
            const numPattern = /(-?\\d{1,7}[.,]\\d{2})\\s*$/;
            const montoMatch = line.match(numPattern);
            let monto = 0;
            if (montoMatch) {
                let montoStr = montoMatch[1].replace(',', '.');
                monto = parseFloat(montoStr);
                if (isNaN(monto)) monto = 0;
                return monto;
            }
            return null; // Return null if no valid amount found to ignore text lines
        }

        function extractDataFromTextImportes(text) {
            const lines = text.split('\\n');
            const arr = [];
            lines.forEach(line => {
                const monto = parseOcrLineImportes(line.trim());
                if (monto !== null) {
                    arr.push(monto);
                }
            });
            return arr;
        }

        function processOcrDataImportes(textAnt, textAct) {
            const dataAnt = extractDataFromTextImportes(textAnt);
            const dataAct = extractDataFromTextImportes(textAct);

            const thresholdInput = parseFloat(document.getElementById('input-umbral-porcentaje-importes').value) || 0;
            const umbralMin = thresholdInput - 0.2;
            const umbralMax = thresholdInput + 0.2;

            const sec1 = [];
            const sec2 = [];

            const maxLen = Math.max(dataAnt.length, dataAct.length);

            for(let i = 0; i < maxLen; i++) {
                const montoAnt = dataAnt[i] || 0;
                const montoAct = dataAct[i] || 0;

                const difMonto = montoAct - montoAnt;
                let difPorcentual = 0;
                if (montoAnt !== 0) {
                    difPorcentual = (difMonto / montoAnt) * 100;
                } else if (montoAct !== 0) {
                    difPorcentual = 100;
                }

                const row = {
                    montoAnt: montoAnt,
                    montoAct: montoAct,
                    difMonto: difMonto,
                    difPorcentual: difPorcentual,
                    hasWarning: false
                };

                if (difPorcentual < umbralMin || difPorcentual > umbralMax) {
                    row.hasWarning = true;
                    sec2.push(row);
                }
                sec1.push(row);
            }

            document.getElementById('ocr-table-sec1-importes').innerHTML = sec1.map(r => \`
                <tr style="\${r.hasWarning ? 'background-color: #ffeaa7;' : ''} border-bottom: 1px solid #eee;">
                    <td style="padding: 10px; text-align: right;">\${formatterMoney(r.montoAnt)}</td>
                    <td style="padding: 10px; text-align: right;">\${formatterMoney(r.montoAct)}</td>
                    <td style="padding: 10px; text-align: right; color: \${r.difMonto > 0 ? '#27ae60' : (r.difMonto < 0 ? '#e74c3c' : '#555')}">\${formatterMoney(r.difMonto)}</td>
                    <td style="padding: 10px; text-align: right; font-weight: bold;">\${r.difPorcentual.toFixed(2)}%</td>
                </tr>
            \`).join('');

            document.getElementById('ocr-table-sec2-importes').innerHTML = sec2.map(r => \`
                <tr style="border-bottom: 1px solid #eee;">
                    <td style="padding: 10px; text-align: right;">\${formatterMoney(r.montoAnt)}</td>
                    <td style="padding: 10px; text-align: right;">\${formatterMoney(r.montoAct)}</td>
                    <td style="padding: 10px; text-align: right;">\${formatterMoney(r.difMonto)}</td>
                    <td style="padding: 10px; text-align: right; color:#e74c3c; font-weight: bold;">\${r.difPorcentual.toFixed(2)}%</td>
                </tr>
            \`).join('');
        }
`;

content = content.replace('// Parseo de líneas individuales desde OCR (ENFOCADO EN EXTREMOS)', newLogic + '\n        // Parseo de líneas individuales desde OCR (ENFOCADO EN EXTREMOS)');

fs.writeFileSync(fpath, content, 'utf8');
console.log('Script patched index.html successfully.');
