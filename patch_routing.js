const fs = require('fs');

const fpath = 'c:\\proyecto-aud\\public\\index.html';
let content = fs.readFileSync(fpath, 'utf8');

// 1. Add hide logic to showSection
const hideTargetStr = "const detalleVsDetalleView = document.getElementById('detalle-vs-detalle-view');\n            if (detalleVsDetalleView) detalleVsDetalleView.style.display = 'none';";
content = content.replace(hideTargetStr, hideTargetStr + "\n            const detalleVsDetalleImportesView = document.getElementById('detalle-vs-detalle-importes-view');\n            if (detalleVsDetalleImportesView) detalleVsDetalleImportesView.style.display = 'none';");

// 2. Add show logic to showSection
const showTargetStr = "else if (sectionId === 'detalle-vs-detalle-view') {\n                if (detalleVsDetalleView) detalleVsDetalleView.style.display = 'block';\n            }";
content = content.replace(showTargetStr, showTargetStr + "\n            else if (sectionId === 'detalle-vs-detalle-importes-view') {\n                if (detalleVsDetalleImportesView) detalleVsDetalleImportesView.style.display = 'block';\n            }");

// 3. Add event listener definition
const blockToMatch = "        // --- LÓGICA DETALLE VS DETALLE ---\n        const menuDetalleVsDetalle = document.getElementById('menu-detalle-vs-detalle');\n        if (menuDetalleVsDetalle) {\n            menuDetalleVsDetalle.addEventListener('click', function(e) {\n                e.preventDefault();\n                showSection('detalle-vs-detalle-view');\n                closeNav();\n            });\n        }";

const clickListenerStr = `        const menuDetalleImportes = document.getElementById('menu-detalle-vs-detalle-importes');
        if (menuDetalleImportes) {
            menuDetalleImportes.addEventListener('click', function(e) {
                e.preventDefault();
                showSection('detalle-vs-detalle-importes-view');
                closeNav();
            });
        }`;

content = content.replace(blockToMatch, blockToMatch + "\n\n" + clickListenerStr);

fs.writeFileSync(fpath, content, 'utf8');
console.log('Routing patched.');
