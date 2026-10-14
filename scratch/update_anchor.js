const fs = require('fs');
let content = fs.readFileSync('c:/proyecto-aud/public/index.html', 'utf8');
const match = content.match(/<a href="#" id="menu-novedades-ld">.*Novedades LD<\/a>/);
console.log(match[0]);
content = content.replace(
    match[0],
    match[0] + '\n                <a href="#" id="menu-novedades-ld-importes-dni" style="padding-left: 70px !important; font-size: 14px !important; color: #999 !important;">└─ Novedades LD Importes Por DNI</a>'
);
fs.writeFileSync('c:/proyecto-aud/public/index.html', content, 'utf8');
console.log('Update complete');
