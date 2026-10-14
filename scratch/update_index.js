const fs = require('fs');
let content = fs.readFileSync('c:/proyecto-aud/public/index.html', 'utf8');

content = content.replace(
    '<a href="#" id="menu-novedades-ld">📁 Novedades LD</a>',
    '<a href="#" id="menu-novedades-ld">📁 Novedades LD</a>\n                <a href="#" id="menu-novedades-ld-importes-dni" style="padding-left: 70px !important; font-size: 14px !important; color: #999 !important;">└─ Novedades LD Importes Por DNI</a>'
);

content = content.replace(
    "setupNovedadesMenu('menu-novedades-ld', 'Novedades LD', 'ld');",
    "setupNovedadesMenu('menu-novedades-ld', 'Novedades LD', 'ld');\n        setupNovedadesMenu('menu-novedades-ld-importes-dni', 'Novedades LD Importes Por DNI', 'ld-importes-dni');"
);

content = content.replace(
    '{ id: "menu-novedades-ld", label: "Novedades LD", elementId: "menu-novedades-ld" },',
    '{ id: "menu-novedades-ld", label: "Novedades LD", elementId: "menu-novedades-ld" },\n                { id: "menu-novedades-ld-importes-dni", label: "Novedades LD Importes Por DNI", elementId: "menu-novedades-ld-importes-dni" },'
);

content = content.replace(
    "'menu-novedades-residentes', 'menu-novedades-residentes-no-liquidados', 'menu-novedades-criticidad', 'menu-novedades-fortalecimiento'];",
    "'menu-novedades-ld-importes-dni', 'menu-novedades-residentes', 'menu-novedades-residentes-no-liquidados', 'menu-novedades-criticidad', 'menu-novedades-fortalecimiento'];"
);

content = content.replace(
    "'menu-novedades-ld', 'menu-novedades-residentes', 'menu-novedades-residentes-no-liquidados',",
    "'menu-novedades-ld', 'menu-novedades-ld-importes-dni', 'menu-novedades-residentes', 'menu-novedades-residentes-no-liquidados',"
);

content = content.replace(
    "                  'menu-novedades-ld',",
    "                  'menu-novedades-ld',\n                  'menu-novedades-ld-importes-dni',"
);

fs.writeFileSync('c:/proyecto-aud/public/index.html', content, 'utf8');
console.log('Update complete');
