const fs = require('fs');

const fpath = 'c:\\proyecto-aud\\public\\index.html';
let content = fs.readFileSync(fpath, 'utf8');

// The issue mostly affects template strings inside javascript.
// Many occurrences of `< div class= "breakdown-item" >`
content = content.replace(/<\s*div\s*class=\s*"breakdown-item"\s*>/g, '<div class="breakdown-item">');
content = content.replace(/<\/\s*div\s*>/g, '</div>');
content = content.replace(/<\s*tr\s*>/g, '<tr>');
content = content.replace(/<\/\s*tr\s*>/g, '</tr>');
content = content.replace(/<\s*td\s*>/g, '<td>');
content = content.replace(/<\/\s*td\s*>/g, '</td>');
content = content.replace(/<\s*td\s*colspan="2"/g, '<td colspan="2"');
content = content.replace(/<\s*td\s*style=/g, '<td style=');
content = content.replace(/<\s*tr\s*style=/g, '<tr style=');
content = content.replace(/<\s*label\s*style=/g, '<label style=');

fs.writeFileSync(fpath, content, 'utf8');
console.log('Fixed tags');
