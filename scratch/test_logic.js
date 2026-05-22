const row = {
  "NIVEL": "NIVEL d",
  "DESCAGENTE": "PONCE GRACIELA ANALIA",
  "CUIT_CUIL": "27271414167",
  "NRO_DOCUMENTO": "27141416",
  "D_TRAB": "30.00",
  "ANTIGUEDAD": "22.00",
  "PLANTA": "Permanente Titular",
  "ORGANISMO": "Área Op. Santa Ana",
  "FUNCION": "AUXILIAR ENFERMERIA - NIVEL d",
  "AGRUPAMIENTO": "Asistenciales",
  "PERIODO_IMPUTADO": "01/05/2026",
  "PERIODO_LIQUIDADO": "01/05/2026",
  "NUMERO_CARGO": "39",
  "FECHA_NACIMIENTO": "14/03/1979",
  "SEXO": "F",
  "Area2": "A1",
  "AP100_090_54": "0.00"
};

const antiguedad = "18";
const edad_f = "48";
const edad_m = "53";
const fecha_calculo = "2026-05-22";

const [fcYear, fcMonth, fcDay] = fecha_calculo.split('-');
const targetDate = new Date(fcYear, fcMonth - 1, fcDay);

function calculateAge(birthDateStr, targetDateObj) {
    if (!birthDateStr) return -1;
    let parts = birthDateStr.split('/');
    if (parts.length !== 3) {
        if (birthDateStr.includes('-')) {
            parts = birthDateStr.split('-');
            const bDate = new Date(parts[0], parts[1] - 1, parts[2]);
            let age = targetDateObj.getFullYear() - bDate.getFullYear();
            let m = targetDateObj.getMonth() - bDate.getMonth();
            if (m < 0 || (m === 0 && targetDateObj.getDate() < bDate.getDate())) age--;
            return age;
        }
        return -1;
    }
    let bDate = new Date(parts[2], parts[1] - 1, parts[0]);
    let age = targetDateObj.getFullYear() - bDate.getFullYear();
    let m = targetDateObj.getMonth() - bDate.getMonth();
    if (m < 0 || (m === 0 && targetDateObj.getDate() < bDate.getDate())) age--;
    return age;
}

const minAntiguedad = parseInt(antiguedad, 10);
const minEdadF = parseInt(edad_f, 10);
const minEdadM = parseInt(edad_m, 10);

console.log("Starting validation...");

const area2 = row.Area2 ? row.Area2.trim() : '';
if (area2 !== 'A1' && area2 !== 'A26' && area2 !== 'A27') {
    console.log("Failed Area2:", area2);
} else {
    console.log("Passed Area2");
}

const pImputado = row.PERIODO_IMPUTADO ? row.PERIODO_IMPUTADO.trim() : '';
const pLiquidado = row.PERIODO_LIQUIDADO ? row.PERIODO_LIQUIDADO.trim() : '';
if (pImputado !== pLiquidado) {
    console.log("Failed Period:", pImputado, pLiquidado);
} else {
    console.log("Passed Period");
}

const planta = row.PLANTA ? row.PLANTA.trim() : '';
if (planta === 'Reemplazante no permanente-LD' || planta === 'Reemplazante no permanente') {
    console.log("Failed Planta:", planta);
} else {
    console.log("Passed Planta");
}

const ap100 = parseFloat(row.AP100_090_54);
if (isNaN(ap100) || ap100 > 0) {
    console.log("Failed AP100:", ap100);
} else {
    console.log("Passed AP100");
}

const antiguedadRow = parseInt(row.ANTIGUEDAD, 10) || 0;
if (antiguedadRow < minAntiguedad) {
    console.log(`Failed Antiguedad: Row has ${antiguedadRow}, required ${minAntiguedad}`);
} else {
    console.log("Passed Antiguedad");
}

const sexo = row.SEXO ? row.SEXO.trim().toUpperCase() : '';
const edad = calculateAge(row.FECHA_NACIMIENTO, targetDate);
console.log("Calculated Age:", edad);
if (edad === -1) {
    console.log("Failed Age: parsing error");
} else {
    if (sexo === 'F' && edad < minEdadF) {
        console.log(`Failed Age check for Female: Age ${edad} < required ${minEdadF}`);
    } else if (sexo === 'M' && edad < minEdadM) {
        console.log(`Failed Age check for Male: Age ${edad} < required ${minEdadM}`);
    } else if (sexo !== 'F' && sexo !== 'M') {
        console.log("Failed Sexo:", sexo);
    } else {
        console.log("Passed Age & Sexo check!");
    }
}
