
const http = require('http');
const { spawn } = require('child_process');

console.log("Starting Server for API Test...");
const serverProcess = spawn('node', ['server.js'], {
    cwd: __dirname,
    stdio: 'pipe',
    shell: true
});

serverProcess.stdout.on('data', (data) => {
    console.log(`SERVER: ${data}`);
    if (data.toString().includes('ejecutándose en http://localhost:3000')) {
        console.log("Server started. Running API tests...");
        runTests();
    }
});

serverProcess.stderr.on('data', (data) => console.error(`SERVER ERROR: ${data}`));

function makeRequest(path) {
    return new Promise((resolve, reject) => {
        http.get(`http://localhost:3000${path}`, (res) => {
            let data = '';
            res.on('data', chunk => data += chunk);
            res.on('end', () => {
                if (res.statusCode >= 200 && res.statusCode < 300) {
                    try {
                        const json = JSON.parse(data);
                        console.log(`[PASS] ${path} returned ${json.length} records.`);
                        resolve(true);
                    } catch (e) {
                        console.error(`[FAIL] ${path} returned invalid JSON.`);
                        resolve(false);
                    }
                } else {
                    console.error(`[FAIL] ${path} returned status ${res.statusCode}`);
                    resolve(false);
                }
            });
        }).on('error', (err) => {
            console.error(`[ERROR] Request to ${path} failed: ${err.message}`);
            resolve(false);
        });
    });
}

async function runTests() {
    await new Promise(r => setTimeout(r, 2000)); // Wait a bit for DB/CSV load if needed

    try {
        await makeRequest('/api/liquidacion-completa');
        await makeRequest('/api/residentes');
        await makeRequest('/api/ley100');
        await makeRequest('/api/preparar-acumulado');

        console.log("All tests completed.");
    } catch (error) {
        console.error("Test execution error:", error);
    } finally {
        console.log("Stopping server...");
        serverProcess.kill();
        process.exit(0);
    }
}
