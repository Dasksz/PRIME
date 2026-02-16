const { webcrypto } = require('crypto');
const crypto = webcrypto;

async function computeHash(data) {
    try {
        const json = JSON.stringify(data);
        const encoder = new TextEncoder();
        const dataBuffer = encoder.encode(json);
        const hashBuffer = await crypto.subtle.digest('SHA-256', dataBuffer);
        const hashArray = Array.from(new Uint8Array(hashBuffer));
        const hashHex = hashArray.map(b => b.toString(16).padStart(2, '0')).join('');
        return hashHex;
    } catch (e) {
        console.warn("Hashing failed:", e);
        return null;
    }
}

async function test() {
    const data1 = { a: 1, b: "test" };
    const data2 = { a: 1, b: "test" };
    const data3 = { a: 1, b: "changed" };

    const h1 = await computeHash(data1);
    const h2 = await computeHash(data2);
    const h3 = await computeHash(data3);

    console.log("Hash 1:", h1);
    console.log("Hash 2:", h2);
    console.log("Hash 3:", h3);

    if (h1 === h2 && h1 !== h3 && h1.length === 64) {
        console.log("SUCCESS: Hash logic works and is consistent.");
    } else {
        console.error("FAILURE: Hash logic is inconsistent.");
        process.exit(1);
    }
}

test();
