
async function read_lines(filepath, start, end) {
    const fs = require('fs');
    const readline = require('readline');

    const fileStream = fs.createReadStream(filepath);

    const rl = readline.createInterface({
        input: fileStream,
        crlfDelay: Infinity
    });

    let lineCount = 0;
    for await (const line of rl) {
        lineCount++;
        if (lineCount >= start && lineCount <= end) {
            console.log(`${lineCount}: ${line}`);
        }
        if (lineCount > end) {
            break;
        }
    }
}

const args = process.argv.slice(2);
read_lines(args[0], parseInt(args[1]), parseInt(args[2]));
