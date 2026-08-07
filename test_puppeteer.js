const puppeteer = require('puppeteer');

(async () => {
    const browser = await puppeteer.launch({
        args: ['--no-sandbox', '--disable-setuid-sandbox']
    });
    const page = await browser.newPage();

    page.on('pageerror', err => {
        console.log('Page error:', err.message);
    });

    await page.goto('http://localhost:8000', { waitUntil: 'networkidle0' });
    console.log("Navigation complete");
    await browser.close();
})();
