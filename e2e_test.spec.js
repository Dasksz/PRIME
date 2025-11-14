const { test, expect } = require('@playwright/test');

test('force orders view visibility and take screenshot', async ({ page }) => {
  await page.goto('file:///app/index.html');

  // Force the orders view to be visible
  await page.evaluate(() => {
    document.getElementById('dashboard-view')?.classList.add('hidden');
    document.getElementById('orders-view')?.classList.remove('hidden');
    document.getElementById('loader')?.classList.add('hidden');
  });

  // Wait for rendering
  await page.waitForTimeout(1000);

  // Take a screenshot for final verification
  const screenshotPath = '/home/jules/verification/orders_view_verification.png';
  await page.screenshot({ path: screenshotPath });

  console.log(`Orders view screenshot saved to ${screenshotPath}`);
});
