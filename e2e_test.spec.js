const { test, expect } = require('@playwright/test');

test('force coverage view visibility and take screenshot', async ({ page }) => {
  await page.goto('file:///app/index.html');

  // Force the coverage view to be visible
  await page.evaluate(() => {
    const views = ['dashboard-view', 'orders-view', 'city-view', 'stock-view', 'weekly-view', 'comparison-view', 'innovations-view'];
    views.forEach(id => document.getElementById(id)?.classList.add('hidden'));

    document.getElementById('coverage-view')?.classList.remove('hidden');
    document.getElementById('loader')?.classList.add('hidden');
  });

  // Wait for rendering
  await page.waitForTimeout(1000);

  // Take a screenshot for final verification
  const screenshotPath = '/home/jules/verification/coverage_view_verification.png';
  await page.screenshot({ path: screenshotPath });

  console.log(`Coverage view screenshot saved to ${screenshotPath}`);
});
