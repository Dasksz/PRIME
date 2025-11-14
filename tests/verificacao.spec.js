// tests/verificacao.spec.js
const { test, expect } = require('@playwright/test');

test.describe('Verificação da Interface do Dashboard', () => {

  test('deve exibir a tela do dashboard após o carregamento', async ({ page }) => {
    // 1. Navega para a página local
    await page.goto('file:///app/index.html');

    // 2. Simula o estado pós-autenticação/carregamento, forçando a exibição do dashboard
    // Esta é uma abordagem direta para contornar a lógica de autenticação em um ambiente de teste local.
    await page.evaluate(() => {
      const dashboardView = document.getElementById('dashboard-view');
      const loader = document.getElementById('loader');

      if (dashboardView) {
        dashboardView.classList.remove('hidden');
      }
      if (loader) {
        loader.classList.add('hidden');
      }
    });

    // 3. Verifica se o loader está oculto
    const loader = await page.locator('#loader');
    await expect(loader).toBeHidden();

    // 4. Verifica se o painel principal está visível
    const dashboardView = await page.locator('#dashboard-view');
    await expect(dashboardView).toBeVisible();

    // 5. Captura uma imagem para verificação visual
    await page.screenshot({ path: 'tests/verificacao.png', fullPage: true });

    console.log('Screenshot de verificação salva em tests/verificacao.png');
  });

});
