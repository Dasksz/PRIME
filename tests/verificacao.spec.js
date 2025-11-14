// tests/verificacao.spec.js
const { test, expect } = require('@playwright/test');

test.describe('Verificação da Interface do Dashboard', () => {

  test('deve esconder o loader e exibir a tela do dashboard após o carregamento dos dados', async ({ page }) => {
    // 1. Navega para a página local
    await page.goto('file:///app/index.html', { waitUntil: 'networkidle' });

    // 2. Define um tempo limite generoso para o carregamento dos dados.
    // O teste irá falhar se o loader não desaparecer dentro deste tempo.
    const loadingTimeout = 20000; // 20 segundos

    // 3. Verifica se o loader está inicialmente visível (opcional, mas bom para sanidade)
    const loader = page.locator('#loader');
    await expect(loader).toBeVisible();

    // 4. A asserção principal: Espera que o loader se torne oculto.
    // Isto só acontecerá se a função initializeNewDashboard for executada com sucesso
    // (ou seja, o Promise.all resolver e o código para esconder o loader for executado).
    await expect(loader).toBeHidden({ timeout: loadingTimeout });

    // 5. Verifica se o painel principal agora está visível.
    const dashboardView = page.locator('#dashboard-view');
    await expect(dashboardView).toBeVisible();

    // 6. Captura uma imagem para verificação visual do resultado final.
    await page.screenshot({ path: 'tests/verificacao.png', fullPage: true });

    console.log('Screenshot de verificação salva em tests/verificacao.png');
    console.log('Verificação concluída: O loader foi ocultado e o dashboard está visível.');
  });

});
