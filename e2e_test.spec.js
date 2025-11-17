
const { test, expect } = require('@playwright/test');
const jwt = require('jsonwebtoken'); // Importa o módulo no ambiente Node.js

test('Verifica o carregamento do dashboard com um token JWT simulado', async ({ page }) => {
  test.setTimeout(60000);

  page.on('console', msg => console.log('LOG DO BROWSER:', msg.text()));
  page.on('pageerror', error => console.error('ERRO NA PÁGINA:', error.message));

  // 1. Gera o token no ambiente Node.js, onde 'jwt' está disponível
  // Usa a chave anon como segredo, pois é o que o Supabase espera para tokens auto-assinados
  const supabaseAnonKey = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRob3p3aGZtcndpdW13cGNxYWJpIiwicm9sZSI6ImFub24iLCJpYXQiOjE3NjIyNjMzNjAsImV4cCI6MjA3NzgzOTM2MH0.syWqcBCbfH5Ey5AB4NGrsF2-ZuBw4W3NZAPIAZb6Bq4';
  const fakeToken = jwt.sign({
      "aud": "authenticated",
      "exp": Math.floor(Date.now() / 1000) + (60 * 60), // Expira em 1 hora
      "role": "authenticated"
  }, supabaseAnonKey);

  // Navega para a página
  await page.goto('file:///app/index.html');

  // 2. Passa o token gerado como argumento para a função page.evaluate
  await page.evaluate(async (token) => {
    // Esconde as telas de login
    document.getElementById('tela-login')?.classList.add('hidden');
    document.getElementById('loader')?.classList.remove('hidden');

    const supabaseUrl = window.SUPABASE_URL;
    const supabaseAnonKey = window.SUPABASE_ANON_KEY;

    // Cria o cliente Supabase
    const { createClient } = window.supabase;
    const supabaseClient = createClient(supabaseUrl, supabaseAnonKey);

    // 3. Usa o token recebido para definir a sessão
    await supabaseClient.auth.setSession({ access_token: token, refresh_token: 'fake-refresh-token' });

    // Chama a função de carregamento com o cliente agora "autenticado"
    await window.carregarDadosDoSupabase(supabaseClient);

  }, fakeToken); // Passa a variável 'fakeToken'

  // O restante do teste permanece o mesmo
  await expect(page.locator('#total-vendas')).not.toHaveText('R$ 0,00', { timeout: 30000 });

  await expect(page.locator('#main-dashboard')).toBeVisible();

  await page.waitForTimeout(2000);

  await page.screenshot({ path: 'test-results/dashboard-final-screenshot.png', fullPage: true });
});
