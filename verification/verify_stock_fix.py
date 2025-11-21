from playwright.sync_api import sync_playwright
import json

def verify_fix():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Capture console logs
        page.on('console', lambda msg: print(f'Browser Console: {msg.text}'))

        page.goto('file:///app/index.html')

        # Inject Mock Data and Supabase Client Spy
        page.evaluate("""
            window.embeddedData = {
                productDetails: {
                    'P001': { descricao: 'Produto Teste 1', fornecedor: 'Forn 1', codfor: '100' },
                    'P002': { descricao: 'Produto Teste 2', fornecedor: 'Forn 2', codfor: '200' }
                },
                filters: {
                    suppliers: [{codfor: '100', fornecedor: 'Forn 1'}, {codfor: '200', fornecedor: 'Forn 2'}],
                    sellers: ['Vendedor A', 'Vendedor B'],
                    supervisors: ['Sup A', 'Sup B']
                },
                stockMap05: {},
                stockMap08: {}
            };

            window.supabaseClient = {
                rpc: async (funcName, params) => {
                    console.log(`RPC Call: ${funcName}`);
                    console.log(`RPC Params: ${JSON.stringify(params)}`);
                    return { data: { stock_table: [] }, error: null };
                },
                from: () => ({ select: () => ({ data: [], error: null }) })
            };
        """)

        # Switch View and Trigger Updates
        page.evaluate("""
            document.getElementById('loader').classList.add('hidden');
            document.getElementById('tela-login').classList.add('hidden');
            document.getElementById('main-dashboard').classList.add('hidden');
            document.getElementById('stock-view').classList.remove('hidden');

            // Trigger population
            if (typeof updateStockProductFilter === 'function') updateStockProductFilter();
            if (typeof updateStockSupplierFilter === 'function') updateStockSupplierFilter();
            if (typeof updateStockSellerFilter === 'function') updateStockSellerFilter();

            // Trigger updateStockView to check params
            // Set some values to check if they are passed
            const cityInput = document.getElementById('stock-city-filter');
            if(cityInput) cityInput.value = 'Cidade Teste';

            const filialInput = document.getElementById('stock-filial-filter');
            if(filialInput) filialInput.value = '08';

            if (typeof updateStockView === 'function') updateStockView();
        """)

        # Open the product filter dropdown to see options
        page.click('#stock-product-filter-btn')

        page.wait_for_timeout(1000) # Wait for animation/rendering

        # Screenshot
        page.screenshot(path='verification/stock_fix_verified.png')
        print('Verification completed.')
        browser.close()

if __name__ == '__main__':
    verify_fix()
