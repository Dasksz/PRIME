from playwright.sync_api import sync_playwright, expect

def test_meta_realizado_view():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Navigate
        page.goto("http://localhost:8080/index.html")

        # Mock Data and Inject app.js manually
        print("Mocking data and injecting app.js...")
        page.evaluate("""
            window.embeddedData = {
                detailed: [],
                history: [],
                clients: [],
                byOrder: [],
                stockMap05: {},
                stockMap08: {},
                innovationsMonth: [],
                activeProductCodes: [],
                productDetails: {},
                metadata: [],
                clientCoordinates: [],
                passedWorkingDaysCurrentMonth: 1,
                isColumnar: false // Simpler for mock
            };
            window.isDataLoaded = true;

            // Manually load app.js since init.js won't reach success
            const script = document.createElement('script');
            script.src = 'app.js';
            document.body.appendChild(script);
        """)

        # Wait for app.js to execute (it runs setupEventListeners)
        page.wait_for_timeout(1000)

        # Bypass Gatekeeper UI
        page.evaluate("""
            document.getElementById('tela-login').classList.add('hidden');
            document.getElementById('tela-loading').classList.add('hidden');
            document.getElementById('tela-pendente').classList.add('hidden');
            document.getElementById('page-transition-loader').classList.add('hidden');
            document.getElementById('content-wrapper').classList.remove('hidden');
        """)

        # Click sidebar toggle if needed
        sidebar = page.locator("#side-menu")
        if "translate-x-full" in sidebar.get_attribute("class"):
            print("Opening sidebar...")
            page.evaluate("document.getElementById('side-menu').classList.remove('-translate-x-full')")
            page.wait_for_timeout(500)

        # Click on "Meta Vs. Realizado" link
        print("Clicking 'Meta Vs. Realizado' menu item...")
        meta_link = page.locator("button[data-target='meta-realizado']")
        meta_link.wait_for(state="visible")
        meta_link.click()

        # Wait for view to appear
        view = page.locator("#meta-realizado-view")
        expect(view).to_be_visible()
        print("Meta Vs. Realizado view is visible.")

        # Verify Headers
        header_text = page.locator("#meta-realizado-view h1").inner_text()
        print(f"Header found: {header_text}")
        assert "PAINEL DE VENDAS" in header_text

        subtitle = page.locator("#meta-realizado-view p.text-lg").inner_text()
        print(f"Subtitle found: {subtitle}")
        assert "Meta Vs. Realizado" in subtitle

        # Verify Filters exist
        expect(page.locator("#meta-realizado-supervisor-filter-btn")).to_be_visible()
        expect(page.locator("#meta-realizado-vendedor-filter-btn")).to_be_visible()
        expect(page.locator("#meta-realizado-supplier-filter-btn")).to_be_visible()

        # VERIFY PASTA BUTTONS (UPDATED)
        # PEPSICO Should NOT exist
        expect(page.locator("button[data-pasta='PEPSICO']")).not_to_be_visible()
        # ELMA and FOODS Should exist
        expect(page.locator("button[data-pasta='ELMA']")).to_be_visible()
        expect(page.locator("button[data-pasta='FOODS']")).to_be_visible()

        # Verify Chart and Table Containers
        expect(page.locator("#metaRealizadoChartContainer")).to_be_visible()
        expect(page.locator("#metaRealizadoTableContainer")).to_be_visible()

        # Verify Table Headers (at least static ones)
        # We need to wait a bit as JS renders table after navigation
        page.wait_for_timeout(1000)

        table_head = page.locator("#meta-realizado-table-head")
        expect(table_head).to_contain_text("Vendedor")
        expect(table_head).to_contain_text("Meta Total")
        expect(table_head).to_contain_text("Realizado Total")

        expect(table_head).to_contain_text("Semana 1")

        print("Verification successful. Taking screenshot.")
        page.screenshot(path="verification/meta_realizado_screenshot.png")

        browser.close()

if __name__ == "__main__":
    test_meta_realizado_view()
