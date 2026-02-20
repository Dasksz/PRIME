from playwright.sync_api import sync_playwright
import os
import json

def test_filters(page):
    # Mock Data
    mock_data = {
        "detailed": { "columns": ["CODCLI", "DTPED", "VLVENDA", "CODUSUR", "SUPERV", "TIPOVENDA", "FILIAL", "RCAS", "NOME", "CODFOR"], "values": { "CODCLI": [], "DTPED": [], "VLVENDA": [], "CODUSUR": [], "SUPERV": [], "TIPOVENDA": [], "FILIAL": [], "RCAS": [], "NOME": [], "CODFOR": [] }, "length": 0 },
        "history": { "columns": [], "values": {}, "length": 0 },
        "clients": { "columns": ["Código", "rca1", "rcas", "cidade", "ramo", "razaoSocial"], "values": { "Código": ["1"], "rca1": ["100"], "rcas": [["100"]], "cidade": ["Test City"], "ramo": ["Test Rede"], "razaoSocial": ["Test Client"] }, "length": 1 },
        "byOrder": [],
        "stockMap05": {},
        "stockMap08": {},
        "innovationsMonth": [],
        "activeProductCodes": [],
        "productDetails": {},
        "metadata": [],
        "clientCoordinates": [],
        "passedWorkingDaysCurrentMonth": 1,
        "isColumnar": True
    }

    # Inject Mock Data and Bypass Login
    page.add_init_script(f"""
        window.embeddedData = {json.dumps(mock_data)};
        window.isDataLoaded = true;
        window.userRole = 'adm';

        // Mock init.js behavior or override it
        window.carregarDadosDoSupabase = function() {{
            console.log("Mocking carregarDadosDoSupabase");
            const scriptEl = document.createElement('script');
            scriptEl.src = 'app.js';
            scriptEl.onload = () => {{
                document.getElementById('loader').classList.add('hidden');
                document.getElementById('content-wrapper').classList.remove('hidden');
                document.getElementById('tela-login').classList.add('hidden');
                document.getElementById('tela-loading').classList.add('hidden');
            }};
            document.body.appendChild(scriptEl);
        }};

        // Auto-trigger load when DOM is ready if it doesn't happen automatically
        document.addEventListener('DOMContentLoaded', () => {{
            setTimeout(() => {{
                if (window.carregarDadosDoSupabase) window.carregarDadosDoSupabase();
            }}, 100);
        }});
    """)

    # Navigate to the local file
    page.goto(f"file://{os.getcwd()}/index.html")

    # Wait for the dashboard to be visible
    try:
        page.wait_for_selector("#main-dashboard", state="visible", timeout=10000)
    except:
        # If timeout, maybe manually trigger logic if init.js didn't fire?
        # But our init script hook should handle it.
        # Let's force hide login if it's still there
        page.evaluate("document.getElementById('tela-login').classList.add('hidden')")
        page.evaluate("document.getElementById('tela-loading').classList.add('hidden')")
        page.evaluate("document.getElementById('content-wrapper').classList.remove('hidden')")
        page.wait_for_selector("#main-dashboard", state="visible", timeout=5000)

    # 1. Cidades View Verification
    print("Navigating to Cidades View...")
    # Click sidebar menu if needed or direct button
    page.evaluate("document.querySelector('button[data-target=\"cidades\"]').click()")

    page.wait_for_selector("#city-view", state="visible")

    # Check for Filial Filter
    filial_filter = page.locator("#city-filial-filter")
    if filial_filter.is_visible():
        print("SUCCESS: City Filial Filter is visible.")
        page.screenshot(path="verification/city_view_success.png")
    else:
        print("FAILURE: City Filial Filter not found.")
        page.screenshot(path="verification/city_view_fail.png")

    # 2. Semanal View Verification
    print("Navigating to Semanal View...")
    page.evaluate("document.querySelector('button[data-target=\"semanal\"]').click()")

    page.wait_for_selector("#weekly-view", state="visible")

    # Check for Filial Filter
    weekly_filial_filter = page.locator("#weekly-filial-filter")
    if weekly_filial_filter.is_visible():
        print("SUCCESS: Weekly Filial Filter is visible.")
        page.screenshot(path="verification/weekly_view_success.png")
    else:
        print("FAILURE: Weekly Filial Filter not found.")
        page.screenshot(path="verification/weekly_view_fail.png")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            test_filters(page)
        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="verification/error_state.png")
        finally:
            browser.close()
