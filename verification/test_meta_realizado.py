from playwright.sync_api import sync_playwright
import os
import json

def test_meta_realizado_filter(page):
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

        window.carregarDadosDoSupabase = function() {{
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
        page.evaluate("document.getElementById('tela-login').classList.add('hidden')")
        page.evaluate("document.getElementById('tela-loading').classList.add('hidden')")
        page.evaluate("document.getElementById('content-wrapper').classList.remove('hidden')")
        page.wait_for_selector("#main-dashboard", state="visible", timeout=5000)

    # Verify Meta Vs Realizado View
    print("Navigating to Meta Vs Realizado View...")
    page.evaluate("document.querySelector('button[data-target=\"meta-realizado\"]').click()")

    page.wait_for_selector("#meta-realizado-view", state="visible")

    # Check for Filial Filter
    filial_filter = page.locator("#meta-realizado-filial-filter")
    if filial_filter.is_visible():
        print("SUCCESS: Meta Realizado Filial Filter is visible.")
        page.screenshot(path="verification/meta_realizado_view_success.png")
    else:
        print("FAILURE: Meta Realizado Filial Filter not found.")
        page.screenshot(path="verification/meta_realizado_view_fail.png")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            test_meta_realizado_filter(page)
        except Exception as e:
            print(f"Error: {e}")
            page.screenshot(path="verification/error_state_meta.png")
        finally:
            browser.close()
