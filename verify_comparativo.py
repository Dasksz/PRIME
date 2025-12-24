
from playwright.sync_api import sync_playwright
import time
import os

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page()

        # Determine file path
        cwd = os.getcwd()
        file_path = f"file://{cwd}/index.html"
        print(f"Navigating to: {file_path}")

        page.goto(file_path)

        # Bypass Login
        print("Bypassing login...")
        page.evaluate("""
            document.getElementById('tela-login').classList.add('hidden');
            document.getElementById('content-wrapper').classList.remove('hidden');
        """)

        # Enable Light Mode
        print("Enabling light mode via JS...")
        page.evaluate("""
            document.documentElement.classList.add('light');
            document.getElementById('checkbox-theme').checked = true;
        """)

        time.sleep(1) # Wait for theme transition

        # Navigate to Comparativo
        print("Navigating to Comparativo...")
        page.click("button[data-target='comparativo']")

        time.sleep(2) # Wait for view transition and rendering

        # Take Screenshot
        output_path = "/home/jules/verification/comparativo_light_mode.png"
        page.screenshot(path=output_path, full_page=True)
        print(f"Screenshot saved to: {output_path}")

        browser.close()

if __name__ == "__main__":
    run()
