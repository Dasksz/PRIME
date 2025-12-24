
from playwright.sync_api import sync_playwright
import time
import os

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox'])
        page = browser.new_page(viewport={'width': 1920, 'height': 1080})

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
        print("Enabling light mode...")
        page.evaluate("""
            document.documentElement.classList.add('light');
            document.getElementById('checkbox-theme').checked = true;
        """)

        time.sleep(1)

        # Helper to switch views directly via JS to avoid navigation issues
        def switch_view(view_id):
            print(f"Switching to {view_id}...")
            page.evaluate(f"""
                // Hiding all potential views manually to ensure clean state
                const views = ['main-dashboard', 'city-view', 'weekly-view', 'comparison-view', 'stock-view', 'cobertura-view', 'innovations-month-view', 'mix-view', 'goals-view'];
                views.forEach(id => {{
                    const el = document.getElementById(id);
                    if(el) el.classList.add('hidden');
                }});

                const target = document.getElementById('{view_id}');
                if(target) target.classList.remove('hidden');
            """)
            time.sleep(1)
            # Take screenshot
            path = f"/home/jules/verification/view_{view_id}.png"
            page.screenshot(path=path, full_page=True)
            print(f"Screenshot saved: {path}")

        # 1. Cities View
        switch_view('city-view')

        # 2. Weekly View
        switch_view('weekly-view')

        # 3. Mix View
        switch_view('mix-view')

        # 4. Innovations Month View
        switch_view('innovations-month-view')

        # 5. Goals View
        switch_view('goals-view')

        browser.close()

if __name__ == "__main__":
    run()
