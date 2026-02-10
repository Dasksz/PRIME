from playwright.sync_api import sync_playwright
import time

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page(viewport={'width': 1280, 'height': 720})
        page.goto("http://localhost:8080/index.html")

        page.wait_for_timeout(1000)

        page.evaluate("""
            document.querySelectorAll('.flex-center-overlay').forEach(el => el.classList.add('hidden'));
            document.getElementById('loader').classList.add('hidden');
            const ptLoader = document.getElementById('page-transition-loader');
            if(ptLoader) ptLoader.classList.add('hidden');
        """)

        page.evaluate("document.getElementById('content-wrapper').classList.remove('hidden')")

        page.evaluate("""
            const sidebar = document.getElementById('side-menu');
            sidebar.classList.remove('-translate-x-full');
        """)

        page.evaluate("""
            const overlay = document.getElementById('sidebar-overlay');
            overlay.classList.remove('hidden');
        """)

        page.wait_for_timeout(500)

        page.screenshot(path="verification/sidebar_zindex_forced.png")

        browser.close()

if __name__ == "__main__":
    run()
