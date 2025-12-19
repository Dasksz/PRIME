from playwright.sync_api import sync_playwright
import os
import time

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()

    # Load local index.html
    page.goto(f"file://{os.getcwd()}/index.html")

    # Bypass Login/Loading Screens AND Page Transition Loader
    # AND Force Sidebar Open
    page.evaluate("""() => {
        document.getElementById('tela-login').classList.add('hidden');
        document.getElementById('tela-loading').classList.add('hidden');
        document.getElementById('tela-pendente').classList.add('hidden');
        document.getElementById('page-transition-loader').classList.add('hidden');
        document.getElementById('content-wrapper').classList.remove('hidden');

        // Force Sidebar Open
        const sidebar = document.getElementById('side-menu');
        sidebar.classList.remove('-translate-x-full');
        sidebar.style.transform = 'translateX(0)'; // Force style just in case
    }""")

    # Wait for transition
    page.wait_for_timeout(500)

    # Click Admin Button
    page.click('#open-admin-btn')

    # Wait for modal to be visible
    page.wait_for_selector('#admin-uploader-modal:not(.hidden)')

    # Take screenshot
    screenshot_path = '/home/jules/verification/modal_verification.png'
    os.makedirs(os.path.dirname(screenshot_path), exist_ok=True)
    page.screenshot(path=screenshot_path)
    print(f"Screenshot saved to {screenshot_path}")

    browser.close()

with sync_playwright() as playwright:
    run(playwright)
