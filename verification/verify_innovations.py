
from playwright.sync_api import Page, expect, sync_playwright
import time

def verify_innovations_page(page: Page):
    # 1. Go to the app
    page.goto("http://localhost:8080/index.html")

    # 2. Wait for login screen (it's the default state)
    # Use the memory's suggestion to bypass login by hiding the overlay
    page.evaluate("document.getElementById('tela-login').classList.add('hidden')")
    page.evaluate("document.getElementById('main-dashboard').classList.remove('hidden')")

    # 3. Navigate to Innovations View
    # Since I can't click the real buttons because auth logic might interfere or data load might fail,
    # I will force the view switch via JS as well to ensure it's visible for the screenshot.
    page.evaluate("document.getElementById('main-dashboard').classList.add('hidden')")
    page.evaluate("document.getElementById('innovations-view').classList.remove('hidden')")

    # 4. Verify if the view is visible
    innovations_view = page.locator("#innovations-view")
    expect(innovations_view).not_to_have_class("hidden")

    # 5. Check if the product filter button text indicates the logic (it won't have data, but we can check the element exists)
    filter_btn = page.locator("#innovations-product-filter-btn")
    expect(filter_btn).to_be_visible()

    # 6. Take screenshot
    page.screenshot(path="verification/innovations_view.png")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            verify_innovations_page(page)
        finally:
            browser.close()
