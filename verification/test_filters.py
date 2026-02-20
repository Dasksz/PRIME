from playwright.sync_api import sync_playwright
import os

def test_filters(page):
    # Navigate to the local file
    page.goto(f"file://{os.getcwd()}/index.html")

    # Wait for the page to load (checking for a key element)
    page.wait_for_selector("#side-menu", state="visible")

    # Navigate to "Cidades" view
    page.click("button[data-target='cidades']")
    page.wait_for_selector("#city-view", state="visible")
    page.wait_for_selector("#city-filial-filter", state="visible")

    # Take a screenshot of the "Cidades" view with the new filter
    page.screenshot(path="verification/city_view_filter.png")

    # Navigate to "Semanal" view
    page.click("button[data-target='semanal']")
    page.wait_for_selector("#weekly-view", state="visible")
    page.wait_for_selector("#weekly-filial-filter", state="visible")

    # Take a screenshot of the "Semanal" view with the new filter
    page.screenshot(path="verification/weekly_view_filter.png")

if __name__ == "__main__":
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        try:
            test_filters(page)
        finally:
            browser.close()
