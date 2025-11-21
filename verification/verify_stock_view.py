
from playwright.sync_api import Page, expect, sync_playwright
import os

def verify_stock_analysis_filters():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Get the absolute path to the index.html file
        file_path = os.path.abspath("index.html")
        page.goto(f"file://{file_path}")

        # Wait for the page to load
        page.wait_for_load_state("networkidle")

        # Since the dashboard is hidden initially and requires data loading (which won't happen without Supabase),
        # we will manually unhide the dashboard or specific sections to verify the elements exist.
        # However, the script modifies 'updateStockProductFilter' etc. which are called when filters change.
        # We can try to inspect if the functions are present and updated by checking source or behavior if possible.
        # But without mock data, the dropdowns will be empty or the page might be stuck in loading.

        # Let's try to make the dashboard visible by removing 'hidden' class via JS
        page.evaluate("document.getElementById('main-dashboard').classList.remove('hidden')")
        page.evaluate("document.getElementById('stock-view').classList.remove('hidden')")

        # Take a screenshot of the Stock View to see if the structure is correct
        page.screenshot(path="verification/stock_view_structure.png")

        print("Screenshot taken: verification/stock_view_structure.png")

        browser.close()

if __name__ == "__main__":
    verify_stock_analysis_filters()
