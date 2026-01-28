from playwright.sync_api import sync_playwright
import time

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto("http://localhost:8000/index.html")

        # Wait a bit
        time.sleep(3)

        # 1. Check if Flatpickr library is loaded
        is_flatpickr_loaded = page.evaluate("typeof flatpickr !== 'undefined'")
        print(f"Flatpickr loaded: {is_flatpickr_loaded}")

        # 2. Check if the input element exists in DOM
        input_count = page.locator("#coverage-date-filter").count()
        print(f"Input element found: {input_count > 0}")

        if input_count > 0 and is_flatpickr_loaded:
            print("Verification passed (Static checks).")
            page.screenshot(path="verification.png")
        else:
            print("Verification failed.")

        browser.close()

if __name__ == "__main__":
    run()
