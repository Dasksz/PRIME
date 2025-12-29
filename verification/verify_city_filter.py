from playwright.sync_api import sync_playwright
import os

def verify_city_filter():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Load local file using absolute path
        cwd = os.getcwd()
        file_path = f'file://{cwd}/index.html'
        print(f'Navigating to: {file_path}')
        page.goto(file_path)

        try:
            # Check if the Supplier Filter exists in the DOM
            # It might be hidden initially because the parent div is hidden
            supplier_filter = page.query_selector('#city-supplier-filter-btn')

            if supplier_filter:
                print('SUCCESS: City Supplier Filter found in DOM.')
            else:
                print('FAILURE: City Supplier Filter NOT found in DOM.')

            # Attempt to make the city view visible manually via JS to take a screenshot
            page.evaluate("document.getElementById('city-view').classList.remove('hidden');")

            # Take screenshot
            page.screenshot(path='/app/verification/city_filter_verification.png')

        except Exception as e:
            print(f'Error during verification: {e}')
            page.screenshot(path='/app/verification/error.png')

        browser.close()

if __name__ == '__main__':
    verify_city_filter()
