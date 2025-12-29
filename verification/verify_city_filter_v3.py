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
            # Clean up overlays
            page.evaluate("document.getElementById('tela-login').classList.add('hidden');")
            page.evaluate("document.getElementById('page-transition-loader').classList.add('hidden');")

            # Hide Main Dashboard
            page.evaluate("document.getElementById('main-dashboard').classList.add('hidden');")

            # Show City View
            page.evaluate("document.getElementById('city-view').classList.remove('hidden');")

            # Show Outer Content Wrapper
            # Note: There are two elements with id 'content-wrapper'. The outer one needs to be visible.
            # We remove 'hidden' from the first one found (outer).
            page.evaluate("document.querySelector('#content-wrapper').classList.remove('hidden');")

            # Check if the Supplier Filter exists in the DOM
            supplier_filter = page.query_selector('#city-supplier-filter-btn')

            if supplier_filter:
                print('SUCCESS: City Supplier Filter found in DOM.')
            else:
                print('FAILURE: City Supplier Filter NOT found in DOM.')

            # Take screenshot of the page
            page.screenshot(path='/app/verification/city_view_filter.png')

        except Exception as e:
            print(f'Error during verification: {e}')
            page.screenshot(path='/app/verification/error_v3.png')

        browser.close()

if __name__ == '__main__':
    verify_city_filter()
