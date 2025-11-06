import asyncio
from playwright.async_api import async_playwright
import os

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        file_path = os.path.abspath('ONDASH.html')
        await page.goto(f'file://{file_path}')

        await page.wait_for_selector("#main-dashboard:not(.hidden)", timeout=60000)

        # Click the menu toggle button to open the side menu
        await page.click("#menu-toggle-btn")

        # Wait for the menu to be open
        await page.wait_for_selector("#side-menu.open")

        # Take a screenshot of the side menu
        await page.locator("#side-menu").screenshot(path="theme_switcher_verification.png")

        await browser.close()
        print("Screenshot 'theme_switcher_verification.png' has been taken.")

if __name__ == "__main__":
    asyncio.run(main())
