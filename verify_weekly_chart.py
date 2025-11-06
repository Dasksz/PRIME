import asyncio
from playwright.async_api import async_playwright
import os

async def main():
    async with async_playwright() as p:
        browser = await p.chromium.launch()
        page = await browser.new_page()

        # Get the absolute path to the HTML file
        file_path = os.path.abspath('ONDASH.html')

        # Go to the local HTML file
        await page.goto(f'file://{file_path}')

        # Wait for the main dashboard to be visible (loader to disappear)
        await page.wait_for_selector("#main-dashboard:not(.hidden)", timeout=60000)

        # Click the menu toggle button to open the side menu
        await page.click("#menu-toggle-btn")

        # Wait for the menu to be open
        await page.wait_for_selector("#side-menu.open")

        # Click the button to navigate to the Weekly View
        await page.click("button[data-view='weekly-view']")

        # Wait for the weekly view container to be visible
        await page.wait_for_selector("#weekly-view:not(.hidden)")

        # Wait for the chart to render (assuming chart has a canvas element)
        await page.wait_for_selector("#weeklySalesChartContainer canvas")

        # Take a screenshot of the chart container
        await page.locator("#weekly-view").screenshot(path="weekly_chart_verification.png")

        await browser.close()
        print("Screenshot 'weekly_chart_verification.png' has been taken.")

if __name__ == "__main__":
    asyncio.run(main())
