
from playwright.sync_api import sync_playwright
import time
import os

def run():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True, args=['--no-sandbox', '--disable-setuid-sandbox'])
        page = browser.new_page()

        # Load the local index.html
        page.goto(f"file://{os.getcwd()}/index.html")

        print("Page loaded.")

        # Check for console errors
        page.on("console", lambda msg: print(f"Console: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"Page Error: {exc}"))

        # Wait for the view to potentially load (simulate user interaction if needed,
        # but here we just want to see if the script crashes on load)

        # Force show the view for verification
        page.evaluate("document.getElementById('meta-realizado-view').classList.remove('hidden')")

        # Wait a bit
        time.sleep(2)

        # Take a screenshot
        page.screenshot(path="verification_fix.png")
        print("Screenshot taken: verification_fix.png")

        browser.close()

if __name__ == "__main__":
    run()
