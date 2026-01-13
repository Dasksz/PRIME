import os
import sys
from playwright.sync_api import sync_playwright

def test_goals_import_ui():
    print("Starting UI verification...")
    cwd = os.getcwd()
    html_file = os.path.join(cwd, "index.html")
    file_url = f"file://{html_file}"
    print(f"Opening {file_url}")

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Capture console messages
        page.on("console", lambda msg: print(f"CONSOLE: {msg.text}"))
        page.on("pageerror", lambda exc: print(f"PAGE ERROR: {exc}"))

        page.goto(file_url)

        # Wait for potential loading
        page.wait_for_timeout(2000)

        # 1. Navigate to Goals (Metas) page
        print("Clicking 'Metas' menu button...")
        try:
            page.click('button[data-target="goals"]')
        except Exception as e:
            print(f"Error clicking Metas: {e}")
            # Try forcing click via JS
            page.evaluate("document.querySelector('button[data-target=\"goals\"]').click()")

        page.wait_for_timeout(1000)

        # 2. Check if Goals view is visible
        is_goals_visible = page.is_visible('#goals-view')
        print(f"Goals view visible: {is_goals_visible}")

        # 3. Click "Relatório" tab (SV)
        print("Clicking 'Relatório' tab (#btn-tab-sv)...")
        try:
            page.click('#btn-tab-sv')
        except Exception as e:
            print(f"Error clicking SV tab: {e}")

        page.wait_for_timeout(1000)

        # 4. Check if SV content is visible
        sv_content_visible = page.is_visible('#goals-sv-content')
        print(f"SV Content (#goals-sv-content) visible: {sv_content_visible}")

        if not sv_content_visible:
            # Check classes
            classes = page.getAttribute('#goals-sv-content', 'class')
            print(f"Classes of #goals-sv-content: {classes}")

        # 5. Check Import Button
        import_btn_visible = page.is_visible('#goals-sv-import-btn')
        print(f"Import Button visible: {import_btn_visible}")

        if not import_btn_visible:
             # Check if it exists in DOM at all
             exists = page.evaluate("!!document.getElementById('goals-sv-import-btn')")
             print(f"Import Button exists in DOM: {exists}")
             if exists:
                 display = page.evaluate("window.getComputedStyle(document.getElementById('goals-sv-import-btn')).display")
                 print(f"Import Button computed display: {display}")
                 # Check parent visibility
                 parent_display = page.evaluate("window.getComputedStyle(document.getElementById('goals-sv-import-btn').parentElement).display")
                 print(f"Import Button parent computed display: {parent_display}")


        browser.close()

if __name__ == "__main__":
    test_goals_import_ui()
