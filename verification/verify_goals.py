from playwright.sync_api import sync_playwright
import os

def verify_goals_styles():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        file_path = f"file://{os.path.abspath('index.html')}"
        page.goto(file_path)

        # Add light class
        page.evaluate("document.documentElement.classList.add('light')")

        # Inject mock html safely
        # Note: Using triple quotes inside f-string in the previous attempt might have caused issues with newlines in JS evaluation
        # We will use a cleaner approach.

        script = """
            const tbody = document.getElementById('goals-sv-table-body');
            const mockContent = `
                <tr id="mock-row">
                    <td class="text-teal-400 p-4">Teal/Green Text (Should be #166534)</td>
                    <td class="text-green-400 p-4">Green Text (Should be #166534)</td>
                    <td class="text-orange-400 p-4">Orange/Red Text (Should be #991b1b)</td>
                    <td class="text-red-400 p-4">Red Text (Should be #991b1b)</td>
                    <td class="text-yellow-400 p-4">Yellow/Amber Text (Should be #92400e)</td>
                    <td class="text-purple-400 p-4">Purple Text (Should be #6b21a8)</td>
                    <td class="text-pink-400 p-4">Pink Text (Should be #9d174d)</td>
                </tr>
            `;

            if (tbody) {
                tbody.innerHTML = mockContent;
                // Reveal containers
                const wrapper = document.getElementById('content-wrapper');
                if (wrapper) wrapper.classList.remove('hidden');

                const goalsView = document.getElementById('goals-view');
                if (goalsView) goalsView.classList.remove('hidden');

                const tableContainer = document.getElementById('goals-sv-table-container');
                if (tableContainer) tableContainer.classList.remove('hidden');

                // Force white bg to see contrast
                document.body.style.backgroundColor = '#ffffff';
            } else {
                document.body.innerHTML = `<table style="background:white; width:100%">
                    <tbody id="goals-sv-table-body">${mockContent}</tbody>
                </table>`;
            }
        """

        page.evaluate(script)

        # Wait a bit for rendering
        page.wait_for_timeout(500)

        # Take screenshot of the body or the table
        page.screenshot(path="verification/goals_styles.png", full_page=True)

        browser.close()

if __name__ == "__main__":
    verify_goals_styles()
