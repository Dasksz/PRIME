from playwright.sync_api import sync_playwright

def verify_light_mode():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Navigate to the page
        page.goto('http://localhost:8000')

        # Aggressively remove overlays and force display
        page.evaluate("""() => {
            // Remove overlays
            const overlays = ['#page-transition-loader', '#loader', '#tela-login'];
            overlays.forEach(sel => {
                const el = document.querySelector(sel);
                if (el) el.remove();
            });

            // Show main content
            const main = document.querySelector('#tela-principal');
            if (main) main.style.display = 'block';

            // FORCE LIGHT MODE explicitly
            document.documentElement.classList.remove('dark');
            document.documentElement.classList.add('light');
            localStorage.setItem('theme', 'light');

            // Inject dummy table content with the problematic classes
            const tableContainer = document.querySelector('#conteudo-tabela');
            if (tableContainer) {
                tableContainer.innerHTML = `
                    <table class="w-full text-sm text-left text-gray-500 dark:text-gray-400">
                        <thead class="text-xs text-gray-700 uppercase bg-gray-50 dark:bg-gray-700 dark:text-gray-400">
                            <tr>
                                <th class="px-6 py-3">Column 1</th>
                                <th class="px-6 py-3">Dark Cell 1</th>
                                <th class="px-6 py-3">Dark Cell 2</th>
                                <th class="px-6 py-3">Adjustment</th>
                                <th class="px-6 py-3">Column 5</th>
                            </tr>
                        </thead>
                        <tbody>
                            <tr class="bg-white border-b dark:bg-gray-800 dark:border-gray-700 hover:bg-orange-500 group">
                                <td class="px-6 py-4">Normal Row</td>
                                <!-- These are the specific classes that were causing issues -->
                                <td class="px-6 py-4 bg-[#1e293b] text-center border-l border-gray-700">Dark Cell 1 (Should be Light)</td>
                                <td class="px-6 py-4 bg-[#151c36] text-center border-l border-gray-700">Dark Cell 2 (Should be Light)</td>
                                <td class="px-6 py-4 text-center border-l border-gray-700 text-yellow-500/70">Adjustment</td>
                                <td class="px-6 py-4">Normal Cell</td>
                            </tr>
                             <tr id="hover-row" class="bg-white border-b dark:bg-gray-800 dark:border-gray-700 hover:bg-orange-500 group">
                                <td class="px-6 py-4">Hover Me</td>
                                <td class="px-6 py-4 bg-[#1e293b] text-center border-l border-gray-700">Should be Transparent</td>
                                <td class="px-6 py-4 bg-[#151c36] text-center border-l border-gray-700">Should be Transparent</td>
                                <td class="px-6 py-4 text-center border-l border-gray-700 text-yellow-500/70">Should be Transparent</td>
                                <td class="px-6 py-4">Normal Cell</td>
                            </tr>
                        </tbody>
                    </table>
                `;
            }
        }""")

        # Wait a bit for styles to apply
        page.wait_for_timeout(1000)

        # Take static screenshot
        page.screenshot(path='verification/light_mode_static.png')
        print("Static screenshot taken.")

        browser.close()

if __name__ == '__main__':
    verify_light_mode()
