from playwright.sync_api import sync_playwright
import os

def run(playwright):
    browser = playwright.chromium.launch(headless=True)
    page = browser.new_page()

    # Load the local index.html
    cwd = os.getcwd()
    page.goto(f"file://{cwd}/index.html")

    # 1. Enable Light Mode via JS
    page.evaluate("""
        document.documentElement.classList.add('light');
        document.getElementById('checkbox-theme').checked = true;
    """)

    # 2. Force Show Table View (remove hidden class, hide others)
    # Also hide the loading screens, login screens, AND page-transition-loader which might overlay
    page.evaluate("""
        const hide = (id) => {
            const el = document.getElementById(id);
            if (el) el.classList.add('hidden');
        };
        const show = (id) => {
            const el = document.getElementById(id);
            if (el) el.classList.remove('hidden');
        };

        hide('tela-login');
        hide('tela-loading');
        hide('tela-pendente');
        hide('loader');
        hide('page-transition-loader'); // THIS WAS THE BLOCKER

        show('content-wrapper');
        show('main-dashboard');

        show('tableView');
        hide('chartView');
    """)

    # 3. Inject Mock Data into the table directly
    row_html = """
    <tr class="hover:bg-slate-700 transition-colors">
        <td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline">53008684</a></td>
        <td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline">11694</a></td>
        <td class="px-4 py-2">Test Vendor</td>
        <td class="px-4 py-2">Test Supplier</td>
        <td class="px-4 py-2">23/12/2025</td>
        <td class="px-4 py-2">24/12/2025</td>
        <td class="px-4 py-2 text-right">198,737 Kg</td>
        <td class="px-4 py-2 text-right">R$ 13.309,46</td>
        <td class="px-4 py-2 text-center"><span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-green-500/20 text-green-300">L</span></td>
    </tr>
    <tr class="hover:bg-slate-700 transition-colors">
        <td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline">53008670</a></td>
        <td class="px-4 py-2"><a href="#" class="text-teal-400 hover:underline">9500</a></td>
        <td class="px-4 py-2">Test Vendor 2</td>
        <td class="px-4 py-2">PEPSICO</td>
        <td class="px-4 py-2">19/12/2025</td>
        <td class="px-4 py-2">22/12/2025</td>
        <td class="px-4 py-2 text-right">278,503 Kg</td>
        <td class="px-4 py-2 text-right">R$ 18.054,91</td>
        <td class="px-4 py-2 text-center"><span class="inline-flex items-center px-2.5 py-0.5 rounded-full text-xs font-medium bg-yellow-500/20 text-yellow-300">F</span></td>
    </tr>
    """

    page.evaluate(f"""
        const tbody = document.getElementById('report-table-body');
        if (tbody) tbody.innerHTML = `{row_html}`;
    """)

    # Wait for visibility
    page.wait_for_timeout(500)

    # 4. Hover over the first row to trigger the effect
    row_locator = page.locator("#report-table-body tr").first
    row_locator.scroll_into_view_if_needed()
    row_locator.hover()

    # Wait for transition
    page.wait_for_timeout(500)

    # 5. Take Screenshot
    page.screenshot(path="verification_light_mode_table.png")

    browser.close()

with sync_playwright() as playwright:
    run(playwright)
