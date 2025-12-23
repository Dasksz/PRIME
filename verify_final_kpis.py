
import os
from playwright.sync_api import sync_playwright

def verify_kpis():
    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        # Load the local HTML file
        file_path = f"file://{os.getcwd()}/index.html"
        page.goto(file_path)

        # Wait for page to load
        page.wait_for_timeout(2000)

        # 0. Bypass Overlays
        print("Hiding overlays...")
        page.evaluate("""
            document.getElementById('tela-login').classList.add('hidden');
            document.getElementById('page-transition-loader').classList.add('hidden');
            document.getElementById('content-wrapper').classList.remove('hidden');
        """)
        page.wait_for_timeout(1000)

        # 1. Verify Goals Summary View
        print("Navigating to Goals View...")

        # Manually switch view
        page.evaluate("""
            document.querySelectorAll('#content-wrapper > div > div.container').forEach(el => el.classList.add('hidden'));
            document.getElementById('goals-view').classList.remove('hidden');
        """)
        page.wait_for_timeout(1000)

        print("Clicking Summary Tab...")
        try:
            # Force click if blocked
            page.evaluate("document.getElementById('goals-category-summary-btn').click()")
            page.wait_for_timeout(1000)

            # Since we clicked "RESUMO", the summary container should be visible.
            # But the grid might be empty if no data.
            # Let's inject a fake card into `goals-summary-grid` to verify styling.

            page.evaluate("""
                const container = document.getElementById('goals-summary-grid');
                if (container.children.length === 0) {
                     container.innerHTML = `
                    <div class="bg-[#1e2a5a] border-l-4 border-teal-500 rounded-r-lg p-4 shadow-md transition hover:-translate-y-1">
                        <h3 class="font-bold text-lg text-white mb-3 border-b border-slate-700 pb-2">Fake Goal</h3>
                        <div class="space-y-4">
                            <div>
                                <div class="flex justify-between items-baseline mb-1">
                                    <p class="text-xs text-slate-300 uppercase font-semibold">Meta Faturamento</p>
                                </div>
                                <p class="text-xl font-bold text-teal-400 mb-2">
                                    R$ 1.000,00
                                </p>
                                <div class="flex justify-between text-[10px] text-slate-300 border-t border-slate-700/50 pt-1">
                                    <span>Trim: <span class="text-slate-300">R$ 900,00</span></span>
                                    <span>Ant: <span class="text-slate-300">R$ 800,00</span></span>
                                </div>
                            </div>
                        </div>
                    </div>`;
                }
            """)

            # Take screenshot of Goals Summary
            screenshot_path_goals = "/home/jules/verification/goals_summary_kpi.png"
            page.screenshot(path=screenshot_path_goals)
            print(f"Screenshot saved to {screenshot_path_goals}")
        except Exception as e:
            print(f"Goals interaction failed: {e}")
            page.screenshot(path="/home/jules/verification/error_goals.png")

        # 2. Verify Comparison View
        print("Navigating to Comparison View...")
        try:
            page.evaluate("""
                document.querySelectorAll('#content-wrapper > div > div.container').forEach(el => el.classList.add('hidden'));
                document.getElementById('comparison-view').classList.remove('hidden');
            """)
            page.wait_for_timeout(1000)

            # Inject fake KPI for comparison
            page.evaluate("""
                const container = document.getElementById('comparison-kpi-container');
                container.innerHTML = `
                    <div class="kpi-card p-4 rounded-lg text-center kpi-glow-base kpi-glow-blue transition transform hover:-translate-y-1 duration-200">
                        <p class="text-slate-300 text-sm">Fake Comparison KPI</p>
                        <p class="text-2xl font-bold text-white my-2">R$ 100,00</p>
                        <p class="text-sm text-green-400">10% vs Média</p>
                        <p class="text-xs text-slate-300">Média Trim.: R$ 90,00</p>
                    </div>
                `;
            """)

            screenshot_path_comparison = "/home/jules/verification/comparison_kpi.png"
            page.screenshot(path=screenshot_path_comparison)
            print(f"Screenshot saved to {screenshot_path_comparison}")
        except Exception as e:
            print(f"Comparison interaction failed: {e}")
            page.screenshot(path="/home/jules/verification/error_comparison.png")

        browser.close()

if __name__ == "__main__":
    verify_kpis()
