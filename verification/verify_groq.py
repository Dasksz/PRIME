from playwright.sync_api import sync_playwright
import os

def test_groq_button(page):
    # Load index.html
    cwd = os.getcwd()
    page.on("console", lambda msg: print(f"Browser Console: {msg.text}"))
    page.goto(f"file://{cwd}/index.html")

    # Inject mock data and scripts
    page.evaluate("""
        window.isDataLoaded = true;

        window.embeddedData = {
            metadata: [{ key: 'groq_api_key', value: 'TEST_KEY' }],
            isColumnar: false,
            detailed: [
                { 'CODUSUR': '1001', 'NOME': 'VENDEDOR A', 'SUPERV': 'SUP A', 'DTPED': Date.now() }
            ],
            history: [
                 { 'CODUSUR': '1001', 'NOME': 'VENDEDOR A', 'SUPERV': 'SUP A', 'DTPED': Date.now(), 'CODFOR': '707', 'VLVENDA': 500 }
            ],
            clients: [
                { 'Código': '1', 'rca1': '1001', 'nomeCliente': 'CLIENTE 1' }
            ],
            byOrder: [],
            stockMap05: {},
            stockMap08: {},
            innovationsMonth: [],
            activeProductCodes: [],
            productDetails: {},
            clientCoordinates: [],
            passedWorkingDaysCurrentMonth: 1
        };

        window.supabaseClient = {
            from: () => ({
                select: () => ({
                    order: () => ({
                        limit: () => Promise.resolve({ data: [] })
                    }),
                    maybeSingle: () => Promise.resolve({ data: null })
                }),
                upsert: () => Promise.resolve({ error: null })
            }),
            auth: {
                getSession: () => Promise.resolve({ data: { session: null } }),
                onAuthStateChange: () => {}
            }
        };

        document.getElementById('tela-login').classList.add('hidden');
        document.getElementById('tela-loading').classList.add('hidden');
        document.getElementById('content-wrapper').classList.remove('hidden');

        console.log("Loading app.js...");
        const script = document.createElement('script');
        script.src = 'app.js';
        script.onload = () => console.log("app.js loaded");
        document.body.appendChild(script);
    """)

    page.wait_for_timeout(2000)

    # Force show Goals View
    page.evaluate("""
        document.getElementById('main-dashboard').classList.add('hidden');
        document.getElementById('goals-view').classList.remove('hidden');
        document.getElementById('goals-sv-content').classList.remove('hidden');
        document.getElementById('import-goals-modal').classList.remove('hidden');
    """)

    # Fill Textarea with TSV
    tsv_content = "EXTRUSADOS\t\t\nFATURAMENTO\t\t\nAJUSTE\t\t\n1001\tVENDEDOR A\t1000"
    page.fill('#import-goals-textarea', tsv_content)

    # Click Analyze
    print("Clicking Analyze...")
    page.click('#import-goals-analyze-btn')

    # Locate AI Button (it should become visible after analysis if valid data found)
    ai_btn = page.locator('#btn-generate-ai')

    if not ai_btn.is_visible():
        print("AI Button not visible after analysis. Check inputs.")
        # Debug: check analysis container visibility
        visible = page.evaluate("!document.getElementById('import-analysis-container').classList.contains('hidden')")
        print(f"Analysis container visible: {visible}")
        return

    # Intercept fetch
    def handle_route(route):
        request = route.request
        if "api.groq.com" in request.url:
            print(f"Intercepted request to: {request.url}")
            print(f"Method: {request.method}")
            print(f"Post Data: {request.post_data}")

            route.fulfill(
                status=200,
                content_type="application/json",
                body='{"choices": [{"message": {"content": "Groq Response Success"}}]}'
            )
        else:
            route.continue_()

    page.route("**/*", handle_route)

    print("Clicking AI button...")
    ai_btn.click()

    page.wait_for_timeout(3000)

    content = page.inner_text('#ai-insights-content')
    print(f"AI Content: {content}")

    page.screenshot(path="verification/groq_verification.png")

with sync_playwright() as p:
    browser = p.chromium.launch()
    page = browser.new_page()
    try:
        test_groq_button(page)
    except Exception as e:
        print(f"Error: {e}")
    finally:
        browser.close()
