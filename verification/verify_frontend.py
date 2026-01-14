import http.server
import socketserver
import threading
import time
from playwright.sync_api import sync_playwright

PORT = 8000

def start_server():
    Handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), Handler) as httpd:
        print("serving at port", PORT)
        httpd.serve_forever()

def verify():
    # Start server in a separate thread
    server_thread = threading.Thread(target=start_server)
    server_thread.daemon = True
    server_thread.start()

    # Give server a second to start
    time.sleep(2)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=True)
        page = browser.new_page()

        try:
            page.goto(f"http://localhost:{PORT}/index.html")

            # Since authentication is required, we expect the login screen ("Acesso Restrito") to appear.
            # We wait for the login card which has id "tela-login" or class "gatekeeper-card"
            page.wait_for_selector(".gatekeeper-card", timeout=10000)

            # Take screenshot
            page.screenshot(path="verification/verification.png")
            print("Screenshot taken.")

        except Exception as e:
            print(f"Verification failed: {e}")
        finally:
            browser.close()

if __name__ == "__main__":
    verify()
