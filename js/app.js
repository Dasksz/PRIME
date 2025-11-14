// js/app.js

document.addEventListener('DOMContentLoaded', () => {

    /**
     * Waits for the global Supabase client to be available before initializing the app.
     * This prevents race conditions where app scripts run before the Supabase CDN script has loaded.
     * @param {Function} callback - The function to call once Supabase is ready.
     * @param {number} [timeout=15000] - Maximum time to wait in milliseconds (Increased to 15s).
     * @param {number} [interval=50] - Time to wait between checks in milliseconds.
     */
    function waitForSupabase(callback, timeout = 15000, interval = 50) {
        let elapsedTime = 0;

        const check = () => {
            // Check if the global 'supabase' object and its 'createClient' method are available
            if (window.supabase && typeof window.supabase.createClient === 'function') {
                callback();
            } else {
                elapsedTime += interval;
                if (elapsedTime < timeout) {
                    setTimeout(check, interval);
                } else {
                    console.error('Erro Fatal: A biblioteca do Supabase não carregou a tempo.');
                    displayFatalError();
                }
            }
        };
        check();
    }

    function displayFatalError() {
        const body = document.querySelector('body');
        if (body) {
            body.innerHTML = '<div style="text-align: center; padding: 50px; font-family: sans-serif;"><h1>Erro Crítico</h1><p>A aplicação não pôde ser carregada. Por favor, verifique a sua ligação à internet e tente novamente.</p></div>';
        }
    }

    // Wait for Supabase to be ready, then initialize the authentication logic
    waitForSupabase(() => {
        if (typeof initAuth === 'function') {
            initAuth();
        } else {
            console.error('Erro Fatal: Função initAuth não encontrada.');
            displayFatalError();
        }
    });
});
