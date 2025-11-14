// js/uploader.js

let supabaseUploaderClient;
let uploaderWorker;

/**
 * Initializes the uploader functionality, setting up event listeners.
 */
function setupUploader() {
    const modal = document.getElementById('admin-uploader-modal');
    const closeBtn = document.getElementById('admin-modal-close-btn');
    const generateBtn = document.getElementById('generate-btn');
    const statusContainer = document.getElementById('status-container');
    const statusText = document.getElementById('status-text');
    const progressBar = document.getElementById('progress-bar');

    // File inputs
    const salesFileInput = document.getElementById('sales-file-input');
    const clientsFileInput = document.getElementById('clients-file-input');
    const productsFileInput = document.getElementById('products-file-input');
    const historyFileInput = document.getElementById('history-file-input');
    const innovationsFileInput = document.getElementById('innovations-file-input');

    if (!modal || !generateBtn) return;

    // --- Close Modal Logic ---
    const closeModal = () => modal.classList.add('hidden');
    closeBtn.addEventListener('click', closeModal);
    modal.addEventListener('click', (e) => {
        if (e.target === modal) closeModal();
    });

    // --- Toggle Password Visibility ---
    const toggleBtn = document.getElementById('toggle-supabase-key');
    const supabaseKeyInput = document.getElementById('supabase-key');
    const eyeIcon = document.getElementById('eye-icon');
    const eyeOffIcon = document.getElementById('eye-off-icon');

    if (toggleBtn && supabaseKeyInput && eyeIcon && eyeOffIcon) {
        toggleBtn.addEventListener('click', () => {
            if (supabaseKeyInput.type === 'password') {
                supabaseKeyInput.type = 'text';
                eyeIcon.classList.add('hidden');
                eyeOffIcon.classList.remove('hidden');
            } else {
                supabaseKeyInput.type = 'password';
                eyeIcon.classList.remove('hidden');
                eyeOffIcon.classList.add('hidden');
            }
        });
    }

    // --- Worker Initialization ---
    if (window.Worker) {
        uploaderWorker = new Worker('js/worker.js');
        uploaderWorker.onmessage = function(e) {
            const { type, message, progress } = e.data;
            statusText.textContent = message;
            progressBar.style.width = `${progress}%`;

            if (type === 'error') {
                statusText.classList.add('text-red-500');
                generateBtn.disabled = false;
            }
            if (type === 'complete') {
                 statusText.classList.remove('text-red-500');
                 statusText.classList.add('text-green-500');
                 setTimeout(() => {
                    statusContainer.classList.add('hidden');
                    generateBtn.disabled = false;
                    alert('Upload concluído com sucesso! A página será recarregada para refletir os novos dados.');
                    window.location.reload();
                 }, 2000);
            }
        };
    } else {
        console.error('Web Workers não são suportados neste navegador.');
        // Display an error message to the user in the UI
    }

    // --- Main Upload Logic ---
    generateBtn.addEventListener('click', async () => {
        const supabaseUrl = document.getElementById('supabase-url').value;
        const supabaseKey = document.getElementById('supabase-key').value;

        if (!supabaseUrl || !supabaseKey) {
            alert('Por favor, preencha a URL e a Chave Secreta do Supabase.');
            return;
        }

        const files = {
            sales: salesFileInput.files[0],
            clients: clientsFileInput.files[0],
            products: productsFileInput.files[0],
            history: historyFileInput.files[0],
            innovations: innovationsFileInput.files[0],
        };

        const hasFiles = Object.values(files).some(f => f);
        if (!hasFiles) {
            alert('Por favor, selecione pelo menos um arquivo para fazer o upload.');
            return;
        }

        generateBtn.disabled = true;
        statusContainer.classList.remove('hidden');
        statusText.classList.remove('text-red-500', 'text-green-500');
        statusText.textContent = 'Iniciando upload...';
        progressBar.style.width = '0%';

        // Send data to the worker for processing and uploading
        uploaderWorker.postMessage({
            type: 'start_upload',
            files,
            supabaseCredentials: { url: supabaseUrl, key: supabaseKey }
        });
    });
}

// Initial setup call when the script is loaded.
// It should be called after the DOM is fully loaded.
// We'll tie this into our main initialization flow.
