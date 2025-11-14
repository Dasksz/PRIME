// js/app.js
document.addEventListener('DOMContentLoaded', () => {
    if (typeof initAuth === 'function') {
        initAuth();
    } else {
        console.error('Erro Fatal: Função initAuth não encontrada. O script de autenticação pode estar em falta ou ter falhado ao carregar.');
        const body = document.querySelector('body');
        if (body) {
            body.innerHTML = '<div style="text-align: center; padding: 50px; font-family: sans-serif;"><h1>Erro Crítico</h1><p>A aplicação não pôde ser carregada. Por favor, contacte o suporte.</p></div>';
        }
    }
});
