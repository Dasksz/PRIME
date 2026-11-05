// Authentication UI for existing accounts. Profile approval is checked by init.js.
window.setupPrimePasswordRecovery = function(client, onPasswordUpdated) {
    const $ = id => document.getElementById(id);
    let recovering = false;
    const message = (text, error = false) => {
        const el = $('auth-message');
        el.textContent = text;
        el.style.color = error ? '#b91c1c' : '#166534';
    };
    const show = view => {
        ['password-signin-view', 'password-forgot-view', 'password-reset-view'].forEach(id => $(id).classList.toggle('hidden', id !== view));
        $('login-button').classList.toggle('hidden', view !== 'password-signin-view');
        $('auth-heading').textContent = view === 'password-reset-view' ? 'Criar nova senha' : view === 'password-forgot-view' ? 'Recuperar senha' : 'Acesso Restrito';
        message('');
    };
    $('password-forgot-link').addEventListener('click', () => show('password-forgot-view'));
    $('password-back-link').addEventListener('click', () => show('password-signin-view'));
    const bind = (id, action) => {
        const form = $(id);
        form.addEventListener('submit', async event => {
            event.preventDefault();
            const button = form.querySelector('button[type="submit"]');
            button.disabled = true;
            message('');
            try { await action(form); }
            catch (error) { message('Não foi possível concluir a solicitação. Tente novamente.', true); }
            finally { button.disabled = false; }
        });
    };
    bind('password-signin-form', async () => {
        const { error } = await client.auth.signInWithPassword({email:$('signin-email').value.trim(),password:$('signin-password').value});
        if (error) message('Não foi possível entrar. Confira seu e-mail e sua senha.', true);
    });
    bind('password-forgot-form', async () => {
        const { error } = await client.auth.resetPasswordForEmail($('recovery-email').value.trim(), {redirectTo:window.location.origin + window.location.pathname});
        if (error) { message('Não foi possível enviar o e-mail. Aguarde alguns instantes e tente novamente.', true); return; }
        show('password-signin-view');
        message('Se este e-mail estiver cadastrado, você receberá um link para redefinir sua senha.');
    });
    bind('password-reset-form', async form => {
        const password = $('new-password').value;
        if (password !== $('confirm-password').value) { message('As senhas não coincidem.', true); return; }
        if (password.length < 8 || !/[a-z]/.test(password) || !/[A-Z]/.test(password) || !/[^A-Za-z0-9\s]/.test(password)) {
            message('Use pelo menos 8 caracteres, uma letra maiúscula, uma minúscula e um caractere especial.', true); return;
        }
        const { error } = await client.auth.updateUser({password});
        if (error) { message('Não foi possível salvar a senha: ' + error.message, true); return; }
        recovering = false;
        form.reset();
        message('Senha atualizada com sucesso!');
        await onPasswordUpdated();
    });
    return {
        isRecovering: () => recovering,
        handleAuth(event) {
            if (event === 'PASSWORD_RECOVERY') {
                recovering = true;
                ['tela-loading','tela-pendente','content-wrapper','loader'].forEach(id => $(id)?.classList.add('hidden'));
                $('tela-login').classList.remove('hidden');
                show('password-reset-view');
                return true;
            }
            if (event === 'SIGNED_OUT') { recovering = false; show('password-signin-view'); }
            return recovering;
        }
    };
};
