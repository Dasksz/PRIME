document.addEventListener('DOMContentLoaded', () => {
    const { createClient } = supabase;
    const supabaseClient = createClient(SUPABASE_URL, SUPABASE_ANON_KEY);

    supabaseClient.auth.onAuthStateChange(async (event, session) => {
        const telaLogin = document.getElementById('tela-login');
        const loader = document.getElementById('loader');

        if (session) {
            const { data: profile } = await supabaseClient
                .from('profiles')
                .select('status')
                .eq('id', session.user.id)
                .single();

            if (profile && profile.status === 'aprovado') {
                telaLogin.classList.add('hidden');
                loader.classList.remove('hidden');

                // A função já está disponível globalmente por causa do index.html
                await initializeNewDashboard(supabaseClient);

            } else {
                // Lógica para status pendente ou não encontrado
            }
        } else {
            telaLogin.classList.remove('hidden');
        }
    });
});
