// js/auth.js

function initAuth() {
    const SUPABASE_URL = 'https://dhozwhfmrwiumwpcqabi.supabase.co';
    const SUPABASE_ANON_KEY = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzdXBhYmFzZSIsInJlZiI6ImRob3pod2hmbXJ3aXVtd3BjcWFiaSIsInJvbGUiOiJhbm9uIiwiaWF0IjoxNzE3Njg2OTg2LCJleHAiOjIwMzMyNjI5ODZ9.5-a-pY8-28fCo-2p3n5nId0JHshqi24NnHiOq-zCOcE';

    // CORREÇÃO: Acessa createClient diretamente do objeto global supabase
    // A variável global é 'window.supabase', e não apenas 'supabase'.
    const supabaseClient = window.supabase.createClient(SUPABASE_URL, SUPABASE_ANON_KEY);
    
    // ADICIONADO: Expõe o cliente para o escopo global para facilitar o teste
    window.supabaseClient = supabaseClient;

    // --- DOM Elements ---
    const telaLogin = document.getElementById('tela-login');
    const telaLoading = document.getElementById('tela-loading');
    const telaPendente = document.getElementById('tela-pendente');
    const loginButton = document.getElementById('login-button');

    // --- State Variables ---
    let dashboardInitialized = false;
    let isHandlingAuth = false;
    let profilesListener = null;
    let pollingInterval = null;

    // --- UI Functions ---
    const showScreen = (screenId) => {
        // Hide all screens first, then show the target one
        [telaLogin, telaLoading, telaPendente].forEach(el => el?.classList.add('hidden'));
        if (screenId) {
            const screen = document.getElementById(screenId);
            screen?.classList.remove('hidden');
        }
    };

    // --- Authentication Logic ---
    const handleLogin = async () => {
        const cleanUrl = window.location.origin + window.location.pathname;
        const { error } = await supabaseClient.auth.signInWithOAuth({
            provider: 'google',
            options: { redirectTo: cleanUrl }
        });
        if (error) console.error('Erro no login:', error);
    };

    const handleLogout = () => {
        supabaseClient.auth.signOut();
        // Remove todas as chaves do localStorage que correspondem ao padrão do Supabase v2
        Object.keys(localStorage).forEach(key => {
            if (key.startsWith('sb-') || key.startsWith('supabase.')) {
                localStorage.removeItem(key);
            }
        });
        localStorage.removeItem('userStatus'); // Remove o status do usuário em cache
        const cleanUrl = window.location.origin + window.location.pathname;
        window.location.href = cleanUrl;
    };

    // --- Realtime & Polling for Pending Status ---
    const stopProfilesListener = () => {
        if (profilesListener) {
            supabaseClient.removeChannel(profilesListener);
            profilesListener = null;
        }
        if (pollingInterval) {
            clearInterval(pollingInterval);
            pollingInterval = null;
        }
    };

    const startPendingStatusPolling = (userId, onApproval) => {
        if (pollingInterval) clearInterval(pollingInterval);
        pollingInterval = setInterval(async () => {
            const { data: profile, error } = await supabaseClient
                .from('profiles')
                .select('status')
                .eq('id', userId)
                .single();
            if (profile && profile.status === 'aprovado') {
                stopProfilesListener();
                onApproval();
            }
        }, 30000);
    };

    const startProfilesRealtimeListener = (userId, onApproval) => {
        stopProfilesListener();
        profilesListener = supabaseClient
            .channel(`profiles:id=eq.${userId}`)
            .on('postgres_changes', {
                event: 'UPDATE',
                schema: 'public',
                table: 'profiles',
                filter: `id=eq.${userId}`
            }, (payload) => {
                if (payload.new?.status === 'aprovado') {
                    stopProfilesListener();
                    onApproval();
                }
            })
            .subscribe((status, err) => {
                if (status === 'SUBSCRIBED' && pollingInterval) {
                    clearInterval(pollingInterval);
                    pollingInterval = null;
                }
                if (status === 'CHANNEL_ERROR' || status === 'TIMED_OUT' || err) {
                    startPendingStatusPolling(userId, onApproval);
                }
            });
    };

    // --- Main Auth Flow ---
    supabaseClient.auth.onAuthStateChange(async (event, session) => {
        if (isHandlingAuth) return;
        isHandlingAuth = true;

        if (event === 'SIGNED_OUT') {
            localStorage.removeItem('userStatus');
            dashboardInitialized = false;
            stopProfilesListener();
            showScreen('tela-login');
            isHandlingAuth = false;
            return;
        }

        if (session) {
            const cachedStatus = localStorage.getItem('userStatus');
            if (cachedStatus === 'aprovado' && !dashboardInitialized) {
                dashboardInitialized = true;
                showScreen(null); // Hide all auth screens
                if (typeof initializeNewDashboard === 'function') {
                    await initializeNewDashboard(supabaseClient);
                } else {
                    console.error("Erro: initializeNewDashboard() não encontrada.");
                }
                isHandlingAuth = false;
                return;
            }

            if (!dashboardInitialized) {
                showScreen('tela-loading');
                let attempt = 0;
                while (attempt < 3) {
                    try {
                        const { data: profile, error } = await supabaseClient
                            .from('profiles')
                            .select('status')
                            .eq('id', session.user.id)
                            .single();

                        if (error && error.code !== 'PGRST116') throw error;

                        if (profile?.status === 'aprovado') {
                            localStorage.setItem('userStatus', 'aprovado');
                            dashboardInitialized = true;
                            showScreen(null); // Hide all auth screens
                            if (typeof initializeNewDashboard === 'function') {
                                await initializeNewDashboard(supabaseClient);
                            }
                            isHandlingAuth = false;
                            return;
                        }

                        if (profile?.status === 'pendente') {
                            localStorage.setItem('userStatus', 'pendente');
                            showScreen('tela-pendente');
                            startProfilesRealtimeListener(session.user.id, () => {
                                if (!dashboardInitialized) {
                                    localStorage.setItem('userStatus', 'aprovado');
                                    window.location.reload(true);
                                }
                            });
                            isHandlingAuth = false;
                            return;
                        }
                    } catch (e) {
                        console.error(`Tentativa ${attempt + 1} falhou:`, e.message);
                    }
                    attempt++;
                    if (attempt < 3) await new Promise(res => setTimeout(res, 1000));
                }
                console.error("Não foi possível obter o perfil do usuário após 3 tentativas. Deslogando.");
                handleLogout();

            }
        } else {
            // No session, ensure login screen is visible
            showScreen('tela-login');
        }
        isHandlingAuth = false;
    });

    // --- Event Listeners ---
    loginButton?.addEventListener('click', handleLogin);
    document.body.addEventListener('click', (event) => {
        if (event.target.matches('button.logout')) {
            handleLogout();
        }
    });
}
