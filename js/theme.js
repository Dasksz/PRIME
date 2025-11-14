// js/theme.js

const professionalPalette = [
    "#264653", "#2a9d8f", "#e9c46a", "#f4a261", "#e76f51",
    "#8ecae6", "#219ebc", "#023047", "#ffb703", "#fb8500",
    "#0077b6", "#00b4d8", "#90e0ef", "#ced4da", "#6c757d"
];

/**
 * Initializes the theme switcher functionality.
 * Reads the saved theme from localStorage and sets up the event listener for the toggle.
 */
function setupTheme() {
    const themeToggle = document.getElementById('theme-toggle-checkbox');
    const currentTheme = localStorage.getItem('theme');

    if (currentTheme) {
        document.documentElement.setAttribute('data-theme', currentTheme);
        if (currentTheme === 'dark') {
            themeToggle.checked = true;
        }
    }

    themeToggle.addEventListener('change', function() {
        if (this.checked) {
            document.documentElement.setAttribute('data-theme', 'dark');
            localStorage.setItem('theme', 'dark');
        } else {
            document.documentElement.setAttribute('data-theme', 'light');
            localStorage.setItem('theme', 'light');
        }
    });

    // Handle visibility changes to prevent chart rendering glitches
    document.addEventListener('visibilitychange', () => {
        if (document.visibilityState === 'visible') {
            if (window.Chart && typeof window.Chart.instances === 'object') {
                Object.values(window.Chart.instances).forEach(chart => {
                    if (chart) {
                       chart.update();
                    }
                });
            }
        }
    });
}
