// js/utils.js

/**
 * Formats a number as Brazilian currency (BRL).
 * @param {number} value - The number to format.
 * @returns {string} The formatted currency string (e.g., "R$ 1.234,56").
 */
function formatCurrency(value) {
    if (typeof value !== 'number') {
        value = 0;
    }
    return value.toLocaleString('pt-BR', { style: 'currency', currency: 'BRL' });
}

/**
 * Formats a number with thousand separators for the pt-BR locale.
 * @param {number} value - The number to format.
 * @param {number} decimalPlaces - The number of decimal places to show.
 * @returns {string} The formatted number string (e.g., "1.234,56").
 */
function formatNumber(value, decimalPlaces = 2) {
    if (typeof value !== 'number') {
        value = 0;
    }
    return value.toLocaleString('pt-BR', {
        minimumFractionDigits: decimalPlaces,
        maximumFractionDigits: decimalPlaces,
    });
}

/**
 * Formats a number as a percentage.
 * @param {number} value - The number to format (e.g., 0.25 for 25%).
 * @param {number} decimalPlaces - The number of decimal places to show.
 * @returns {string} The formatted percentage string (e.g., "25,0%").
 */
function formatPercentage(value, decimalPlaces = 1) {
    if (typeof value !== 'number') {
        value = 0;
    }
    return value.toLocaleString('pt-BR', {
        style: 'percent',
        minimumFractionDigits: decimalPlaces,
        maximumFractionDigits: decimalPlaces,
    });
}

/**
 * Parses a date string in 'YYYY-MM-DD' format and returns a Date object in UTC.
 * @param {string} dateString - The date string to parse.
 * @returns {Date|null} A Date object or null if the input is invalid.
 */
function parseDateUTC(dateString) {
    if (!dateString) return null;
    const parts = dateString.split('-');
    if (parts.length === 3) {
        const year = parseInt(parts[0], 10);
        const month = parseInt(parts[1], 10) - 1; // Month is 0-indexed
        const day = parseInt(parts[2], 10);
        return new Date(Date.UTC(year, month, day));
    }
    return null;
}

/**
 * Debounces a function, ensuring it's only called after a certain delay
 * since the last time it was invoked.
 * @param {Function} func - The function to debounce.
 * @param {number} delay - The debounce delay in milliseconds.
 * @returns {Function} The debounced function.
 */
function debounce(func, delay) {
    let timeoutId;
    return function(...args) {
        clearTimeout(timeoutId);
        timeoutId = setTimeout(() => {
            func.apply(this, args);
        }, delay);
    };
}
