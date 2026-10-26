// Shared, deterministic calculations for goal distribution and input validation.
(function(root) {
    function allocate(total, weights, precision = 0) {
        if (!Number.isFinite(total) || total < 0) throw new Error('A meta deve ser um número positivo ou zero.');
        const units = Math.round(total * 10 ** precision);
        if (!weights.length) return [];
        const clean = weights.map(w => Number.isFinite(w) && w > 0 ? w : 0);
        const sum = clean.reduce((a, b) => a + b, 0);
        const quotas = clean.map(w => units * (sum ? w / sum : 1 / clean.length));
        const result = quotas.map(Math.floor);
        const ranked = quotas.map((q, i) => ({i, remainder: q - result[i]})).sort((a, b) => b.remainder - a.remainder || a.i - b.i);
        for (let left = units - result.reduce((a, b) => a + b, 0), i = 0; left > 0; left--, i++) result[ranked[i % ranked.length].i]++;
        return result.map(v => v / 10 ** precision);
    }
    function category(sale) {
        const supplier = String(sale.CODFOR);
        if (supplier !== '1119') return supplier;
        const desc = String(sale.DESCRICAO || '').normalize('NFD').replace(/[\u0300-\u036f]/g, '').toUpperCase();
        if (desc.includes('TODDYNHO')) return '1119_TODDYNHO';
        if (desc.includes('TODDY')) return '1119_TODDY';
        if (desc.includes('QUAKER') || desc.includes('KEROCOCO')) return '1119_QUAKER_KEROCOCO';
        return null;
    }
    function parseInput(value) {
        const text = String(value).trim();
        if (!/^-?\d+(?:\.\d{3})*(?:,\d+)?$/.test(text)) throw new Error('Informe um número válido, usando vírgula para decimais.');
        const number = Number(text.replace(/\./g, '').replace(',', '.'));
        if (!Number.isFinite(number) || number < 0) throw new Error('A meta não pode ser negativa.');
        return number;
    }
    const api = {allocate, category, parseInput};
    root.GoalsCore = api;
    if (typeof module !== 'undefined') module.exports = api;
})(typeof self !== 'undefined' ? self : globalThis);
