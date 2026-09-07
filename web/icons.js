// web/icons.js
// Inline SVG, tanpa dependensi icon font. Semua ikon memakai stroke:currentColor
// supaya mewarisi warna elemen induknya (cyan di panel, coral di tombol hapus).
const ICONS = {
  trash:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" ' +
    'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M3 6h18"/><path d="M8 6V4a1 1 0 0 1 1-1h6a1 1 0 0 1 1 1v2"/>' +
    '<path d="M19 6l-1 14a2 2 0 0 1-2 2H8a2 2 0 0 1-2-2L5 6"/>' +
    '<path d="M10 11v6"/><path d="M14 11v6"/></svg>',

  plus:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" ' +
    'stroke-linecap="round" aria-hidden="true">' +
    '<path d="M12 5v14"/><path d="M5 12h14"/></svg>',

  search:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" ' +
    'stroke-linecap="round" aria-hidden="true">' +
    '<circle cx="11" cy="11" r="7"/><path d="M20 20l-3.5-3.5"/></svg>',

  pin:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" ' +
    'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<path d="M12 21s7-6.2 7-11a7 7 0 1 0-14 0c0 4.8 7 11 7 11z"/>' +
    '<circle cx="12" cy="10" r="2.5"/></svg>',

  // Galeri preview: bingkai citra dengan garis horizon + matahari, dipakai di
  // judul bagian Preview pada panel Struktur.
  image:
    '<svg viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" ' +
    'stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">' +
    '<rect x="3" y="4" width="18" height="16" rx="2"/>' +
    '<circle cx="8.5" cy="9.5" r="1.5"/>' +
    '<path d="M21 15l-5-4-4.5 5L9 14l-6 5"/></svg>',
};
