// Urutan eksekusi tahap per scene. PREVIEW harus ikut walau bukan tier
const STAGE_ORDER = ['DOWNLOAD','CROP','LEE_FILTER','QUALITY_ANALYTICS','GOLD_EXPORT','PREVIEW','FUSION','CLEANUP'];
// Palet tier: lima hue kategorikal, divalidasi terhadap permukaan gelap
// #121A2B (lightness band, chroma floor, pemisahan CVD pasangan bersebelahan,
// dan kontras). SILVER dulu #9FB0C9 yang chroma-nya di bawah ambang (terbaca
// abu-abu) dan cuma berjarak dE 12 dari GOLD -- dua tier bersebelahan yang
// sulit dibedakan bahkan dengan penglihatan warna normal.

// Palet source untuk panel Struktur. Sengaja jadi satu-satunya dimensi warna
// di panel itu -- tier di sana ditandai teks, bukan warna -- supaya satu hue
// tidak pernah berarti dua hal dalam satu komponen.
const SOURCE_COLORS = { sentinel1: '#5B8DEF', modis: '#2FA07E', gpm: '#C4762E' };
const SOURCE_LABELS = { sentinel1: 'Sentinel-1', modis: 'MODIS', gpm: 'GPM', fusion: 'Fusion', preview: 'Preview' };
// Urutan tampil satelit, sama dengan SOURCE_ORDER di etl/processing_plan.py.
const SOURCE_ORDER_KEYS = ['sentinel1', 'modis', 'gpm'];

// Tier yang punya folder di disk, untuk rincian storage. Beda dari TIER_ORDER
// di atas: itu rantai lineage yang bisa diminta user dan digambar di ring
// progres, sementara PREVIEW adalah turunan (PNG hasil render dari COG) yang
// tidak pernah ada di required_tiers tapi tetap memakan disk dan tetap harus
// muncul di rincian. Urutannya mengikuti urutan eksekusi pipeline.
const STORAGE_TIER_ORDER = [
  'RAW', 'ALIGNED', 'DESPECKLED', 'INDICES', 'ACCUMULATED', 'COG', 'PREVIEW', 'FUSED',
  // Nama pra-D14, supaya dataset lama tetap terurut benar kalau sempat dirender.
  'BRONZE', 'SILVER', 'GOLD', 'FUSION',
];

// Label tier di layar. Dipetakan ke LACI tempat berkasnya benar-benar berada,
// karena itulah yang user lihat saat membuka hasil unduhan: ALIGNED ada di
// {source}/RAW/, COG di {source}/PROCESSED/. Tier yang tidak punya laci
// (artefak antara) memakai namanya sendiri.
const TIER_LABEL = {
  ALIGNED: 'RAW', BRONZE: 'RAW',
  COG: 'PROCESSED', GOLD: 'PROCESSED',
  FUSED: 'FUSION',
};
function tierLabel(t) { return TIER_LABEL[String(t).toUpperCase()] || t; }

// Laci tempat tier itu bermuara, untuk lapisan tengah pohon Struktur.
// Tier rank 2 (DESPECKLED/INDICES/ACCUMULATED) adalah artefak antara yang
// disapu di akhir job, jadi biasanya tidak muncul -- tapi kalau sempat
// terlihat saat job berjalan, tempatnya di jalur PROCESSED.
const TIER_LEVEL = {
  RAW: 'RAW', ALIGNED: 'RAW', BRONZE: 'RAW',
  DESPECKLED: 'PROCESSED', INDICES: 'PROCESSED', ACCUMULATED: 'PROCESSED',
  SILVER: 'PROCESSED', COG: 'PROCESSED', GOLD: 'PROCESSED',
};

// Tier lintas-source: tidak bisa dipecah per sensor, jadi dikeluarkan dari
// legenda source supaya tidak terbaca sebagai sensor keempat.
const SOURCELESS_TIERS = ['fusion', 'preview'];
const ACTIVE_STATUSES = new Set(['QUEUED','PREPARING','DOWNLOADING','PROCESSING','PAUSED','CLEANUP','DELETING']);

const state = {
  datasets: [], progress: {}, logs: {}, firstLogAt: {}, pollTimer: null, livePollTimer: null,
  openScenes: new Set(), openStructure: new Set(), structureHTML: {}, cardElements: {},
  // Kartu dataset yang badannya sedang dilipat (hanya kepala/ringkasan yang
  // terlihat). Kosong = semua terbuka seperti sebelumnya; per-kartu, bertahan
  // antar-polling sama seperti openScenes/openStructure.
  collapsedCards: new Set(),
  // Panel "Gabungkan Dataset": accordion tunggal (bukan per-baris), karena
  // barisnya berbagi satu konteks (tanggal siap digabung hari ini).
  mergeCollapsed: false,
  // Galeri preview per dataset: payload /api/datasets/{id}/preview, plus
  // tanggal dan jenis yang sedang dipilih (bertahan saat panel digambar ulang
  // oleh polling).
  previews: {}, previewScene: {}, previewKind: {}, previewLevel: {}, previewClosed: {},
  // Lokasi: daftar dari /api/regions, filter pencarian, dan pilihan yang dipakai
  // "Buat Dataset". selectedRegionId adalah satu-satunya sumber kebenaran lokasi.
  regions: [], selectedRegionId: null, locationQuery: '',
  geoResults: [], pendingDeleteRegionId: null,
};

async function api(path, options) {
  const opts = Object.assign({ headers: { 'Content-Type': 'application/json' } }, options || {});
  const res = await fetch(path, opts);
  let data = null;
  try { data = await res.json(); } catch (e) {}
  if (!res.ok) {
    const msg = (data && data.detail) ? data.detail : ('Permintaan gagal (' + res.status + ')');
    throw new Error(msg);
  }
  return data;
}

function showToast(message, kind) {
  const stack = document.getElementById('toastStack');
  const el = document.createElement('div');
  el.className = 'toast ' + (kind || '');
  el.textContent = message;
  stack.appendChild(el);
  setTimeout(() => el.remove(), 4200);
}

function escapeHTML(s) {
  const d = document.createElement('div');
  d.textContent = s == null ? '' : String(s);
  return d.innerHTML;
}

function humanBytes(n) {
  if (!n) return '0 MB';
  const gb = n / 1e9;
  if (gb >= 1) return gb.toFixed(2) + ' GB';
  return (n / 1e6).toFixed(1) + ' MB';
}

function statusToClass(status) {
  if (['COMPLETED'].includes(status)) return 'ok';
  if (['PAUSED', 'QUEUED', 'PREPARING', 'PENDING'].includes(status)) return 'warn';
  if (['FAILED', 'CANCELLED', 'DELETING'].includes(status)) return 'danger';
  return 'active';
}

// Radar progres kartu dataset: satu cincin per lapisan kerja yang benar-benar
// dijalankan dataset ini (dari /status -> layers). Urutan dari dalam ke luar
// mengikuti urutan pipeline; satelit yang tidak diunduh tidak punya cincin.
const LAYER_COLORS = {
  // "Muda" = pastel pucat (saturasi rendah, hampir putih), "neon" = saturasi
  // penuh -- sengaja dibuat jauh supaya unduh vs proses satu satelit tidak
  // tertukar.
  sentinel1_download: '#D6ECFF', sentinel1_processing: '#00B7FF',
  modis_download: '#D8F7DC',     modis_processing: '#39FF14',
  gpm_download: '#FFF4C7',       gpm_processing: '#FFE600',
  fusion: '#D400FF',
};
const LAYER_LABELS = {
  download: 'unduh', processing: 'proses', fusion: 'fusi',
};
function buildRingSVG(layers, size) {
  size = size || 96;
  const cx = size / 2, cy = size / 2;
  const n = Math.max(layers.length, 1);
  // Jari-jari dibagi rata supaya 1 sampai 7 cincin tetap mengisi kanvas.
  const outer = size / 2 - 4, inner = size * 0.1;
  const step = n > 1 ? (outer - inner) / (n - 1) : 0;
  const width = Math.max(2.5, Math.min(6, step * 0.7 || 6));
  let circles = '';
  layers.forEach((l, i) => {
    const r = n > 1 ? inner + step * i : outer * 0.6;
    const color = LAYER_COLORS[l.key] || '#35D0C0';
    const c = 2 * Math.PI * r;
    const dash = c * Math.max(0, Math.min(1, l.ratio || 0));
    const title = (SOURCE_LABELS[l.source] || l.source) + ' ' + (LAYER_LABELS[l.phase] || l.phase) + ': ' + Math.round((l.ratio || 0) * 100) + '%';
    circles += '<g><title>' + escapeHTML(title) + '</title>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + color + '" stroke-opacity="0.16" stroke-width="' + width + '"></circle>' +
      '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + color + '" stroke-width="' + width + '" stroke-linecap="round" stroke-dasharray="' + dash + ' ' + (c - dash) + '" transform="rotate(-90 ' + cx + ' ' + cy + ')"></circle></g>';
  });
  return '<svg viewBox="0 0 ' + size + ' ' + size + '" width="' + size + '" height="' + size + '">' + circles + '</svg>';
}
function ringLegendHTML(layers) {
  if (!layers.length) return '';
  return '<div class="ring-legend">' + layers.map(l =>
    '<span class="ring-legend-item"><i style="background:' + (LAYER_COLORS[l.key] || '#35D0C0') + '"></i>' +
    escapeHTML((SOURCE_SHORT[l.source] || SOURCE_LABELS[l.source] || l.source) + (l.phase === 'fusion' ? '' : ' ' + (LAYER_LABELS[l.phase] || l.phase))) +
    ' ' + Math.round((l.ratio || 0) * 100) + '%</span>'
  ).join('') + '</div>';
}

function switchTab(name) {
  document.querySelectorAll('.tab').forEach(b => b.classList.toggle('active', b.dataset.tab === name));
  document.querySelectorAll('.view').forEach(v => v.classList.toggle('hidden', v.id !== ('view-' + name)));
  document.getElementById('mapUI').classList.toggle('hidden', name !== 'create');
  document.getElementById('mapMask').classList.toggle('hidden', name !== 'create');
  document.body.classList.toggle('tab-create', name === 'create');
  if (name === 'create' && bgMap) requestAnimationFrame(() => { bgMap.invalidateSize(); updateMapMask(); positionMapHint(); if (selectedBBox) fitBBoxToGap(selectedBBox, false); });
  if (name === 'datasets') { loadDatasets(); startDatasetPolling(); } else { stopDatasetPolling(); }
  if (name === 'live') { loadLive(); startLivePolling(); } else { stopLivePolling(); }
}
document.querySelectorAll('.tab').forEach(btn => btn.addEventListener('click', () => switchTab(btn.dataset.tab)));
document.body.classList.add('tab-create');

const floatNav = document.getElementById('floatNav');
floatNav.querySelector('.floatnav-brand').addEventListener('click', () => floatNav.classList.toggle('expanded'));

async function checkHealth() {
  try {
    const h = await api('/api/health');
    setStatus(h.db_connected ? 'ok' : 'degraded', h.db_connected ? 'Terhubung' : 'Basis data bermasalah');
  } catch (e) {
    setStatus('down', 'Tidak terhubung');
  }
}
function setStatus(kind, label) {
  document.getElementById('statusDot').className = 'status-dot ' + kind;
  document.getElementById('statusLabel').textContent = label;
}
checkHealth();
setInterval(checkHealth, 15000);

const COLOR_TILE_URL = 'https://server.arcgisonline.com/ArcGIS/rest/services/World_Street_Map/MapServer/tile/{z}/{y}/{x}';
const COLOR_TILE_ATTR = 'Tiles &copy; Esri &mdash; Esri, HERE, Garmin, USGS, Intermap, NRCan, METI, OpenStreetMap contributors, GIS User Community';

let mapActivated = false;
let selectedBBox = null; // [minLon, minLat, maxLon, maxLat]
const DEFAULT_HINT = 'Pilih lokasi atau seret peta untuk mengubah area';

// The map is the whole app's background: #bgMap fills the viewport and is fully interactive
// (drag/zoom it and that IS the page background moving). The "peta wilayah" column in the
// create-dataset grid is just a transparent gap that reveals it; fitBounds/box math below
// account for that gap so the selection stays visible in it instead of hiding under the cards.
let bgMap = null;
function initBgMap() {
  bgMap = L.map('bgMap', {
    zoomControl: false,
    attributionControl: false,
    dragging: true,
    scrollWheelZoom: true,
    doubleClickZoom: true,
    boxZoom: false,
    keyboard: false,
    touchZoom: true,
    tap: false
  }).setView([-6.28, 106.85], 10);
  L.tileLayer(COLOR_TILE_URL, { maxZoom: 19, attribution: COLOR_TILE_ATTR }).addTo(bgMap);
  bgMap.on('move zoom', syncSelectionBoxFromBBox);
  const relayout = () => {
    bgMap.invalidateSize();
    updateMapMask();
    positionMapHint();
    if (selectedBBox) { fitBBoxToGap(selectedBBox, false); syncSelectionBoxFromBBox(); }
  };
  window.addEventListener('resize', relayout);
  window.addEventListener('scroll', () => { updateMapMask(); positionMapHint(); }, { passive: true });
  initMapZoomButtons();
  requestAnimationFrame(relayout);
}
initBgMap();

function activateMap() {
  mapActivated = true;
  document.getElementById('mapSelectionBox').classList.add('active');
}

function getMapGapRect() {
  const gap = document.getElementById('mapGap');
  if (!gap || gap.offsetWidth === 0) return null;
  return gap.getBoundingClientRect();
}

function positionMapHint() {
  const chip = document.getElementById('mapHintChip');
  const r = getMapGapRect();
  if (!chip || !r) return;
  chip.style.left = (r.left + r.width / 2) + 'px';
  chip.style.top = (r.top + 14) + 'px';
}

const MAP_RADIUS = 18;

// Penutup di luar jendela peta. Satu path SVG: persegi layar penuh, lalu subpath
// persegi panjang rounded searah sama -> dengan fill-rule evenodd bagian dalam
// jadi lubang. Hit-test SVG menghormati fill-rule, jadi path ini juga yang
// memblokir drag/zoom peta di luar jendela, dengan sudut membulat yang presisi.
function roundedRectPath(x, y, w, h, r) {
  r = Math.min(r, w / 2, h / 2);
  return 'M' + (x + r) + ' ' + y +
    'H' + (x + w - r) + 'A' + r + ' ' + r + ' 0 0 1 ' + (x + w) + ' ' + (y + r) +
    'V' + (y + h - r) + 'A' + r + ' ' + r + ' 0 0 1 ' + (x + w - r) + ' ' + (y + h) +
    'H' + (x + r) + 'A' + r + ' ' + r + ' 0 0 1 ' + x + ' ' + (y + h - r) +
    'V' + (y + r) + 'A' + r + ' ' + r + ' 0 0 1 ' + (x + r) + ' ' + y + 'Z';
}

function updateMapMask() {
  const mask = document.getElementById('mapMask');
  const path = document.getElementById('mapMaskPath');
  const clip = document.getElementById('mapClip');
  const r = getMapGapRect();
  if (!mask || !path) return;
  if (!r) { mask.classList.add('hidden'); return; }
  const W = window.innerWidth, H = window.innerHeight;
  mask.setAttribute('viewBox', '0 0 ' + W + ' ' + H);
  path.setAttribute('d',
    'M0 0H' + W + 'V' + H + 'H0Z ' + roundedRectPath(r.left, r.top, r.width, r.height, MAP_RADIUS));
  if (clip) {
    clip.style.left = r.left + 'px';
    clip.style.top = r.top + 'px';
    clip.style.width = r.width + 'px';
    clip.style.height = r.height + 'px';
  }
}

// Saat sebuah wilayah baru dipilih, kotaknya tidak boleh memenuhi jendela peta:
// batasnya 60% dari tinggi (dan lebar) jendela, agar konteks sekitarnya tetap terlihat.
// Pengguna tetap bebas memperbesar dengan drag/zoom setelahnya.
const MAX_SELECTION_FRAC = 0.6;

// fitBounds membuat bbox mengisi ~seluruh area yang tersisa, jadi yang di-fit adalah
// bbox yang sudah dimekarkan 1/0.6 kali terhadap titik tengahnya -- hasilnya bbox asli
// menempati ~60%. Pembulatan zoom Leaflet hanya bisa mengecilkan, jadi batas ini aman.
function inflateBBox(bbox, frac) {
  const cx = (bbox[0] + bbox[2]) / 2, cy = (bbox[1] + bbox[3]) / 2;
  const hw = (bbox[2] - bbox[0]) / 2 / frac, hh = (bbox[3] - bbox[1]) / 2 / frac;
  return [
    Math.max(-180, cx - hw), Math.max(-85, cy - hh),
    Math.min(180, cx + hw), Math.min(85, cy + hh)
  ];
}

function fitBBoxToGap(bbox, animate) {
  const fit = inflateBBox(bbox, MAX_SELECTION_FRAC);
  const minLon = fit[0], minLat = fit[1], maxLon = fit[2], maxLat = fit[3];
  const r = getMapGapRect();
  const opts = { animate: animate !== false };
  if (r) {
    opts.paddingTopLeft = [Math.max(20, r.left + 20), Math.max(20, r.top + 20)];
    opts.paddingBottomRight = [Math.max(20, window.innerWidth - r.right + 20), Math.max(20, window.innerHeight - r.bottom + 20)];
  } else {
    opts.padding = [24, 24];
  }
  bgMap.fitBounds([[minLat, minLon], [maxLat, maxLon]], opts);
}

function updateMapPreview(bbox) {
  activateMap();
  selectedBBox = bbox.slice();
  bgMap.invalidateSize();
  updateMapMask();
  positionMapHint();
  fitBBoxToGap(selectedBBox, true);
  syncSelectionBoxFromBBox();
  renderBBoxReadout();
  setTimeout(() => {
    bgMap.invalidateSize();
    fitBBoxToGap(selectedBBox, false);
    syncSelectionBoxFromBBox();
  }, 200);
}

function clearMapPreview() {
  selectedBBox = null;
  mapActivated = false;
  document.getElementById('mapBboxReadout').textContent = DEFAULT_HINT;
  const box = document.getElementById('mapSelectionBox');
  box.classList.remove('active');
  box.style.left = ''; box.style.top = ''; box.style.width = ''; box.style.height = '';
}

function syncSelectionBoxFromBBox() {
  if (!selectedBBox) return;
  const box = document.getElementById('mapSelectionBox');
  const minLon = selectedBBox[0], minLat = selectedBBox[1], maxLon = selectedBBox[2], maxLat = selectedBBox[3];
  const p1 = bgMap.latLngToContainerPoint([maxLat, minLon]);
  const p2 = bgMap.latLngToContainerPoint([minLat, maxLon]);
  // #mapClip diposisikan pada rect jendela peta, sedangkan titik Leaflet berada di
  // koordinat viewport (peta memenuhi layar) -- kurangi offset jendela agar kotak
  // tetap terkunci di lokasi geografisnya saat peta digeser.
  const gap = getMapGapRect();
  const ox = gap ? gap.left : 0, oy = gap ? gap.top : 0;
  box.style.left = (Math.min(p1.x, p2.x) - ox) + 'px';
  box.style.top = (Math.min(p1.y, p2.y) - oy) + 'px';
  box.style.width = Math.max(24, Math.abs(p2.x - p1.x)) + 'px';
  box.style.height = Math.max(24, Math.abs(p2.y - p1.y)) + 'px';
}

function renderBBoxReadout() {
  if (!selectedBBox) return;
  const [minLon, minLat, maxLon, maxLat] = selectedBBox;
  document.getElementById('mapBboxReadout').innerHTML =
    'Area: <span class="val">' + minLat.toFixed(3) + ', ' + minLon.toFixed(3) + '</span> &rarr; <span class="val">' + maxLat.toFixed(3) + ', ' + maxLon.toFixed(3) + '</span>';
}

function initMapZoomButtons() {
  document.getElementById('mapZoomIn').addEventListener('click', () => bgMap.zoomIn());
  document.getElementById('mapZoomOut').addEventListener('click', () => bgMap.zoomOut());
}

// ---------------------------------------------------------------------------
// Lokasi: daftar dari tabel regions_of_interest (bukan lagi config.json)
// ---------------------------------------------------------------------------
// State pemilihan lokasi cukup satu variabel: state.selectedRegionId. Nilainya
// yang dikirim ke POST /api/datasets sebagai region_id, jadi tidak ada lagi
// pencocokan nama yang bisa salah kalau ada dua lokasi dengan nama mirip.

function debounce(fn, wait) {
  let timer = null;
  return function () {
    const args = arguments;
    clearTimeout(timer);
    timer = setTimeout(() => fn.apply(null, args), wait);
  };
}

function fmtBBox(bbox) {
  if (!bbox || bbox.length !== 4) return '';
  return bbox[1].toFixed(3) + ', ' + bbox[0].toFixed(3) + ' → ' +
         bbox[3].toFixed(3) + ', ' + bbox[2].toFixed(3);
}

const SOURCE_LABEL = { SEEDER: 'sistem', USER: 'buatan sendiri', GEOCODE: 'hasil pencarian' };

// Filter lokal supaya ketikan langsung terasa (tanpa menunggu jaringan). Query
// yang sama juga dikirim ke server setelah debounce, untuk menjaring lokasi yang
// belum ikut terambil kalau daftarnya panjang (limit 200 per request).
function visibleRegions() {
  const q = state.locationQuery.trim().toLowerCase();
  if (!q) return state.regions;
  return state.regions.filter(r =>
    r.name.toLowerCase().includes(q) || (r.region_code || '').toLowerCase().includes(q));
}

function renderRegionCards() {
  const grid = document.getElementById('regionGrid');
  const rows = visibleRegions();
  if (rows.length === 0) {
    grid.innerHTML = '<div class="empty-small">' +
      (state.locationQuery.trim()
        ? 'Tidak ada lokasi cocok dengan "' + escapeHTML(state.locationQuery.trim()) + '"'
        : 'Belum ada lokasi. Tambahkan lewat tombol di atas.') +
      '</div>';
    return;
  }
  grid.innerHTML = rows.map(r =>
    '<div class="region-card' + (r.region_id === state.selectedRegionId ? ' selected' : '') + '"' +
         ' data-region-id="' + r.region_id + '" role="button" tabindex="0">' +
      '<div class="region-body">' +
        '<div class="region-name">' + escapeHTML(r.name) + '</div>' +
        '<div class="region-bbox">' + fmtBBox(r.bbox) + '</div>' +
        '<div class="region-meta">' +
          (r.area_km2 ? '<span class="region-area">' + r.area_km2.toFixed(1) + ' km&sup2;</span>' : '') +
          '<span class="src-badge src-' + escapeHTML((r.source || 'SEEDER').toLowerCase()) + '">' +
            escapeHTML(SOURCE_LABEL[r.source] || 'sistem') +
          '</span>' +
        '</div>' +
      '</div>' +
      (r.deletable
        ? '<button type="button" class="region-del" data-del-id="' + r.region_id + '"' +
          ' title="Hapus lokasi" aria-label="Hapus ' + escapeHTML(r.name) + '">' + ICONS.trash + '</button>'
        : '') +
    '</div>'
  ).join('');

  grid.querySelectorAll('.region-card').forEach(card => {
    const id = Number(card.dataset.regionId);
    card.addEventListener('click', (e) => {
      if (e.target.closest('.region-del')) return;
      selectRegion(id);
    });
    card.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); selectRegion(id); }
    });
  });
  grid.querySelectorAll('.region-del').forEach(btn => {
    btn.addEventListener('click', (e) => {
      e.stopPropagation();
      openDeleteLocationModal(Number(btn.dataset.delId));
    });
  });
}

function selectRegion(id) {
  state.selectedRegionId = id;
  document.querySelectorAll('.region-card').forEach(c =>
    c.classList.toggle('selected', Number(c.dataset.regionId) === id));
  const region = state.regions.find(r => r.region_id === id);
  if (region) updateMapPreview(region.bbox);
  updateWizardRegion();
}

function clearRegionSelection() {
  state.selectedRegionId = null;
  document.querySelectorAll('.region-card').forEach(c => c.classList.remove('selected'));
  clearMapPreview();
  updateWizardRegion();
}

async function loadRegions(options) {
  const opts = options || {};
  const grid = document.getElementById('regionGrid');
  try {
    const q = state.locationQuery.trim();
    const result = await api('/api/regions' + (q ? '?q=' + encodeURIComponent(q) : ''));
    state.regions = result.items;
    updateWizardRegion();
    // Lokasi terpilih bisa hilang dari hasil filter; itu tidak membatalkan pilihan,
    // hanya menyembunyikan kartunya sampai filter dikosongkan lagi.
    renderRegionCards();
    if (opts.highlightId) {
      const card = grid.querySelector('[data-region-id="' + opts.highlightId + '"]');
      if (card) {
        card.classList.add('just-added');
        card.scrollIntoView({ block: 'nearest', behavior: 'smooth' });
      }
    }
  } catch (e) {
    grid.innerHTML = '<div class="empty-small">Gagal memuat daftar lokasi</div>';
  }
}

const loadRegionsDebounced = debounce(() => loadRegions(), 260);

document.getElementById('locSearch').addEventListener('input', (e) => {
  state.locationQuery = e.target.value;
  renderRegionCards();      // instan, dari data yang sudah ada
  loadRegionsDebounced();   // menyusul, mencakup lokasi di luar 200 baris pertama
});

// ---------------------------------------------------------------------------
// Modal: tambah lokasi
// ---------------------------------------------------------------------------
const addLocModal = document.getElementById('addLocationModal');

function setLocTab(name) {
  document.querySelectorAll('.modal-tab').forEach(t =>
    t.classList.toggle('active', t.dataset.loctab === name));
  document.getElementById('loctabSearch').classList.toggle('hidden', name !== 'search');
  document.getElementById('loctabManual').classList.toggle('hidden', name !== 'manual');
}

function setAddLocError(msg) {
  const el = document.getElementById('alError');
  el.textContent = msg || '';
  el.classList.toggle('hidden', !msg);
}

function openAddLocationModal() {
  ['alName', 'alPaste', 'alMinLon', 'alMinLat', 'alMaxLon', 'alMaxLat', 'alDesc', 'geoSearchInput']
    .forEach(id => { document.getElementById(id).value = ''; });
  document.getElementById('geoResults').innerHTML =
    '<p class="geo-hint">Ketik minimal 2 huruf lalu pilih salah satu hasil.</p>';
  setAddLocError('');
  setLocTab('search');
  addLocModal.classList.remove('hidden');
  setTimeout(() => document.getElementById('geoSearchInput').focus(), 30);
}

function closeAddLocationModal() { addLocModal.classList.add('hidden'); }

document.getElementById('addLocationBtn').addEventListener('click', openAddLocationModal);
document.getElementById('alCancel').addEventListener('click', closeAddLocationModal);
addLocModal.addEventListener('click', (e) => { if (e.target === addLocModal) closeAddLocationModal(); });
document.querySelectorAll('.modal-tab').forEach(t =>
  t.addEventListener('click', () => setLocTab(t.dataset.loctab)));

// Bagian A: pencarian nama lewat proxy /api/regions/geocode (Nominatim).
// Debounce 400 ms karena Nominatim membatasi 1 request/detik per klien.
const runGeoSearch = debounce(async (query) => {
  const box = document.getElementById('geoResults');
  if (query.length < 2) {
    box.innerHTML = '<p class="geo-hint">Ketik minimal 2 huruf lalu pilih salah satu hasil.</p>';
    return;
  }
  box.innerHTML = '<p class="geo-hint">Mencari...</p>';
  try {
    const result = await api('/api/regions/geocode?q=' + encodeURIComponent(query));
    state.geoResults = result.items;
    if (!result.items.length) {
      box.innerHTML = '<p class="geo-hint">Tidak ada hasil untuk "' + escapeHTML(query) + '"</p>';
      return;
    }
    box.innerHTML = result.items.map((it, i) =>
      '<button type="button" class="geo-item" data-geo-index="' + i + '">' +
        '<span class="geo-item-icon">' + ICONS.pin + '</span>' +
        '<span class="geo-item-text">' +
          '<span class="geo-item-name">' + escapeHTML(it.name) + '</span>' +
          '<span class="geo-item-sub">' + escapeHTML(it.display_name) + '</span>' +
          '<span class="geo-item-bbox">' + fmtBBox(it.bbox) + '</span>' +
        '</span>' +
      '</button>'
    ).join('');
    box.querySelectorAll('.geo-item').forEach(btn => {
      btn.addEventListener('click', () => useGeoResult(state.geoResults[Number(btn.dataset.geoIndex)]));
    });
  } catch (err) {
    box.innerHTML = '<p class="geo-hint error">' + escapeHTML(err.message) + '</p>';
  }
}, 400);

document.getElementById('geoSearchInput').addEventListener('input', (e) => {
  runGeoSearch(e.target.value.trim());
});

// Hasil pencarian tidak langsung disimpan: isi form koordinat lalu pindah ke tab
// manual, supaya pengguna melihat bbox persisnya sebelum menekan Simpan.
function useGeoResult(item) {
  if (!item) return;
  document.getElementById('alName').value = item.name;
  document.getElementById('alMinLon').value = item.bbox[0];
  document.getElementById('alMinLat').value = item.bbox[1];
  document.getElementById('alMaxLon').value = item.bbox[2];
  document.getElementById('alMaxLat').value = item.bbox[3];
  document.getElementById('alDesc').value = item.display_name;
  setAddLocError('');
  setLocTab('manual');
}

// Tempel "min_lon, min_lat, max_lon, max_lat" (urutan bbox GDAL/GeoJSON).
document.getElementById('alPaste').addEventListener('input', (e) => {
  const nums = (e.target.value.match(/-?\d+(\.\d+)?/g) || []).map(Number);
  if (nums.length !== 4) return;
  document.getElementById('alMinLon').value = nums[0];
  document.getElementById('alMinLat').value = nums[1];
  document.getElementById('alMaxLon').value = nums[2];
  document.getElementById('alMaxLat').value = nums[3];
  setAddLocError('');
});

// Validasi di sini hanya untuk umpan balik cepat; API tetap yang menentukan,
// aturannya ada di etl/geo_utils.validate_bbox.
function readAddLocationForm() {
  const num = (id) => {
    const raw = document.getElementById(id).value.trim();
    return raw === '' ? NaN : Number(raw);
  };
  const name = document.getElementById('alName').value.trim();
  const b = { min_lon: num('alMinLon'), min_lat: num('alMinLat'), max_lon: num('alMaxLon'), max_lat: num('alMaxLat') };
  if (!name) return { error: 'Nama lokasi belum diisi' };
  if ([b.min_lon, b.min_lat, b.max_lon, b.max_lat].some(v => !isFinite(v))) {
    return { error: 'Keempat koordinat harus diisi dengan angka' };
  }
  if (b.min_lon < -180 || b.max_lon > 180) return { error: 'Longitude harus di rentang -180 sampai 180' };
  if (b.min_lat < -90 || b.max_lat > 90) return { error: 'Latitude harus di rentang -90 sampai 90' };
  if (b.min_lon >= b.max_lon) return { error: 'Longitude minimum harus lebih kecil dari longitude maksimum' };
  if (b.min_lat >= b.max_lat) return { error: 'Latitude minimum harus lebih kecil dari latitude maksimum' };
  return { body: Object.assign({ name: name, description: document.getElementById('alDesc').value.trim() || null }, b) };
}

document.getElementById('alSave').addEventListener('click', async () => {
  const parsed = readAddLocationForm();
  if (parsed.error) { setAddLocError(parsed.error); setLocTab('manual'); return; }
  const btn = document.getElementById('alSave');
  btn.disabled = true; btn.textContent = 'Menyimpan...';
  try {
    const created = await api('/api/regions', { method: 'POST', body: JSON.stringify(parsed.body) });
    closeAddLocationModal();
    // Kosongkan filter supaya lokasi yang baru dibuat pasti terlihat.
    state.locationQuery = '';
    document.getElementById('locSearch').value = '';
    await loadRegions({ highlightId: created.region_id });
    selectRegion(created.region_id);
    showToast('Lokasi "' + created.name + '" ditambahkan', 'success');
    // Dibuka dari form Tambah Daerah Live: kembali ke form itu dengan lokasi
    // baru terpilih.
    if (state.lmReopenAdd) { state.lmReopenAdd = false; openLmAddModal(created.region_id); }
  } catch (err) {
    setAddLocError(err.message);
    setLocTab('manual');
  } finally {
    btn.disabled = false; btn.textContent = 'Simpan Lokasi';
  }
});

// ---------------------------------------------------------------------------
// Modal: hapus lokasi (soft-delete)
// ---------------------------------------------------------------------------
const delLocModal = document.getElementById('deleteLocationModal');

function openDeleteLocationModal(id) {
  const region = state.regions.find(r => r.region_id === id);
  if (!region) return;
  state.pendingDeleteRegionId = id;
  document.getElementById('deleteLocationText').innerHTML =
    'Lokasi <strong>' + escapeHTML(region.name) + '</strong> akan dihilangkan dari daftar pilihan.';
  delLocModal.classList.remove('hidden');
}

function closeDeleteLocationModal() {
  state.pendingDeleteRegionId = null;
  delLocModal.classList.add('hidden');
}

document.getElementById('deleteLocationCancel').addEventListener('click', closeDeleteLocationModal);
delLocModal.addEventListener('click', (e) => { if (e.target === delLocModal) closeDeleteLocationModal(); });

document.getElementById('deleteLocationConfirm').addEventListener('click', async () => {
  const id = state.pendingDeleteRegionId;
  if (!id) return;
  const btn = document.getElementById('deleteLocationConfirm');
  btn.disabled = true; btn.textContent = 'Menghapus...';
  try {
    const result = await api('/api/regions/' + id, { method: 'DELETE' });
    closeDeleteLocationModal();
    const card = document.querySelector('.region-card[data-region-id="' + id + '"]');
    if (card) {
      card.classList.add('removing');
      await new Promise(r => setTimeout(r, 180));
    }
    if (state.selectedRegionId === id) clearRegionSelection();
    await loadRegions();
    showToast(result.message, 'success');
  } catch (err) {
    showToast(err.message, 'error');
    closeDeleteLocationModal();
  } finally {
    btn.disabled = false; btn.textContent = 'Ya, Hapus';
  }
});

document.addEventListener('keydown', (e) => {
  if (e.key !== 'Escape') return;
  if (!addLocModal.classList.contains('hidden')) closeAddLocationModal();
  if (!delLocModal.classList.contains('hidden')) closeDeleteLocationModal();
});

document.getElementById('addLocationIcon').innerHTML = ICONS.plus;
document.getElementById('locSearchIcon').innerHTML = ICONS.search;
document.getElementById('cloneConfigIcon').innerHTML = ICONS.gear;
loadRegions();

/* ===========================================================================
   Wisaya "Buat Dataset" (4 langkah) -- DOCS/INTERFACE.md, "User Journey".

   Sumber dan level pemrosesan dipilih PER satelit: satu kartu per sensor
   dengan kotak centang RAW/PROCESSED-nya sendiri. Model lama (satu daftar tier
   global) dihapus karena arti RAW/PROCESSED berbeda tiap sensor
   (DOCS/PIPELINE.md) -- "RAW" untuk GPM adalah curah hujan harian, untuk S1 adalah
   citra terkalibrasi -- jadi satu sakelar global tidak pernah bisa berarti
   hal yang sama untuk ketiganya.
   =========================================================================== */

const SATELLITE_SOURCES = [
  {
    key: 'sentinel1',
    label: 'Sentinel-1 SAR (ESA)',
    desc: 'radar, tembus awan, ~10 m, revisit ~7-8 hari',
    processing: [
      { value: 'RAW', desc: 'kalibrasi + crop (tanpa Lee filter, tanpa QA)' },
      { value: 'PROCESSED', desc: '+ Lee filter 7x7 + QA analytics + COG' },
    ],
  },
  {
    key: 'modis',
    label: 'MODIS Optical (NASA)',
    desc: 'banjir/vegetasi, 250 m, harian',
    processing: [
      { value: 'RAW', desc: 'peta banjir saja (tanpa indeks turunan)' },
      { value: 'PROCESSED', desc: '+ hitung NDVI + NDWI dari reflectance' },
    ],
  },
  {
    key: 'gpm',
    label: 'GPM IMERG Rainfall (NASA/JAXA)',
    desc: 'curah hujan, ~10 km, harian',
    processing: [
      { value: 'RAW', desc: 'curah hujan harian (hari itu saja)' },
      { value: 'PROCESSED', desc: '+ akumulasi 24 jam / 72 jam / 7 hari' },
    ],
  },
];

const FUSION_STRATEGIES = [
  { value: 'CO_OCCURRENCE', label: 'CO-OCCURRENCE', desc: 'hanya tanggal yang semua sumber punya data' },
  { value: 'FULL_COVERAGE', label: 'FULL COVERAGE', desc: 'setiap hari; MODIS/GPM diunduh harian, unduhan jauh lebih besar' },
  { value: 'HYBRID', label: 'HYBRID', desc: 'unduh harian, rakit per tanggal Sentinel-1' },
];

const PREVIEW_OPTION_DEFS = [
  { value: 'GRAYSCALE', desc: 'peregangan persentil 2-98' },
  { value: 'COLORED', desc: 'colormap per sumber' },
  { value: 'COMPOSITE', desc: 'false color RGB (khusus Sentinel-1)' },
];

const SOURCE_SHORT = { sentinel1: 'S1', modis: 'MODIS', gpm: 'GPM' };

// Cache klon config: dipakai HANYA untuk memutuskan tombol "Pakai Config
// Sebelumnya" boleh tampil sebelum jaringan menjawab. Nilai yang benar-benar
// diterapkan selalu diambil ulang dari API saat diklik (DOCS/DECISIONS.md D13:
// database yang jadi sumber kebenaran, localStorage cuma penghapus kedipan).
const LAST_CONFIG_KEY = 'trinity.lastDatasetConfig';
const WIZARD_LAST_STEP = 4;
let wizardStep = 1;

function $id(id) { return document.getElementById(id); }

/* ---- Langkah 2: render kartu satelit ------------------------------------ */

function renderSatelliteCards() {
  $id('satelliteList').innerHTML = SATELLITE_SOURCES.map(s =>
    '<div class="satellite-card is-off" data-source="' + s.key + '">' +
      '<label class="sat-head">' +
        '<input type="checkbox" class="sat-enable" data-source="' + s.key + '">' +
        '<span class="option-text">' +
          '<span class="option-label">' + escapeHTML(s.label) + '</span>' +
          '<span class="option-desc">' + escapeHTML(s.desc) + '</span>' +
        '</span>' +
      '</label>' +
      '<div class="sat-body">' +
        '<div class="sat-body-head">' +
          '<span class="mini-label">Tingkat Pemrosesan</span>' +
          '<label class="mini-toggle">' +
            '<input type="checkbox" class="sat-all" data-source="' + s.key + '">Semua' +
          '</label>' +
        '</div>' +
        s.processing.map(p =>
          '<label class="processing-option">' +
            '<input type="checkbox" class="proc-check" data-source="' + s.key + '" value="' + p.value + '">' +
            '<span class="option-text">' +
              '<span class="option-label">' + p.value + '</span>' +
              '<span class="option-desc">' + escapeHTML(p.desc) + '</span>' +
            '</span>' +
          '</label>').join('') +
      '</div>' +
    '</div>').join('');
}

function renderFusionOptions() {
  $id('fusionList').innerHTML = FUSION_STRATEGIES.map(f =>
    '<label class="option-row">' +
      '<input type="radio" name="fusionStrategy" value="' + f.value + '">' +
      '<span class="option-text">' +
        '<span class="option-label">' + f.label + '</span>' +
        '<span class="option-desc">' + escapeHTML(f.desc) + '</span>' +
      '</span>' +
    '</label>').join('');
}

function renderPreviewOptions() {
  $id('previewOptions').innerHTML = PREVIEW_OPTION_DEFS.map(p =>
    '<label class="option-row">' +
      '<input type="checkbox" class="preview-check" value="' + p.value + '">' +
      '<span class="option-text">' +
        '<span class="option-label">' + p.value + '</span>' +
        '<span class="option-desc">' + escapeHTML(p.desc) + '</span>' +
      '</span>' +
    '</label>').join('');
}

/* ---- Langkah 2: pembacaan & sinkronisasi status -------------------------- */

function sourceEnableBox(src) {
  return document.querySelector('.sat-enable[data-source="' + src + '"]');
}

function sourceProcBoxes(src) {
  return Array.from(document.querySelectorAll('.proc-check[data-source="' + src + '"]'));
}

function checkedProcessing(src) {
  return sourceProcBoxes(src).filter(c => c.checked).map(c => c.value);
}

function enabledSourceKeys() {
  return SATELLITE_SOURCES.map(s => s.key).filter(k => sourceEnableBox(k).checked);
}

// Bentuk payload API: {"sentinel1": {"processing": ["RAW","PROCESSED"]}, ...}.
// Sumber yang tidak diaktifkan sengaja TIDAK dikirim sebagai key kosong --
// DOCS/INTERFACE.md: key yang hilang berarti "tidak diingest", sedangkan key dengan
// processing kosong ditolak backend.
function collectSources() {
  const out = {};
  enabledSourceKeys().forEach(k => {
    const levels = checkedProcessing(k);
    if (levels.length) out[k] = { processing: levels };
  });
  return out;
}

function selectedFusionStrategy() {
  const picked = document.querySelector('input[name="fusionStrategy"]:checked');
  return picked ? picked.value : null;
}

function selectedPreviewOptions() {
  return Array.from(document.querySelectorAll('.preview-check:checked')).map(c => c.value);
}

function setSourceState(src, enabled, levels) {
  sourceEnableBox(src).checked = enabled;
  sourceProcBoxes(src).forEach(c => { c.checked = enabled && levels.indexOf(c.value) !== -1; });
}

// Satu-satunya tempat yang menulis status turunan langkah 2: kartu mati/hidup,
// kotak "Semua" (termasuk status indeterminate), master toggle, lalu bagian
// fusi dan ringkasan yang ikut bergantung pada jumlah sumber.
function syncWizardSources() {
  let allOn = true;
  SATELLITE_SOURCES.forEach(s => {
    const on = sourceEnableBox(s.key).checked;
    const boxes = sourceProcBoxes(s.key);
    const checked = boxes.filter(c => c.checked).length;
    const card = document.querySelector('.satellite-card[data-source="' + s.key + '"]');
    card.classList.toggle('is-off', !on);
    // Input-nya dinonaktifkan, bukan cuma disamarkan: kartu yang mati tidak
    // boleh masih bisa dicentang lewat Tab walau tampak abu-abu.
    const allBox = card.querySelector('.sat-all');
    boxes.concat([allBox]).forEach(c => { c.disabled = !on; });
    allBox.checked = on && checked === boxes.length;
    allBox.indeterminate = on && checked > 0 && checked < boxes.length;
    if (!on || checked < boxes.length) allOn = false;
  });
  const master = $id('masterAll');
  master.checked = allOn;
  master.indeterminate = !allOn && enabledSourceKeys().length > 0;
  syncFusionVisibility();
  renderWizardReview();
}

// Strategi fusi wajib kalau >1 sumber dan harus null kalau cuma 1
// (DOCS/INTERFACE.md, "Validation"). Pilihan yang terlanjur dibuat dikosongkan saat
// bagiannya disembunyikan, supaya tidak ada nilai tak terlihat yang ikut
// terkirim.
function syncFusionVisibility() {
  const multi = enabledSourceKeys().length > 1;
  $id('fusionGroup').classList.toggle('hidden', !multi);
  if (!multi) {
    document.querySelectorAll('input[name="fusionStrategy"]').forEach(r => { r.checked = false; });
    $id('fusionOutputOnly').checked = false;
  }
  syncToleranceVisibility();
}

// Toleransi hanya dipakai FULL_COVERAGE. CO_OCCURRENCE dan HYBRID berjangkar
// pada scene Sentinel-1, jadi tidak pernah perlu meminjam dari hari lain --
// menampilkan kolomnya di sana akan menyiratkan pengaruh yang tidak ada.
function syncToleranceVisibility() {
  $id('toleranceGroup').classList.toggle(
    'hidden', selectedFusionStrategy() !== 'FULL_COVERAGE'
  );
}

function selectedTolerance() {
  const raw = parseInt($id('s1Tolerance').value, 10);
  return Number.isFinite(raw) ? Math.min(14, Math.max(0, raw)) : 2;
}

/* ---- Navigasi wisaya ---------------------------------------------------- */

function setWizardError(msg) {
  const box = $id('wizardError');
  box.textContent = msg || '';
  box.classList.toggle('hidden', !msg);
}

function validateWizardStep(step) {
  if (step === 1) {
    if (!state.selectedRegionId) return 'Pilih dulu lokasi dari daftar di panel kiri';
    const start = $id('fDateStart').value;
    const end = $id('fDateEnd').value;
    if (!start || !end) return 'Lengkapi rentang tanggal';
    if (start > end) return 'Tanggal awal harus lebih dulu dari tanggal akhir';
    return null;
  }
  if (step === 2) {
    const enabled = enabledSourceKeys();
    if (enabled.length === 0) return 'Pilih minimal satu sumber satelit';
    const kosong = enabled.filter(k => checkedProcessing(k).length === 0);
    if (kosong.length) {
      const def = SATELLITE_SOURCES.find(s => s.key === kosong[0]);
      return 'Pilih minimal satu tingkat pemrosesan untuk ' + def.label;
    }
    return null;
  }
  if (step === 3) {
    if (enabledSourceKeys().length > 1 && !selectedFusionStrategy()) {
      return 'Pilih strategi fusi (wajib kalau sumbernya lebih dari satu)';
    }
    return null;
  }
  if (step === 4) {
    if (!$id('fName').value.trim()) return 'Isi nama dataset';
    return null;
  }
  return null;
}

function showWizardStep(step) {
  wizardStep = step;
  document.querySelectorAll('.wizard-panel').forEach(p => {
    p.classList.toggle('hidden', Number(p.dataset.step) !== step);
  });
  document.querySelectorAll('.wizard-step-pip').forEach(pip => {
    const n = Number(pip.dataset.pip);
    pip.classList.toggle('active', n === step);
    pip.classList.toggle('done', n < step);
  });
  $id('wizardBack').classList.toggle('hidden', step === 1);
  $id('wizardNext').classList.toggle('hidden', step === WIZARD_LAST_STEP);
  $id('createSubmit').classList.toggle('hidden', step !== WIZARD_LAST_STEP);
  setWizardError('');
  if (step === WIZARD_LAST_STEP) renderWizardReview();
}

// Maju hanya lewat langkah yang sudah valid; mundur selalu boleh. Pip di header
// memakai jalur yang sama, jadi melompat ke depan tidak bisa melewati validasi.
function goToWizardStep(target) {
  if (target > wizardStep) {
    for (let s = wizardStep; s < target; s++) {
      const err = validateWizardStep(s);
      if (err) { showWizardStep(s); setWizardError(err); return false; }
    }
  }
  showWizardStep(Math.min(Math.max(target, 1), WIZARD_LAST_STEP));
  return true;
}

function describeSourceSelection(sources) {
  const keys = Object.keys(sources);
  if (keys.length === 0) return 'belum ada sumber dipilih';
  return keys.map(k => {
    const levels = (sources[k].processing || []).map(l => (l === 'PROCESSED' ? 'PROC' : l));
    return (SOURCE_SHORT[k] || k) + '[' + (levels.join('+') || '-') + ']';
  }).join(' | ');
}

function renderWizardReview() {
  const box = $id('wizardReview');
  if (!box) return;
  const region = state.regions.find(r => r.region_id === state.selectedRegionId);
  const previews = selectedPreviewOptions();
  const multi = enabledSourceKeys().length > 1;
  const rows = [
    ['Lokasi', region ? region.name : 'belum dipilih'],
    ['Tanggal', ($id('fDateStart').value || '-') + ' s/d ' + ($id('fDateEnd').value || '-')],
    ['Sumber', describeSourceSelection(collectSources())],
    ['Strategi fusi', selectedFusionStrategy() || (multi ? 'belum dipilih' : 'tidak dipakai (1 sumber)')],
    ...(selectedFusionStrategy() === 'FULL_COVERAGE'
      ? [['Toleransi S1', selectedTolerance() + ' hari']] : []),
    ...(multi && $id('fusionOutputOnly').checked
      ? [['Penyimpanan', 'hasil fusi saja (berkas per-satelit dihapus)']] : []),
    ['Preview', previews.length ? previews.join(', ') : 'tidak dibuat'],
  ];
  box.innerHTML = rows.map(r =>
    '<div class="review-row"><span>' + r[0] + '</span><span>' + escapeHTML(String(r[1])) + '</span></div>'
  ).join('');
}

// Wilayah dipilih di panel kiri, jadi langkah 1 hanya menampilkan hasilnya.
// Dipanggil ulang oleh selectRegion/clearRegionSelection supaya pembacaan dan
// ringkasan tidak pernah tertinggal dari kartu lokasi yang aktif.
function updateWizardRegion() {
  const box = $id('wizardRegion');
  if (!box) return;
  const region = state.regions.find(r => r.region_id === state.selectedRegionId);
  box.classList.toggle('empty', !region);
  box.textContent = region
    ? region.name + ' -- ' + fmtBBox(region.bbox)
    : 'Belum ada lokasi dipilih -- pilih dari panel kiri';
  renderWizardReview();
}

/* ---- Pra-wisaya: "Pakai Config Sebelumnya" ------------------------------- */

function setCloneError(msg) {
  const box = $id('cloneError');
  box.textContent = msg || '';
  box.classList.toggle('hidden', !msg);
}

function renderClonePreview(cfg) {
  const box = $id('clonePreview');
  if (!cfg) { box.classList.add('hidden'); box.innerHTML = ''; return; }
  const dateLine = (cfg.date_start && cfg.date_end)
    ? '<div class="clone-line">Tanggal: ' + escapeHTML(cfg.date_start) + ' s/d ' + escapeHTML(cfg.date_end) + '</div>'
    : '';
  box.innerHTML =
    '<p class="clone-preview-title">Konfigurasi Terakhir</p>' +
    '<div class="clone-line">' + escapeHTML(cfg.region_name || 'lokasi tidak diketahui') + '</div>' +
    dateLine +
    '<div class="clone-line">' + escapeHTML(describeSourceSelection(cfg.sources || {})) + '</div>' +
    '<div class="clone-line">Strategi: ' + escapeHTML(cfg.fusion_strategy || 'tidak dipakai') + '</div>';
  box.classList.remove('hidden');
}

function cacheLastConfig(cfg) {
  try { localStorage.setItem(LAST_CONFIG_KEY, JSON.stringify(cfg)); } catch (e) {}
}

// Dipanggil saat halaman dimuat dan sesudah dataset baru dibuat. 404 berarti
// user memang belum punya dataset -> tombol disembunyikan dan cache dibuang.
// Kegagalan lain (jaringan/500) TIDAK menyembunyikan tombol: kalau cache bilang
// pernah ada config, tombol tetap ada dan errornya baru muncul saat diklik.
async function refreshCloneAvailability() {
  const block = $id('cloneBlock');
  let cached = null;
  try { cached = JSON.parse(localStorage.getItem(LAST_CONFIG_KEY) || 'null'); } catch (e) { cached = null; }
  if (cached) { block.classList.remove('hidden'); renderClonePreview(cached); }
  try {
    const res = await fetch('/api/datasets/last-config');
    if (res.status === 404) {
      block.classList.add('hidden');
      renderClonePreview(null);
      try { localStorage.removeItem(LAST_CONFIG_KEY); } catch (e) {}
      return;
    }
    if (!res.ok) return;
    const cfg = await res.json();
    cacheLastConfig(cfg);
    block.classList.remove('hidden');
    renderClonePreview(cfg);
  } catch (e) {
    // Offline: biarkan apa adanya. Tombolnya menangani errornya sendiri saat
    // diklik, dan menyembunyikannya di sini justru menghilangkan jalan pintas
    // hanya karena satu request gagal.
  }
}

// Klon adalah preset, bukan kunci: semua field tetap bisa diedit sesudahnya,
// termasuk tanggal (diisi ulang dari config terakhir supaya user tidak perlu
// mengetik ulang). Nama sengaja TIDAK ikut supaya dataset hasil klon tidak
// diam-diam menduplikasi yang lama (DOCS/DECISIONS.md D13).
function applyLastConfig(cfg) {
  if (cfg.date_start) $id('fDateStart').value = cfg.date_start;
  if (cfg.date_end) $id('fDateEnd').value = cfg.date_end;
  SATELLITE_SOURCES.forEach(s => setSourceState(s.key, false, []));
  Object.keys(cfg.sources || {}).forEach(k => {
    if (sourceEnableBox(k)) setSourceState(k, true, cfg.sources[k].processing || []);
  });
  syncWizardSources();
  if (cfg.fusion_strategy && !$id('fusionGroup').classList.contains('hidden')) {
    const radio = document.querySelector('input[name="fusionStrategy"][value="' + cfg.fusion_strategy + '"]');
    if (radio) radio.checked = true;
  }
  const previews = cfg.preview_options || [];
  document.querySelectorAll('.preview-check').forEach(c => { c.checked = previews.indexOf(c.value) !== -1; });

  let regionMissing = false;
  if (cfg.region_id && state.regions.some(r => r.region_id === cfg.region_id)) {
    selectRegion(cfg.region_id);
  } else if (cfg.region_id || cfg.region_name) {
    regionMissing = true;
  }
  showWizardStep(1);
  renderWizardReview();
  $id('fDateStart').focus();
  return regionMissing;
}

/* ---- Pemasangan listener ------------------------------------------------ */

renderSatelliteCards();
renderFusionOptions();
renderPreviewOptions();

$id('satelliteList').addEventListener('change', (e) => {
  const target = e.target;
  const src = target.dataset.source;
  if (target.classList.contains('sat-enable')) {
    // Mengaktifkan sumber tanpa level apa pun akan langsung gagal validasi,
    // jadi PROCESSED (keluaran siap analisis) dipasang sebagai default; user
    // tinggal menguranginya. Level yang sudah dipilih sebelumnya dipertahankan.
    if (target.checked && checkedProcessing(src).length === 0) {
      sourceProcBoxes(src).forEach(c => { c.checked = c.value === 'PROCESSED'; });
    }
  } else if (target.classList.contains('sat-all')) {
    sourceProcBoxes(src).forEach(c => { c.checked = target.checked; });
  } else if (target.classList.contains('proc-check') && target.checked) {
    sourceEnableBox(src).checked = true;
  }
  syncWizardSources();
  setWizardError('');
});

$id('masterAll').addEventListener('change', (e) => {
  const on = e.target.checked;
  SATELLITE_SOURCES.forEach(s => {
    setSourceState(s.key, on, on ? s.processing.map(p => p.value) : []);
  });
  syncWizardSources();
  setWizardError('');
});

$id('previewOptions').addEventListener('change', renderWizardReview);
$id('fusionList').addEventListener('change', () => {
  syncToleranceVisibility();
  renderWizardReview();
  setWizardError('');
});
$id('fusionOutputOnly').addEventListener('change', renderWizardReview);
$id('s1Tolerance').addEventListener('change', renderWizardReview);
$id('fDateStart').addEventListener('change', renderWizardReview);
$id('fDateEnd').addEventListener('change', renderWizardReview);

$id('wizardNext').addEventListener('click', () => {
  const err = validateWizardStep(wizardStep);
  if (err) { setWizardError(err); return; }
  showWizardStep(Math.min(wizardStep + 1, WIZARD_LAST_STEP));
});
$id('wizardBack').addEventListener('click', () => showWizardStep(Math.max(wizardStep - 1, 1)));
document.querySelectorAll('.wizard-step-pip').forEach(pip => {
  pip.addEventListener('click', () => goToWizardStep(Number(pip.dataset.pip)));
});

$id('cloneConfigBtn').addEventListener('click', async (e) => {
  const btn = e.currentTarget;
  const markup = btn.innerHTML;
  btn.disabled = true;
  btn.textContent = 'Memuat config...';
  setCloneError('');
  try {
    const res = await fetch('/api/datasets/last-config');
    if (!res.ok) {
      throw new Error(res.status === 404
        ? 'Belum ada dataset sebelumnya untuk disalin'
        : 'Gagal mengambil config terakhir (' + res.status + ')');
    }
    const cfg = await res.json();
    cacheLastConfig(cfg);
    renderClonePreview(cfg);
    const regionMissing = applyLastConfig(cfg);
    if (regionMissing) {
      setCloneError('Lokasi "' + (cfg.region_name || cfg.region_id) +
        '" tidak ada lagi di daftar -- pilih lokasi lain.');
    }
    showToast('Config terakhir dipakai. Isi tanggal dan nama dataset.', 'success');
  } catch (err) {
    // Tombolnya sengaja tetap aktif: klon itu jalan pintas, kegagalannya tidak
    // boleh ikut memblokir pembuatan dataset secara manual.
    setCloneError(err.message);
  } finally {
    btn.disabled = false;
    btn.innerHTML = markup;
  }
});

function resetWizard() {
  $id('createForm').reset();
  // form.reset() mengembalikan kotak centang ke atribut `checked` di markup,
  // tapi kartu satelit dirender JS tanpa atribut itu -- statusnya ditulis ulang
  // di sini supaya class is-off dan master toggle ikut kembali ke posisi awal.
  SATELLITE_SOURCES.forEach(s => setSourceState(s.key, false, []));
  document.querySelectorAll('.preview-check').forEach(c => { c.checked = false; });
  syncWizardSources();
  clearRegionSelection();
  updateWizardRegion();
  showWizardStep(1);
}

$id('createForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  for (let s = 1; s <= WIZARD_LAST_STEP; s++) {
    const err = validateWizardStep(s);
    if (err) { showWizardStep(s); setWizardError(err); showToast(err, 'error'); return; }
  }
  const qs = {};
  const cloud = $id('fMinCloud').value;
  const qual = $id('fMinQuality').value;
  const resolution = $id('fResolution').value;
  if (cloud !== '') qs.min_cloud_cover = Number(cloud);
  if (qual !== '') qs.min_quality_score = Number(qual);
  if (resolution !== '') qs.resolution_m = Number(resolution);
  const previewOptions = selectedPreviewOptions();
  const body = {
    region_id: state.selectedRegionId,
    date_start: $id('fDateStart').value,
    date_end: $id('fDateEnd').value,
    name: $id('fName').value.trim(),
    description: $id('fDescription').value.trim() || null,
    // Menggantikan tier global + satu processing level: satu objek sumber ->
    // level pemrosesan (DOCS/INTERFACE.md, "Create Dataset"). `tiers` diturunkan
    // backend dari sini, jadi tidak lagi dikirim frontend.
    sources: collectSources(),
    fusion_strategy: enabledSourceKeys().length > 1 ? selectedFusionStrategy() : null,
    // Keduanya hanya bermakna kalau ada fusi: backend menolak
    // fusion_output_only tanpa strategi, karena tanpa stack HDF5 menghapus
    // artefak per-satelit tidak menyisakan output apa pun.
    fusion_output_only: enabledSourceKeys().length > 1 && $id('fusionOutputOnly').checked,
    s1_match_tolerance_days: selectedFusionStrategy() === 'FULL_COVERAGE'
      ? selectedTolerance() : null,
    preview_options: previewOptions,
    quality_settings: Object.keys(qs).length ? qs : null,
    // Sakelar tahap pipeline, bukan ambang mutu data: tanpa satu pun opsi
    // preview yang dipilih, tahap PREVIEW tidak perlu dijalankan sama sekali.
    generate_preview: previewOptions.length > 0,
  };
  const submitBtn = $id('createSubmit');
  submitBtn.disabled = true; submitBtn.textContent = 'Membuat...';
  try {
    const result = await api('/api/datasets', { method: 'POST', body: JSON.stringify(body) });
    showToast('Dataset dibuat (status: ' + result.status + ')', 'success');
    resetWizard();
    refreshCloneAvailability();
    switchTab('datasets');
  } catch (err) {
    setWizardError(err.message);
    showToast(err.message, 'error');
  } finally {
    submitBtn.disabled = false; submitBtn.textContent = 'Buat Dataset';
  }
});

syncWizardSources();
showWizardStep(1);
updateWizardRegion();
refreshCloneAvailability();

async function loadDatasets() {
  try {
    const result = await api('/api/datasets?limit=50');
    state.datasets = result.items;
    await refreshProgress();
  } catch (err) { showToast(err.message, 'error'); }
  // Sengaja HANYA di sini, bukan di refreshProgress: /api/merge/candidates
  // membuka atribut setiap berkas HDF5 di disk, jadi terlalu mahal untuk ikut
  // polling 10 detik. Pemuatan eksplisit (buka tab, tekan Segarkan, selesai
  // menggabungkan) sudah cukup -- daftar kandidat berubah hanya saat ada
  // stack fusion baru, yang butuh menit sampai jam.
  renderMergePanel();
}
async function loadDatasetsQuiet() {
  try {
    const result = await api('/api/datasets?limit=50');
    state.datasets = result.items;
  } catch (e) {}
}
async function refreshProgress() {
  for (const ds of state.datasets) {
    if (ACTIVE_STATUSES.has(ds.status) || !state.progress[ds.dataset_id]) {
      try { state.progress[ds.dataset_id] = await api('/api/datasets/' + ds.dataset_id + '/status'); }
      catch (e) {}
    }
    if (ACTIVE_STATUSES.has(ds.status) || !state.logs[ds.dataset_id]) {
      try { state.logs[ds.dataset_id] = (await api('/api/datasets/' + ds.dataset_id + '/logs?limit=5')).logs; }
      catch (e) {}
    }
    // Log pertama cukup diambil sekali untuk hitung durasi.
    if (!state.firstLogAt[ds.dataset_id]) {
      try {
        const first = (await api('/api/datasets/' + ds.dataset_id + '/logs?limit=1&order=asc')).logs[0];
        if (first) state.firstLogAt[ds.dataset_id] = first.timestamp;
      } catch (e) {}
    }
  }
  renderDatasets();
}
function startDatasetPolling() {
  stopDatasetPolling();
  state.pollTimer = setInterval(async () => { await loadDatasetsQuiet(); await refreshProgress(); }, 10000);
}
function stopDatasetPolling() { if (state.pollTimer) clearInterval(state.pollTimer); state.pollTimer = null; }
document.getElementById('refreshDatasets').addEventListener('click', loadDatasets);

// Kartu dataset dulu dibongkar total (container.innerHTML = '') dan dibangun
// ulang dari nol tiap polling, walau datanya sama persis -- itu yang bikin
// seluruh list "berkedip" tiap beberapa detik. Sekarang elemen kartu per
// dataset_id dipertahankan (state.cardElements) dan hanya bagian yang
// datanya beda saja yang ditulis ulang isinya lewat updateCardShell().
function renderDatasets() {
  const container = document.getElementById('datasetList');
  if (state.datasets.length === 0) {
    container.innerHTML = '<div class="empty">Belum ada dataset. Buat satu di tab Buat Dataset.</div>';
    state.cardElements = {};
    return;
  }
  const seen = new Set();
  state.datasets.forEach((ds, idx) => {
    seen.add(String(ds.dataset_id));
    let el = state.cardElements[ds.dataset_id];
    if (!el) {
      el = createDatasetCard(ds);
      state.cardElements[ds.dataset_id] = el;
    } else {
      updateDatasetCard(el, ds);
    }
    const atPos = container.children[idx];
    if (atPos !== el) container.insertBefore(el, atPos || null);
  });
  Object.keys(state.cardElements).forEach(id => {
    if (!seen.has(id)) {
      state.cardElements[id].remove();
      delete state.cardElements[id];
      delete state.structureHTML[id];
    }
  });
}

// Ringkasan level per satelit: "S1[R+P]", "MODIS[P]". Satu huruf per level
// supaya tiga satelit tetap muat satu baris di kartu yang sempit.
function sourceChipsHTML(ds) {
  const cfgs = ds.source_configs || [];
  if (!cfgs.length) return '';
  return '<div class="card-sources">' + cfgs.map(c => {
    const initials = (c.processing || []).map(p => p.charAt(0)).join('+') || '-';
    return '<span class="chip" style="--chip-color:' + sourceColor(c.source) + '">'
      + escapeHTML(SOURCE_SHORT[c.source] || c.source) + '[' + initials + ']</span>';
  }).join('') + '</div>';
}

// Hitungan scene dan ukuran per satelit. Keduanya datang dari listing
// endpoint (satu query agregat untuk seluruh halaman), bukan dari panggilan
// detail per kartu.
function perSourceStatsHTML(ds) {
  const scenes = ds.scenes_by_source || {};
  const bytes = ds.bytes_by_source || {};
  const keys = (ds.source_configs || [])
    .map(c => c.source)
    .filter(k => scenes[k] || bytes[k]);
  if (!keys.length) return '';

  const sceneLine = keys
    .map(k => (SOURCE_SHORT[k] || k) + ': ' + (scenes[k] || 0) + ' scene')
    .join(' · ');
  const byteLine = keys
    .map(k => (SOURCE_SHORT[k] || k) + ' ' + humanBytes(bytes[k] || 0))
    .join(' | ');
  return '<div class="card-persource">' + escapeHTML(sceneLine) + '</div>'
    + '<div class="card-persource dim">' + escapeHTML(byteLine) + '</div>';
}

// Strategi fusi ditampilkan di kartu karena dialah yang menentukan bentuk
// output -- berapa berkas HDF5 dan dari tanggal mana (DOCS/PIPELINE.md).
function fusionLabelHTML(ds) {
  if (!ds.fusion_strategy) return '';
  const extra = ds.fusion_output_only ? ' &middot; hasil fusi saja' : '';
  return '<div class="card-fusion">Strategi Fusi: <strong>'
    + escapeHTML(ds.fusion_strategy) + '</strong>' + extra + '</div>';
}

function cardShellHTML(ds) {
  const statusClass = statusToClass(ds.status);
  const canPause = ['QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canResume = ds.status === 'PAUSED';
  const canRetry = ds.status === 'FAILED';
  const canCancel = ['DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canDownload = ds.total_size_bytes > 0;
  const spinning = ACTIVE_STATUSES.has(ds.status) && ds.status !== 'PAUSED';
  const prog = state.progress[ds.dataset_id];
  const layers = (prog && prog.layers) || [];
  const collapsed = state.collapsedCards.has(String(ds.dataset_id));
  return (
    '<div class="card-head' + (spinning ? ' spinning' : '') + '">' +
      (layers.length ? '<div class="card-ring' + (spinning ? ' spinning' : '') + '">' + buildRingSVG(layers) + '</div>' : '') +
      '<div class="card-info">' +
        '<div class="card-title-row">' +
          '<span class="card-name">' + escapeHTML(ds.name) + '</span>' +
          '<span class="badge ' + statusClass + '">' + ds.status + '</span>' +
          '<button type="button" class="card-collapse-btn' + (collapsed ? ' collapsed' : '') +
            '" data-action="toggle-body" title="' + (collapsed ? 'Buka panel' : 'Tutup panel') +
            '" aria-label="' + (collapsed ? 'Buka panel' : 'Tutup panel') + '"><span class="chevron"></span></button>' +
        '</div>' +
        '<div class="card-meta">' + escapeHTML(ds.location_label || '-') + ' &middot; ' + ds.date_start + ' - ' + ds.date_end + '</div>' +
        datasetProgressHTML(ds, prog) +
        // Chip per-satelit, bukan per-tier: tier bukan pilihan user dan namanya
        // tidak pernah muncul di layar lain, sementara "S1[R+P]" adalah persis
        // yang user centang di wizard.
        sourceChipsHTML(ds) +
      '</div>' +
    '</div>' +
    '<div class="card-body' + (collapsed ? ' hidden' : '') + '">' +
      perSourceStatsHTML(ds) +
      fusionLabelHTML(ds) +
      ringLegendHTML(layers) +
      '<div class="card-stats">' +
        '<div><span class="stat-num">' + ds.total_scenes + '</span><span class="stat-label">scene</span></div>' +
        '<div><span class="stat-num">' + ds.completed_scenes + '</span><span class="stat-label">selesai</span></div>' +
        '<div><span class="stat-num">' + ds.failed_scenes + '</span><span class="stat-label">gagal</span></div>' +
        '<div><span class="stat-num">' + humanBytes(ds.total_size_bytes) + '</span><span class="stat-label">ukuran</span></div>' +
        '<div><span class="stat-num">' + logDurationText(ds.dataset_id) + '</span><span class="stat-label">durasi</span></div>' +
      '</div>' +
      renderLogPanel(ds.dataset_id) +
      '<div class="card-actions">' +
        (canPause ? '<button class="btn btn-warn" data-action="pause">Jeda</button>' : '') +
        (canResume ? '<button class="btn btn-accent" data-action="resume">Lanjutkan</button>' : '') +
        (canRetry ? '<button class="btn btn-accent" data-action="retry">Coba lagi</button>' : '') +
        (canCancel ? '<button class="btn btn-danger" data-action="cancel">Batalkan</button>' : '') +
        (canDownload ? '<a class="btn btn-ghost" href="/api/datasets/' + ds.dataset_id + '/download">Unduh</a>' : '') +
        (canDownload ? '<a class="btn btn-ghost" href="/api/datasets/' + ds.dataset_id + '/report" target="_blank">Laporan</a>' : '') +
        '<button class="btn btn-danger" data-action="delete">Hapus</button>' +
        '<button class="btn btn-ghost" data-action="toggle-scenes">Detail</button>' +
        (canDownload ? '<button class="btn btn-ghost" data-action="toggle-structure">Struktur</button>' : '') +
      '</div>' +
    '</div>'
  );
}

function createDatasetCard(ds) {
  const el = document.createElement('div');
  el.className = 'card';
  el._shellHTML = cardShellHTML(ds);
  el.innerHTML =
    '<div class="card-shell">' + el._shellHTML + '</div>' +
    '<div class="card-scenes' + (state.openScenes.has(ds.dataset_id) ? '' : ' hidden') + '" id="scenes-' + ds.dataset_id + '"></div>' +
    '<div class="card-structure' + (state.openStructure.has(ds.dataset_id) ? '' : ' hidden') + '" id="structure-' + ds.dataset_id + '"></div>';
  bindCardShell(el, ds.dataset_id);
  if (state.openScenes.has(ds.dataset_id)) renderSceneTable(el.querySelector('.card-scenes'), ds.dataset_id);
  if (state.openStructure.has(ds.dataset_id)) renderStructurePanel(el.querySelector('.card-structure'), ds.dataset_id);
  return el;
}

function bindCardShell(el, id) {
  el.querySelectorAll('.card-shell [data-action]').forEach(btn => btn.addEventListener('click', () => handleCardAction(btn.dataset.action, id)));
}

// Hanya menulis ulang bagian "shell" (ring/status/stats/log/tombol) kalau
// isinya benar-benar beda -- kalau tidak ada perubahan, DOM tidak disentuh
// sama sekali sehingga tidak ada kedipan. Panel scenes/structure tetap
// elemen yang sama antar-render, jadi isinya juga tidak sempat hilang dulu
// sebelum data baru muncul.
function updateDatasetCard(el, ds) {
  const nextHTML = cardShellHTML(ds);
  // Dibandingkan dengan string terakhir yang kita tulis, bukan shell.innerHTML:
  // browser menormalkan hasil baca innerHTML (mis. &middot; jadi ·), jadi
  // perbandingan ke DOM selalu "beda" dan kartu tetap ditulis ulang tiap poll.
  if (el._shellHTML !== nextHTML) {
    el._shellHTML = nextHTML;
    el.querySelector('.card-shell').innerHTML = nextHTML;
    bindCardShell(el, ds.dataset_id);
  }
  if (state.openScenes.has(ds.dataset_id)) renderSceneTable(el.querySelector('.card-scenes'), ds.dataset_id);
  if (state.openStructure.has(ds.dataset_id)) renderStructurePanel(el.querySelector('.card-structure'), ds.dataset_id);
}

// Durasi = selisih timestamp log pertama dataset dan log terbaru; ikut
// berubah tiap kali log baru masuk.
function logDurationText(id) {
  const logs = state.logs[id];
  if (!logs || logs.length === 0) return '-';
  let times = logs.map(l => new Date(l.timestamp).getTime());
  if (state.firstLogAt[id]) times.push(new Date(state.firstLogAt[id]).getTime());
  times = times.filter(t => !isNaN(t));
  if (times.length === 0) return '-';
  const mins = Math.floor((Math.max(...times) - Math.min(...times)) / 60000);
  return Math.floor(mins / 60) + 'j ' + (mins % 60) + 'm';
}

function renderLogPanel(id) {
  const logs = state.logs[id];
  if (!logs || logs.length === 0) return '';
  return '<div class="live-logs-title">Live Logs (terbaru)</div>' +
    '<div class="live-logs">' +
      logs.map(l => {
        const t = new Date(l.timestamp).toLocaleTimeString('id-ID', { hour12: false });
        const detail = formatLogDetail(l);
        return '<div class="log-row ' + logStatusClass(l.status) + '">' +
          '<span class="log-time">' + t + '</span>' +
          '<span class="log-scene">' + escapeHTML(shortenSceneId(l.scene_id)) + '</span>' +
          '<span class="log-stage">' + escapeHTML(l.stage) + '</span>' +
          '<span class="log-status">' + escapeHTML(l.status) + '</span>' +
          '<span class="log-msg">' + escapeHTML(l.message || '') + '</span>' +
        '</div>' +
        (detail ? '<div class="log-detail">' + detail + '</div>' : '');
      }).join('') +
    '</div>';
}
function formatLogDetail(l) {
  const d = l.details || {};
  const parts = [];
  if (d.progress_percent !== undefined && l.status === 'RUNNING') parts.push('Progress: ' + d.progress_percent + '%');
  if (d.attempt !== undefined && d.max_retries !== undefined) parts.push('Attempt: ' + d.attempt + '/' + d.max_retries);
  if (d.duration_seconds !== undefined) parts.push('Duration: ' + formatDuration(d.duration_seconds));
  if (d.file_size_mb !== undefined && d.file_size_mb !== null) parts.push('Size: ' + d.file_size_mb.toFixed(1) + ' MB');
  if (d.quality_score !== undefined && d.quality_score !== null) parts.push('Quality: ' + d.quality_score + '/100');
  if (d.memory_peak_mb !== undefined) parts.push('Mem peak: ' + d.memory_peak_mb.toFixed(0) + ' MB');
  if (l.status === 'FAILED' && d.error_type) parts.push(d.error_type + ': ' + (d.error_message || ''));
  return escapeHTML(parts.join('  ·  '));
}
function formatDuration(seconds) {
  if (seconds === undefined || seconds === null) return '-';
  const s = Math.round(seconds);
  if (s < 60) return s + 's';
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return m + 'm ' + rem + 's';
}
function logStatusClass(status) {
  if (status === 'COMPLETED') return 'log-ok';
  if (status === 'RUNNING' || status === 'WARNING') return 'log-progress';
  if (status === 'FAILED') return 'log-error';
  return 'log-muted';
}
function shortenSceneId(id) {
  if (!id) return '-';
  return id.length > 28 ? id.slice(0, 25) + '...' : id;
}

async function handleCardAction(action, id) {
  if (action === 'pause') {
    try { await api('/api/datasets/' + id + '/pause', { method: 'POST', body: JSON.stringify({}) }); showToast('Dataset dijeda', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'resume') {
    try { await api('/api/datasets/' + id + '/resume', { method: 'POST' }); showToast('Dataset dilanjutkan', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'retry') {
    try { await api('/api/pipeline/trigger?dataset_id=' + id, { method: 'POST' }); showToast('Job dijalankan ulang', 'success'); await refreshProgress(); }
    catch (e) { showToast(e.message, 'error'); }
  } else if (action === 'cancel') {
    openCancelModal(id);
  } else if (action === 'delete') {
    openDeleteModal(id);
  } else if (action === 'toggle-body') {
    const key = String(id);
    if (state.collapsedCards.has(key)) state.collapsedCards.delete(key);
    else state.collapsedCards.add(key);
    const el = state.cardElements[id];
    if (el) {
      el._shellHTML = cardShellHTML(state.datasets.find(d => String(d.dataset_id) === key));
      el.querySelector('.card-shell').innerHTML = el._shellHTML;
      bindCardShell(el, id);
    }
  } else if (action === 'toggle-scenes') {
    const box = document.getElementById('scenes-' + id);
    box.classList.toggle('hidden');
    if (!box.classList.contains('hidden')) { state.openScenes.add(id); renderSceneTable(box, id); }
    else { state.openScenes.delete(id); }
  } else if (action === 'toggle-structure') {
    const box = document.getElementById('structure-' + id);
    box.classList.toggle('hidden');
    if (!box.classList.contains('hidden')) { state.openStructure.add(id); renderStructurePanel(box, id); }
    else { state.openStructure.delete(id); }
  }
}

// Panel Detail: per satelit, daftar data yang sudah diunduh dan diproses,
// dengan bar storage tiap tahap. Warna tahap sama dengan radar di kartu
// (pastel = unduh, neon = proses), jadi keduanya dibaca dengan kunci yang sama.
// Tabel status pipeline S1 per scene tetap ada di bawahnya untuk pesan error.
state.detailData = {};
state.detailOpenStages = new Set();

async function renderSceneTable(box, id) {
  if (state.detailData[id]) drawDetailPanel(box, id);
  else if (!box._html) { box._html = '<div class="empty-small">Memuat detail…</div>'; box.innerHTML = box._html; }
  try {
    state.detailData[id] = await api('/api/datasets/' + id + '/storage/by-source');
  } catch (e) {
    if (!state.detailData[id]) { box._html = '<div class="empty-small">' + escapeHTML(e.message) + '</div>'; box.innerHTML = box._html; }
    return;
  }
  drawDetailPanel(box, id);
}

const DETAIL_PHASE_LABEL = { download: 'Diunduh', processing: 'Diproses', fusion: 'Difusi' };

function detailStageHTML(id, src, st, total) {
  const key = id + ':' + src + ':' + st.tier;
  const color = LAYER_COLORS[src + '_' + st.phase] || LAYER_COLORS.fusion;
  const pct = total > 0 ? st.size_bytes / total * 100 : 0;
  const open = state.detailOpenStages.has(key);
  return '<div class="detail-stage">' +
    '<button class="detail-stage-head" data-detail-stage="' + escapeHTML(key) + '" aria-expanded="' + open + '">' +
      '<span class="detail-caret">' + (open ? '▾' : '▸') + '</span>' +
      '<span class="detail-phase" style="--phase-color:' + color + '">' + (DETAIL_PHASE_LABEL[st.phase] || st.phase) + '</span>' +
      '<span class="detail-tier">' + escapeHTML(st.tier) + '</span>' +
      '<span class="detail-count">' + st.scenes.length + ' data · ' + st.file_count + ' berkas</span>' +
      '<span class="detail-size">' + humanBytes(st.size_bytes) + ' · ' + (pct < 1 && pct > 0 ? '<1' : Math.round(pct)) + '%</span>' +
    '</button>' +
    '<div class="detail-track"><div class="detail-bar" style="width:' + Math.max(pct, st.size_bytes ? 1 : 0).toFixed(2) + '%;background:' + color + '"></div></div>' +
    (open ? '<ul class="detail-scenes">' + st.scenes.map(sc =>
      '<li><span class="mono">' + escapeHTML(sc.scene) + '</span><span class="detail-scene-meta">' + sc.file_count + ' berkas · ' + humanBytes(sc.size_bytes) + '</span></li>'
    ).join('') + '</ul>' : '') +
  '</div>';
}

function drawDetailPanel(box, id) {
  const data = state.detailData[id] || { sources: {}, fusion: null };
  const srcKeys = SOURCE_ORDER_KEYS.filter(k => data.sources[k]);
  let html = '';
  if (!srcKeys.length && !data.fusion) {
    html += '<div class="empty-small">Belum ada data yang diunduh</div>';
  }
  srcKeys.forEach(src => {
    const info = data.sources[src];
    html += '<div class="detail-source">' +
      '<div class="detail-source-head"><span class="detail-source-name">' + escapeHTML(sourceLabel(src)) + '</span>' +
      '<span class="detail-size">' + humanBytes(info.size_bytes) + '</span></div>' +
      info.stages.map(st => detailStageHTML(id, src, st, info.size_bytes)).join('') +
    '</div>';
  });
  if (data.fusion) {
    const st = { tier: 'FUSED', phase: 'fusion', size_bytes: data.fusion.size_bytes,
      file_count: data.fusion.scenes.reduce((n, s) => n + s.file_count, 0), scenes: data.fusion.scenes };
    html += '<div class="detail-source">' +
      '<div class="detail-source-head"><span class="detail-source-name">Fusion</span>' +
      '<span class="detail-size">' + humanBytes(st.size_bytes) + '</span></div>' +
      detailStageHTML(id, 'fusion', st, st.size_bytes) +
    '</div>';
  }
  const prog = state.progress[id];
  if (prog && prog.scenes.length) {
    html += '<div class="struct-title">Status pipeline Sentinel-1</div>' +
      '<table class="scene-table"><thead><tr><th>Scene</th><th>Tahap</th><th>Status</th><th>Catatan</th></tr></thead><tbody>' +
      prog.scenes.map(s => '<tr><td class="mono">' + escapeHTML(s.product_identifier) + '</td><td>' + (s.current_stage || '-') + '</td><td><span class="badge ' + statusToClass(s.stage_status) + '">' + s.stage_status + '</span></td><td class="mono small">' + escapeHTML(s.last_error || '') + '</td></tr>').join('') +
      '</tbody></table>';
  }
  if (box._html === html) return;
  box._html = html;
  box.innerHTML = html;
  box.querySelectorAll('[data-detail-stage]').forEach(btn => btn.addEventListener('click', () => {
    const k = btn.dataset.detailStage;
    if (state.detailOpenStages.has(k)) state.detailOpenStages.delete(k); else state.detailOpenStages.add(k);
    drawDetailPanel(box, id);
  }));
}

let pendingDeleteId = null;
function openDeleteModal(id) { pendingDeleteId = id; document.getElementById('deleteModal').classList.remove('hidden'); }
function closeDeleteModal() { document.getElementById('deleteModal').classList.add('hidden'); pendingDeleteId = null; document.getElementById('deleteForce').checked = false; }
document.getElementById('deleteCancel').addEventListener('click', closeDeleteModal);
document.getElementById('deleteConfirm').addEventListener('click', async () => {
  const force = document.getElementById('deleteForce').checked;
  try {
    await api('/api/datasets/' + pendingDeleteId + '?force=' + force, { method: 'DELETE' });
    showToast('Penghapusan dimulai', 'success');
    closeDeleteModal();
    await loadDatasets();
  } catch (e) { showToast(e.message, 'error'); }
});

let pendingCancelId = null;
function openCancelModal(id) { pendingCancelId = id; document.getElementById('cancelModal').classList.remove('hidden'); }
function closeCancelModal() { document.getElementById('cancelModal').classList.add('hidden'); pendingCancelId = null; }
document.getElementById('cancelModalCancel').addEventListener('click', closeCancelModal);
document.getElementById('cancelModalConfirm').addEventListener('click', async () => {
  const btn = document.getElementById('cancelModalConfirm');
  btn.disabled = true; btn.textContent = 'Membatalkan...';
  try {
    const r = await api('/api/datasets/' + pendingCancelId + '/cancel', { method: 'POST', body: JSON.stringify({ cascade_delete: true }) });
    showToast('Dataset dibatalkan (' + r.deleted_files + ' file dihapus, tier ' + r.retained_tier + ' disimpan)', 'success');
    closeCancelModal();
    await loadDatasets();
  } catch (e) { showToast(e.message, 'error'); }
  finally { btn.disabled = false; btn.textContent = 'Ya, Batalkan'; }
});

// ---------------------------------------------------------------------------
// Live Monitoring (LIVE_MONITORING.md). Semua angka & kalimat dihitung server
// (etl/live_*.py); di sini hanya dirender. Kartu: 8 preview (2-3-3) + kalimat
// kondisi, daftar tanggal tersimpan, 3 grafik tren + perkiraan.
// ---------------------------------------------------------------------------

const LM = { areas: [], areaId: null, date: null, card: null };
const LM_ROWS = [
  ['s1_vv', 's1_vh'],
  ['modis_flood', 'modis_ndvi', 'modis_ndwi'],
  ['gpm_rain_24h', 'gpm_rain_72h', 'gpm_rain_7d'],
];
const LM_ROW_TITLES = ['Sentinel-1 (radar)', 'MODIS (optik) di atas Sentinel-1 VH', 'GPM (curah hujan) di atas Sentinel-1 VH'];
const LM_KEY_LABEL = {
  s1_vv: 'Sentinel-1 VV', s1_vh: 'Sentinel-1 VH', modis_flood: 'MODIS Peta Banjir',
  modis_ndvi: 'MODIS NDVI', modis_ndwi: 'MODIS Indeks Air (NDWI)',
  gpm_rain_24h: 'GPM Hujan 24 jam', gpm_rain_72h: 'GPM Hujan 72 jam', gpm_rain_7d: 'GPM Hujan 7 hari',
};
const LM_STATUS_TEXT = {
  BACKFILLING: 'Mengisi scene awal', RUNNING: 'Memeriksa scene baru', ACTIVE: 'Aktif',
  WAITING: 'Menunggu giliran',
  ERROR: 'Bermasalah', DELETED: 'Dihapus',
};

function lmLevelClass(level) {
  return level === 2 ? 'lv-high' : level === 1 ? 'lv-warn' : level === 0 ? 'lv-ok' : 'lv-na';
}
function lmDate(iso, long) {
  if (!iso) return '-';
  const d = new Date(iso + 'T00:00:00');
  return long ? d.toLocaleDateString('id-ID', { day: 'numeric', month: 'long', year: 'numeric' })
              : d.toLocaleDateString('id-ID', { day: '2-digit', month: '2-digit' });
}
function lmNum(v, nd) {
  if (v == null || !isFinite(v)) return '-';
  return Number(v).toLocaleString('id-ID', { maximumFractionDigits: nd == null ? 1 : nd, minimumFractionDigits: 0 });
}

async function loadLive() {
  try {
    LM.areas = await api('/api/live/areas');
  } catch (err) {
    document.getElementById('lmCard').innerHTML = '<div class="empty-small">' + escapeHTML(err.message) + '</div>';
    return;
  }
  const sel = document.getElementById('lmAreaSelect');
  if (!LM.areas.some(a => a.area_id === LM.areaId)) { LM.areaId = LM.areas.length ? LM.areas[0].area_id : null; LM.date = null; }
  sel.innerHTML = LM.areas.length
    ? LM.areas.map(a => '<option value="' + a.area_id + '"' + (a.area_id === LM.areaId ? ' selected' : '') + '>' +
        escapeHTML(a.name) + (a.latest_scene_date ? ' · ' + lmDate(a.latest_scene_date, true) : '') + '</option>').join('')
    : '<option value="">Belum ada daerah</option>';
  sel.disabled = !LM.areas.length;
  await loadLmActivity();
  renderLmMeta();
  await loadLmCard();
}

// 5 log terbaru daerah terpilih, hanya selama ada proses (bar tampil):
// sama seperti panel "Live Logs" di kartu Dataset Saya.
async function loadLmActivity() {
  const a = lmArea();
  const key = a ? 'lm-' + a.area_id : null;
  if (!a || !a.progress) { if (key) delete state.logs[key]; return; }
  try { state.logs[key] = await api('/api/live/areas/' + a.area_id + '/activity?limit=5'); }
  catch (err) { /* panel log bersifat tambahan; bar tetap tampil */ }
}

// Bar untuk kartu Dataset Saya: hanya selama job aktif. QUEUED menampilkan
// posisi antrean (MAX_ACTIVE_JOBS di server), bukan persen.
function datasetProgressHTML(ds, prog) {
  if (!ACTIVE_STATUSES.has(ds.status) || ds.status === 'DELETING') return '';
  const tp = timingParts(prog && prog.timing);
  if (ds.status === 'QUEUED') {
    const pos = prog && prog.queue_position;
    return progressBarHTML((pos ? 'Menunggu antrean (posisi ' + pos + ')' : 'Menunggu dimulai') + tp.suffix, null, { waiting: true });
  }
  if (ds.status === 'PAUSED') return progressBarHTML('Dijeda', prog ? prog.progress_percent : null, { waiting: true });
  const w = prog && prog.waiting;
  const obstacle = {
    waiting: !!w || !!(prog && prog.timing && prog.timing.stalled),
    notes: [{ text: waitNoteText(w), kind: 'warn' }, { text: tp.note, kind: 'warn' }],
  };
  const alerts = authAlertHTML(prog && prog.alerts);
  if (ds.status === 'PREPARING' || !prog || !prog.total_scenes) return progressBarHTML('Menyiapkan…' + tp.suffix, null, obstacle) + alerts;
  const total = prog.total_scenes;
  const failed = Math.min(total, prog.failed_count || 0);
  const ok = Math.min(total - failed, prog.processed_count || 0);
  const label = (ds.status === 'DOWNLOADING' ? 'Mengunduh' : 'Memproses') + ' · ' + sceneCountText(ok, failed, total) + tp.suffix;
  return progressBarHTML(label, prog.progress_percent, { ...obstacle, failPct: failed / total * 100 }) + alerts;
}

// Loading bar. percent null/undefined = indeterminate (tahap tanpa ukuran).
// "45 dtk" / "12 mnt" / "1 j 5 mnt"
function fmtDuration(s) {
  if (s === null || s === undefined) return '';
  if (s < 60) return Math.max(0, Math.round(s)) + ' dtk';
  const m = Math.floor(s / 60);
  if (m < 60) return m + ' mnt';
  return Math.floor(m / 60) + ' j' + (m % 60 ? ' ' + (m % 60) + ' mnt' : '');
}

// Durasi ditempel ke label, dan catatan "tidak ada kemajuan" kalau server
// menilai run ini diam lebih lama dari PROGRESS_STALL_AFTER_S.
function timingParts(t) {
  if (!t) return { suffix: '', note: '' };
  const suffix = t.elapsed_s !== null && t.elapsed_s !== undefined ? ' · ' + fmtDuration(t.elapsed_s) : '';
  let note = '';
  if (t.stalled && t.last_activity_at) {
    const at = new Date(t.last_activity_at * 1000).toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' });
    note = 'Tidak ada kemajuan sejak ' + at + ' (' + fmtDuration(t.idle_s) + ') — cek log atau batalkan & ulangi';
  }
  return { suffix, note };
}

// opts: waiting (bar amber), failPct (segmen merah, bagian dari percent),
// notes: [{text, kind: 'warn' | 'error'}] baris keterangan di bawah bar.
function progressBarHTML(label, percent, opts) {
  const o = opts || {};
  const known = typeof percent === 'number' && isFinite(percent);
  const pct = known ? Math.max(0, Math.min(100, Math.round(percent))) : null;
  const failW = known ? Math.max(0, Math.min(pct, Math.round(o.failPct || 0))) : 0;
  const fill = known
    ? '<div class="pbar-fill" style="width:' + (pct - failW) + '%"></div>' +
      (failW ? '<div class="pbar-fail" style="width:' + failW + '%"></div>' : '')
    : '<div class="pbar-fill"></div>';
  return '<div class="pbar' + (known ? '' : ' indeterminate') + (o.waiting ? ' waiting' : '') + '"' +
      ' role="progressbar" aria-label="' + escapeHTML(label) + '"' +
      (known ? ' aria-valuemin="0" aria-valuemax="100" aria-valuenow="' + pct + '"' : '') + '>' +
    '<div class="pbar-row"><span>' + escapeHTML(label) + '</span>' +
      (known ? '<span class="pbar-pct">' + pct + '%</span>' : '') + '</div>' +
    '<div class="pbar-track">' + fill + '</div>' +
    (o.notes || []).filter(n => n && n.text).map(n =>
      '<div class="pbar-note ' + (n.kind || 'warn') + '">' + escapeHTML(n.text) + '</div>').join('') +
  '</div>';
}

// "Menunggu server CDSE (Sentinel-1): dibatasi server (429) · ±45 dtk · percobaan 2/8"
function waitNoteText(w) {
  if (!w) return '';
  return 'Menunggu server ' + w.source_label + ': ' + w.reason +
    ' · ±' + w.remaining_s + ' dtk' +
    (w.attempt ? ' · percobaan ' + w.attempt + (w.max_attempts ? '/' + w.max_attempts : '') : '');
}

// Token NASA ditolak: bukan hambatan sementara, pengguna harus bertindak.
function authAlertHTML(alerts) {
  if (!alerts || !alerts.length) return '';
  return '<div class="pbar-alert" role="alert"><strong>Token NASA tidak valid atau kedaluwarsa.</strong> ' +
    'MODIS/GPM tidak bisa diunduh (' + alerts.map(x => escapeHTML(x.source)).join(', ') + '). ' +
    'Perbarui <code>NASA_EARTHDATA_TOKEN</code> di .env lalu jalankan ulang server.</div>';
}

// "2 selesai · 1 gagal / 4 scene"
function sceneCountText(ok, failed, total) {
  return ok + ' selesai' + (failed ? ' · ' + failed + ' gagal' : '') + ' / ' + total + ' scene';
}

// Ringkasan siklus terakhir (beberapa menit setelah selesai).
function lmResultHTML(r) {
  const at = r.at ? new Date(r.at).toLocaleTimeString('id-ID', { hour: '2-digit', minute: '2-digit' }) : '';
  return '<div class="pbar-result ' + escapeHTML(r.level || 'ok') + '" role="status">' +
    escapeHTML(r.text) + (at ? ' <span class="pbar-result-at">· ' + at + '</span>' : '') + '</div>';
}

function lmProgressHTML(a, withAlerts) {
  const p = a && a.progress;
  let html = '';
  if (p) {
    const tp = timingParts(p.timing);
    const label = (p.total ? p.phase + ' · ' + sceneCountText(p.ok, p.failed, p.total) : p.phase) + tp.suffix;
    const w = p.waiting;
    html = progressBarHTML(label, p.percent, {
      waiting: a.status === 'WAITING' || !!w || !!(p.timing && p.timing.stalled),
      failPct: p.total ? p.failed / p.total * 100 : 0,
      notes: [{ text: waitNoteText(w), kind: 'warn' }, { text: tp.note, kind: 'warn' }],
    }) + (withAlerts === false ? '' : '<div class="lm-logs">' + renderLogPanel('lm-' + a.area_id) + '</div>');
  } else if (a && a.last_result && withAlerts !== false) {
    html = lmResultHTML(a.last_result);
  }
  // Peringatan token cukup sekali, di baris meta; kartu kosong hanya bar-nya.
  return html + (withAlerts === false ? '' : authAlertHTML(a && a.alerts));
}

function lmArea() { return LM.areas.find(a => a.area_id === LM.areaId) || null; }

function renderLmMeta() {
  const box = document.getElementById('lmAreaMeta');
  const a = lmArea();
  if (!a) { box.innerHTML = ''; return; }
  const opts = Array.from({ length: 12 }, (_, i) => i + 1)
    .map(n => '<option value="' + n + '"' + (n === a.retention ? ' selected' : '') + '>' + n + '</option>').join('');
  box.innerHTML =
    '<span class="lm-pill ' + (a.status === 'ERROR' ? 'lv-high' : a.running ? 'lv-warn' : 'lv-ok') + '">' +
      escapeHTML(a.running ? (LM_STATUS_TEXT[a.status] || 'Memproses') + '…' : (LM_STATUS_TEXT[a.status] || a.status)) + '</span>' +
    '<span class="lm-meta-item">' + a.scene_count + '/' + a.retention + ' scene</span>' +
    '<span class="lm-meta-item">' + humanBytes(a.total_size_bytes) + '</span>' +
    '<span class="lm-meta-item">dicek ' + (a.last_checked_at ? new Date(a.last_checked_at).toLocaleString('id-ID') : 'belum') + '</span>' +
    (a.status_message ? '<span class="lm-meta-item lm-meta-msg">' + escapeHTML(a.status_message) + '</span>' : '') +
    '<span class="lm-meta-actions">' +
      '<label class="lm-ret">Simpan <select id="lmRetSelect">' + opts + '</select> scene</label>' +
      '<button type="button" class="btn btn-ghost btn-sm" id="lmCheckBtn"' + (a.running ? ' disabled' : '') + '>Cek sekarang</button>' +
      '<button type="button" class="btn btn-danger btn-sm" id="lmDeleteBtn">Hapus daerah</button>' +
    '</span>' +
    lmProgressHTML(a);
  document.getElementById('lmRetSelect').addEventListener('change', e => lmChangeRetention(a, parseInt(e.target.value, 10), e.target));
  document.getElementById('lmCheckBtn').addEventListener('click', lmCheckNow);
  document.getElementById('lmDeleteBtn').addEventListener('click', () => lmConfirm(
    'Hapus daerah "' + a.name + '"?',
    'Semua citra dan preview daerah ini dihapus permanen (' + humanBytes(a.total_size_bytes) + '). Log dan angka ringkas tetap disimpan untuk audit.',
    async () => {
      const r = await api('/api/live/areas/' + a.area_id, { method: 'DELETE' });
      showToast('Daerah dihapus, ' + humanBytes(r.freed_bytes) + ' dibebaskan', 'success');
      LM.areaId = null; await loadLive();
    }));
}

async function lmChangeRetention(a, n, selectEl) {
  const apply = async () => {
    await api('/api/live/areas/' + a.area_id, { method: 'PATCH', body: JSON.stringify({ retention: n }) });
    showToast('Retensi diubah ke ' + n + ' scene', 'success');
    await loadLive();
  };
  if (n < a.scene_count) {
    selectEl.value = a.retention;
    lmConfirm('Turunkan retensi ke ' + n + '?',
      (a.scene_count - n) + ' scene paling lama akan dihapus permanen sekarang juga. Log-nya tetap disimpan.', apply);
    return;
  }
  try { await apply(); } catch (err) { showToast(err.message, 'error'); }
}

async function lmCheckNow() {
  try {
    const r = await api('/api/live/areas/' + LM.areaId + '/check', { method: 'POST' });
    showToast(r.message, 'success'); await loadLive();
  } catch (err) { showToast(err.message, 'error'); }
}

async function loadLmCard() {
  const box = document.getElementById('lmCard');
  const a = lmArea();
  if (!a) {
    box.innerHTML = '<div class="lm-empty"><p>Belum ada Daerah Live.</p>' +
      '<p class="lm-dim">Tambahkan daerah untuk memantau genangan &amp; hujan secara otomatis.</p></div>';
    return;
  }
  try {
    LM.card = await api('/api/live/areas/' + a.area_id + '/card' + (LM.date ? '?date=' + LM.date : ''));
  } catch (err) { box.innerHTML = '<div class="empty-small">' + escapeHTML(err.message) + '</div>'; return; }
  renderLmCard();
}

function renderLmCard() {
  const box = document.getElementById('lmCard');
  const { area, scene, dates, forecast } = LM.card;
  if (!scene) {
    box.innerHTML = '<div class="lm-empty"><p>' + escapeHTML(area.name) + ' sedang disiapkan.</p>' +
      '<p class="lm-dim">' + (area.running
        ? 'Sistem sedang mengunduh dan memproses scene Sentinel-1 terbaru. Proses awal bisa memakan waktu 15–60 menit per scene.'
        : escapeHTML(area.status_message || 'Belum ada scene Sentinel-1 yang berhasil diproses.')) + '</p>' +
      lmProgressHTML(lmArea() || area, false) + '</div>';
    return;
  }
  const latest = dates.length ? dates[0].date : scene.date;
  const st = scene.area_status || {};
  let html =
    '<header class="lm-head">' +
      '<h2>' + escapeHTML(area.name.toUpperCase()) + ' <span class="lm-dim">· scene terbaru: ' + lmDate(latest, true) + '</span></h2>' +
      '<p class="lm-status ' + lmLevelClass(st.level) + '">' + escapeHTML(st.text || '-') + '</p>' +
      (scene.date !== latest ? '<p class="lm-dim">Menampilkan scene ' + lmDate(scene.date, true) + '</p>' : '') +
    '</header>';

  LM_ROWS.forEach((row, i) => {
    html += '<h3 class="lm-row-title">' + LM_ROW_TITLES[i] + '</h3><div class="lm-row lm-row-' + row.length + '">' +
      row.map(k => lmTile(k, scene)).join('') + '</div>';
  });

  html += '<h3 class="lm-row-title">Tanggal tersimpan</h3><div class="lm-dates">' +
    dates.map(d => '<button type="button" class="lm-date' + (d.date === scene.date ? ' active' : '') + '" data-date="' + d.date + '">' +
      '<i class="' + lmLevelClass(d.level) + '"></i>' + lmDate(d.date) + '<small>' + new Date(d.date + 'T00:00:00').getFullYear() + '</small></button>').join('') +
    '</div>';

  const series = (forecast && forecast.series) || {};
  html += '<h3 class="lm-row-title">Tren &amp; perkiraan</h3><div class="lm-charts">' +
    [['sentinel1', 'Sentinel-1'], ['modis', 'MODIS'], ['gpm', 'GPM']].map(([k, t]) =>
      '<figure class="lm-chart"><figcaption>' + t + ' — ' + escapeHTML(series[k] ? series[k].label + ' (' + series[k].unit + ')' : '') +
      '</figcaption>' + (series[k] ? lmChartSVG(series[k], scene.date) : '<div class="empty-small">Belum ada data</div>') + '</figure>').join('') +
    '</div><p class="lm-dim lm-fc-note">Garis putus-putus dan pita = <b>perkiraan</b> (' + lmFcMethod(forecast) + '), bukan data pengamatan.</p>';
  box.innerHTML = html;

  box.querySelectorAll('.lm-date').forEach(b => b.addEventListener('click', () => { LM.date = b.dataset.date; loadLmCard(); }));
  box.querySelectorAll('.lm-tile img').forEach(img => img.addEventListener('click', () => {
    document.getElementById('lmImageFull').src = img.src;
    document.getElementById('lmImageCaption').textContent = img.dataset.caption;
    document.getElementById('lmImageModal').classList.remove('hidden');
  }));
  box.querySelectorAll('.lm-retry').forEach(b => b.addEventListener('click', async () => {
    try { const r = await api('/api/live/areas/' + area.area_id + '/scenes/' + scene.date + '/retry', { method: 'POST' });
      if (r && r.started === false) showToast(r.message || 'Siklus daerah ini sedang berjalan');
      else showToast('Mencoba ulang MODIS/GPM untuk ' + lmDate(scene.date, true), 'success'); }
    catch (err) { showToast(err.message, 'error'); }
  }));
  lmBindChartHover(box);
}

function lmFcMethod(fc) {
  const m = fc && fc.series && fc.series.sentinel1 && fc.series.sentinel1.forecast.method;
  return { holt: 'Holt exponential smoothing', ses: 'exponential smoothing sederhana', persistence: 'nilai terakhir — data belum cukup' }[m] || 'exponential smoothing';
}

function lmTile(key, scene) {
  const p = scene.previews[key];
  const it = (scene.interpretations || {})[key] || {};
  const src = key.startsWith('modis') ? 'modis' : key.startsWith('gpm') ? 'gpm' : 'sentinel1';
  const failed = (scene.source_status[src] || {}).status === 'FAILED';
  const nearest = p && p.source_date && p.source_date !== scene.date;
  const sentence = it.text
    ? escapeHTML(it.text).replace(escapeHTML(it.category), '<b class="' + lmLevelClass(it.level) + '">' + escapeHTML(it.category) + '</b>')
    : '';
  return '<div class="lm-tile">' +
    (p ? '<img loading="lazy" src="' + p.url + '" alt="' + escapeHTML(LM_KEY_LABEL[key]) + '" data-caption="' + escapeHTML(LM_KEY_LABEL[key] + ' — ' + lmDate(scene.date, true)) + '">'
       : '<div class="lm-noimg">tidak tersedia</div>') +
    '<div class="lm-tile-head"><span>' + LM_KEY_LABEL[key] + '</span>' +
      (nearest ? '<span class="lm-badge" title="Data sumber dari tanggal terdekat">terdekat ' + lmDate(p.source_date) + '</span>' : '') + '</div>' +
    (p ? lmLegend(p.legend) : '') +
    '<p class="lm-sentence">' + sentence + '</p>' +
    (failed && src !== 'sentinel1' ? '<button type="button" class="lm-link lm-retry">Coba unduh ulang</button>' : '') +
  '</div>';
}

function lmLegend(lg) {
  if (!lg) return '';
  if (lg.type === 'categorical') {
    return '<div class="lm-legend lm-legend-cat">' + lg.categories.map(c =>
      '<span><i style="background:' + c.color + '"></i>' + escapeHTML(c.label) + '</span>').join('') + '</div>';
  }
  const unit = lg.units && lg.units !== 'indeks' ? ' ' + lg.units : '';
  return '<div class="lm-legend"><span>' + lmNum(lg.min, 2) + '</span>' +
    '<i class="lm-ramp" style="background:linear-gradient(90deg,' + lg.stops.join(',') + ')"></i>' +
    '<span>' + lmNum(lg.max, 2) + unit + '</span></div>';
}

// Grafik SVG satu deret: aktual (garis/batang) + perkiraan (putus-putus + pita)
// + garis ambang (GPM) + penanda scene terpilih. Hover: tooltip per titik.
function lmChartSVG(s, selected) {
  const W = 560, H = 190, L = 44, R = 12, T = 12, B = 26;
  const act = s.actual.filter(p => p.value != null);
  const fc = (s.forecast && s.forecast.points) || [];
  if (!act.length) return '<div class="empty-small">Belum ada data</div>';
  const t = d => new Date(d + 'T00:00:00').getTime();
  const xsAll = act.map(p => t(p.date)).concat(fc.map(p => t(p.date)));
  let x0 = Math.min(...xsAll), x1 = Math.max(...xsAll);
  if (x1 === x0) { x0 -= 6 * 864e5; x1 += 6 * 864e5; }
  const thr = s.thresholds || {};
  const ys = act.map(p => p.value).concat(fc.flatMap(p => [p.lo, p.hi])).concat(Object.values(thr));
  let y0 = Math.min(...ys), y1 = Math.max(...ys);
  if (s.chart === 'bar') y0 = 0;
  const pad = (y1 - y0) * 0.08 || 1; if (s.chart !== 'bar') y0 -= pad; y1 += pad;
  const X = v => L + (v - x0) / (x1 - x0) * (W - L - R);
  const Y = v => T + (1 - (v - y0) / (y1 - y0)) * (H - T - B);
  let g = '';
  for (let i = 0; i <= 3; i++) {
    const v = y0 + (y1 - y0) * i / 3;
    g += '<line class="lm-grid" x1="' + L + '" x2="' + (W - R) + '" y1="' + Y(v) + '" y2="' + Y(v) + '"/>' +
      '<text class="lm-axis" x="' + (L - 6) + '" y="' + (Y(v) + 3) + '" text-anchor="end">' + lmNum(v, Math.abs(y1 - y0) < 5 ? 1 : 0) + '</text>';
  }
  const ticks = act.concat(fc.length ? [fc[fc.length - 1]] : []);
  const step = Math.max(1, Math.ceil(ticks.length / 6));
  ticks.forEach((p, i) => { if (i % step === 0 || i === ticks.length - 1)
    g += '<text class="lm-axis" x="' + X(t(p.date)) + '" y="' + (H - 8) + '" text-anchor="middle">' + lmDate(p.date) + '</text>'; });
  if (selected && act.some(p => p.date === selected))
    g += '<line class="lm-sel" x1="' + X(t(selected)) + '" x2="' + X(t(selected)) + '" y1="' + T + '" y2="' + (H - B) + '"/>';
  Object.entries(thr).forEach(([name, v]) => {
    const cls = name === 'tinggi' ? 'lv-high' : 'lv-warn';
    g += '<line class="lm-thr ' + cls + '" x1="' + L + '" x2="' + (W - R) + '" y1="' + Y(v) + '" y2="' + Y(v) + '"/>' +
      '<text class="lm-thr-label" x="' + (L + 4) + '" y="' + (Y(v) - 4) + '">ambang ' + name + ' ' + lmNum(v, 0) + '</text>';
  });
  const last = act[act.length - 1];
  if (fc.length) {
    const band = [[X(t(last.date)), Y(last.value)]].concat(fc.map(p => [X(t(p.date)), Y(p.hi)]))
      .concat(fc.slice().reverse().map(p => [X(t(p.date)), Y(p.lo)]));
    g += '<polygon class="lm-band" points="' + band.map(q => q.join(',')).join(' ') + '"/>';
    g += '<polyline class="lm-fc" points="' + [[X(t(last.date)), Y(last.value)]].concat(fc.map(p => [X(t(p.date)), Y(p.mean)])).map(q => q.join(',')).join(' ') + '"/>';
    const lp = fc[fc.length - 1];
    g += '<text class="lm-fc-label" x="' + (W - R - 2) + '" y="' + (T + 10) + '" text-anchor="end">perkiraan' +
      (s.forecast.note ? ' (' + s.forecast.note + ')' : '') + ' →</text>';
  }
  const unit = ' ' + s.unit;
  if (s.chart === 'bar') {
    const bw = Math.max(6, Math.min(22, (W - L - R) / (xsAll.length * 2.2)));
    act.forEach(p => { const y = Y(p.value), yb = Y(0);
      g += '<rect class="lm-bar' + (p.date === selected ? ' sel' : '') + '" x="' + (X(t(p.date)) - bw / 2) + '" y="' + y + '" width="' + bw + '" height="' + Math.max(1, yb - y) + '" rx="3"/>'; });
    fc.forEach(p => { const y = Y(p.mean), yb = Y(0);
      g += '<rect class="lm-bar-fc" x="' + (X(t(p.date)) - bw / 2) + '" y="' + y + '" width="' + bw + '" height="' + Math.max(1, yb - y) + '" rx="3"/>'; });
  } else {
    g += '<polyline class="lm-line" points="' + act.map(p => X(t(p.date)) + ',' + Y(p.value)).join(' ') + '"/>';
    act.forEach(p => { g += '<circle class="lm-dot' + (p.date === selected ? ' sel' : '') + '" cx="' + X(t(p.date)) + '" cy="' + Y(p.value) + '" r="4"/>'; });
  }
  // Area hover per titik, lebih besar dari marknya.
  const pts = act.map(p => ({ d: p.date, v: p.value, kind: 'aktual' }))
    .concat(fc.map(p => ({ d: p.date, v: p.mean, lo: p.lo, hi: p.hi, kind: 'perkiraan' })));
  pts.forEach(p => {
    const tip = lmDate(p.d, true) + ' — ' + p.kind + ': ' + lmNum(p.v) + unit +
      (p.kind === 'perkiraan' ? ' (rentang ' + lmNum(p.lo) + '–' + lmNum(p.hi) + ')' : '');
    g += '<circle class="lm-hit" cx="' + X(t(p.d)) + '" cy="' + Y(p.v) + '" r="14" data-tip="' + escapeHTML(tip) + '"/>';
  });
  return '<div class="lm-chart-box"><svg viewBox="0 0 ' + W + ' ' + H + '" role="img" aria-label="' + escapeHTML(s.label) + '">' + g + '</svg><div class="lm-tip hidden"></div></div>';
}

function lmBindChartHover(root) {
  root.querySelectorAll('.lm-chart-box').forEach(box => {
    const tip = box.querySelector('.lm-tip');
    box.querySelectorAll('.lm-hit').forEach(h => {
      h.addEventListener('mouseenter', () => {
        const r = box.getBoundingClientRect(), c = h.getBoundingClientRect();
        tip.textContent = h.dataset.tip; tip.classList.remove('hidden');
        tip.style.left = Math.min(r.width - 200, Math.max(0, c.left - r.left - 80)) + 'px';
        tip.style.top = Math.max(0, c.top - r.top - 36) + 'px';
      });
      h.addEventListener('mouseleave', () => tip.classList.add('hidden'));
    });
  });
}

function startLivePolling() {
  stopLivePolling();
  // Dimuat ulang hanya saat ada daerah yang sedang diproses: preview yang
  // sudah jadi tidak perlu diambil ulang terus-menerus.
  state.livePollTimer = setInterval(() => { if (LM.areas.some(a => a.running || a.status === 'BACKFILLING' || a.status === 'WAITING')) loadLive(); }, 10000);
}
function stopLivePolling() { if (state.livePollTimer) clearInterval(state.livePollTimer); state.livePollTimer = null; }

let lmConfirmAction = null;
function lmConfirm(title, text, action) {
  document.getElementById('lmConfirmTitle').textContent = title;
  document.getElementById('lmConfirmText').textContent = text;
  lmConfirmAction = action;
  document.getElementById('lmConfirmModal').classList.remove('hidden');
}
document.getElementById('lmConfirmCancel').addEventListener('click', () => document.getElementById('lmConfirmModal').classList.add('hidden'));
document.getElementById('lmConfirmOk').addEventListener('click', async () => {
  const btn = document.getElementById('lmConfirmOk');
  btn.disabled = true;
  try { if (lmConfirmAction) await lmConfirmAction(); }
  catch (err) { showToast(err.message, 'error'); }
  finally { btn.disabled = false; document.getElementById('lmConfirmModal').classList.add('hidden'); }
});
document.getElementById('lmImageClose').addEventListener('click', () => document.getElementById('lmImageModal').classList.add('hidden'));

document.getElementById('lmAreaSelect').addEventListener('change', e => {
  LM.areaId = parseInt(e.target.value, 10) || null; LM.date = null; renderLmMeta(); loadLmCard();
});

function openLmAddModal(regionId) {
  const sel = document.getElementById('lmRegion');
  sel.innerHTML = (state.regions || []).map(r => '<option value="' + r.region_id + '"' + (r.region_id === regionId ? ' selected' : '') + '>' + escapeHTML(r.name) + '</option>').join('');
  document.getElementById('lmAddModal').classList.remove('hidden');
}
document.getElementById('lmAddBtn').addEventListener('click', async () => {
  if (!state.regions || !state.regions.length) { try { await loadRegions(); } catch (e) {} }
  openLmAddModal();
});
document.getElementById('lmAddCancel').addEventListener('click', () => document.getElementById('lmAddModal').classList.add('hidden'));
document.getElementById('lmNewLocation').addEventListener('click', () => {
  document.getElementById('lmAddModal').classList.add('hidden');
  state.lmReopenAdd = true;
  openAddLocationModal();
});
document.getElementById('lmAddConfirm').addEventListener('click', async () => {
  const btn = document.getElementById('lmAddConfirm');
  const retention = parseInt(document.getElementById('lmRetention').value, 10);
  if (!(retention >= 1 && retention <= 12)) { showToast('Jumlah scene harus 1–12', 'error'); return; }
  btn.disabled = true;
  try {
    const a = await api('/api/live/areas', { method: 'POST', body: JSON.stringify({
      region_id: parseInt(document.getElementById('lmRegion').value, 10),
      name: document.getElementById('lmName').value.trim() || null, retention }) });
    document.getElementById('lmAddModal').classList.add('hidden');
    document.getElementById('lmName').value = '';
    showToast('Daerah "' + a.name + '" ditambahkan, pengisian awal dimulai', 'success');
    LM.areaId = a.area_id; LM.date = null;
    await loadLive();
  } catch (err) { showToast(err.message, 'error'); }
  finally { btn.disabled = false; }
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.getElementById('deleteModal').classList.add('hidden');
    document.getElementById('cancelModal').classList.add('hidden');
    ['lmAddModal', 'lmConfirmModal', 'lmImageModal'].forEach(id => document.getElementById(id).classList.add('hidden'));
  }
});

switchTab('create');


// ---------------------------------------------------------------------------
// Panel "Struktur": rincian storage per tier x source, kualitas per source,
// dan daftar berkas per tier. Semua angkanya berasal dari
// folder_manager.storage_breakdown lewat /api/datasets/{id}/storage/summary,
// sumber yang sama dengan metadata.json -- jadi UI dan file di disk tidak bisa
// bercerita beda.
// ---------------------------------------------------------------------------

function sourceColor(src) { return SOURCE_COLORS[src] || 'var(--text-dim)'; }
function sourceLabel(src) { return SOURCE_LABELS[src] || src; }

async function renderStructurePanel(box, id) {
  // Kartu dataset digambar ulang tiap polling, jadi `box` selalu elemen baru
  // yang kosong. Isi dulu dari cache (kalau ada) supaya panel tidak berkedip
  // ke "Memuat struktur..." tiap beberapa detik sementara fetch terbaru jalan
  // di belakang -- sama seperti pola yang dipakai renderPreviewGallery.
  // Box-nya sendiri sekarang elemen yang sama antar-polling (lihat
  // createDatasetCard/updateDatasetCard), jadi kalau sudah pernah terisi
  // tidak perlu disentuh sampai data baru benar-benar siap.
  const cached = state.structureHTML[id];
  if (!box.innerHTML) {
    box.innerHTML = cached || '<div class="empty-small">Memuat struktur…</div>';
    if (cached) bindStructurePanel(box, id);
  }

  let storage;
  try {
    storage = await api('/api/datasets/' + id + '/storage/summary');
  } catch (e) {
    if (!cached) box.innerHTML = '<div class="empty-small">' + escapeHTML(e.message) + '</div>';
    return;
  }

  // Kualitas opsional: dataset yang belum sempat lewat tahap analitik tetap
  // harus bisa menampilkan rincian storage-nya.
  let quality = { sources: [] };
  try { quality = await api('/api/quality/dataset/' + id + '/by-source'); } catch (e) {}

  const html =
    renderStorageBreakdown(id, storage, quality) +
    '<div class="struct-files" id="structfiles-' + id + '"></div>' +
    '<div class="mask-section" id="masks-' + id + '"></div>' +
    '<div class="preview-section" id="preview-' + id + '"></div>';
  // Ganti langsung isinya kalau ada perubahan saja -- kalau datanya sama
  // persis dengan sebelumnya, DOM tidak disentuh sama sekali.
  if (html !== state.structureHTML[id]) {
    state.structureHTML[id] = html;
    box.innerHTML = html;
    bindStructurePanel(box, id);
  }

  // Galeri di-fetch terpisah dan tidak di-await: rincian storage sudah bisa
  // dibaca sementara daftar preview masih jalan, dan dataset yang tier
  // preview-nya kosong tidak menahan apa pun.
  renderPreviewGallery(id);
  renderMaskLayers(id);
}

// ---------------------------------------------------------------------------
// Layer referensi (masks/): darat-laut dan air permanen.
//
// Beda mendasar dari galeri preview di bawah, dan itu yang menentukan
// bentuk UI-nya: preview itu PER TANGGAL, layer referensi PER DATASET. Garis
// pantai tidak berubah antar tanggal, jadi tidak ada pemilih tanggal di sini
// dan tidak boleh ada -- satu berkas berlaku untuk seluruh stack.
//
// Angka statistiknya datang dari manifest JSON yang ditulis modul pembuatnya,
// bukan dihitung ulang di sini, supaya yang dibaca peneliti di layar persis
// yang tertanam di berkas yang dikirim ke deep learning engineer.
// ---------------------------------------------------------------------------

async function renderMaskLayers(id) {
  const box = document.getElementById('masks-' + id);
  if (!box) return;

  let data;
  try {
    data = await api('/api/datasets/' + id + '/masks');
  } catch (e) {
    box.innerHTML = '';
    return;
  }
  if (!data.layers || data.layers.length === 0) {
    // Sengaja kosong tanpa pesan: dataset yang belum sampai fusion memang
    // belum punya layer ini, dan itu keadaan normal -- bukan sesuatu yang
    // perlu diumumkan sebagai kekurangan di tiap kartu dataset.
    box.innerHTML = '';
    return;
  }

  const cards = data.layers.map(l => {
    const st = l.statistics || {};
    let facts = [];
    if (st.pct_sea !== undefined) {
      facts.push(['laut', st.pct_sea.toFixed(2) + '%']);
      facts.push(['darat', st.pct_land.toFixed(2) + '%']);
      if (st.clamp_m) facts.push(['batas jarak', '±' + (st.clamp_m / 1000) + ' km']);
    }
    if (st.pct_occurrence_ge_90 !== undefined) {
      facts.push(['air permanen (≥90%)', st.pct_occurrence_ge_90.toFixed(2) + '%']);
      facts.push(['musiman (≥50%)', st.pct_occurrence_ge_50.toFixed(2) + '%']);
      if (st.source_resolution_m) facts.push(['sumber', st.source_resolution_m + ' m']);
    }

    const img = l.image_url
      ? '<img src="' + escapeHTML(l.image_url) + '" alt="' + escapeHTML(l.label) + '" loading="lazy">'
      : '<div class="mask-noimg">tanpa preview</div>';

    return '<figure class="mask-card">' +
      img +
      '<figcaption>' +
        '<span class="mask-title">' + escapeHTML(l.label) + '</span>' +
        '<span class="mask-interp">' + escapeHTML(l.interpretation || '') + '</span>' +
        '<div class="mask-facts">' +
          facts.map(f =>
            '<span class="mask-fact"><b>' + escapeHTML(f[1]) + '</b>' +
            escapeHTML(f[0]) + '</span>').join('') +
        '</div>' +
        '<span class="mask-file">' + escapeHTML(l.data_file) + ' · ' +
          humanBytes(l.size_bytes) + '</span>' +
      '</figcaption>' +
    '</figure>';
  }).join('');

  const html =
    '<div class="mask-head">' +
      '<h4>Layer Referensi</h4>' +
      '<span class="mask-sub">' + escapeHTML(data.applies_to) + ' · ' +
        humanBytes(data.total_size_bytes) + '</span>' +
    '</div>' +
    '<p class="mask-note">Informasi tambahan, bukan penyaring — data mentah ' +
      'dan fusion tidak diubah sama sekali. Sungai dan danau sengaja ' +
      'dipertahankan karena luapannya justru sinyal banjir yang dicari.</p>' +
    '<div class="mask-grid">' + cards + '</div>';

  if (box._html !== html) { box._html = html; box.innerHTML = html; }
}

function bindStructurePanel(box, id) {
  box.querySelectorAll('[data-tier-files]').forEach(btn => {
    btn.addEventListener('click', () =>
      loadTierFiles(id, btn.dataset.tierFiles, btn.dataset.source || null));
  });
}


// ---------------------------------------------------------------------------
// Galeri PREVIEW: PNG hasil render module10 dari tier GOLD, dua jenis
// (grayscale untuk pembacaan ilmiah, colored untuk publikasi). Sumbernya
// /api/datasets/{id}/preview, yang membaca sidecar JSON di disk -- jadi
// keterangan colormap di UI ini persis yang ditulis modul yang me-render-nya,
// bukan salinan kedua yang bisa menyimpang.
// ---------------------------------------------------------------------------

async function renderPreviewGallery(id) {
  const box = document.getElementById('preview-' + id);
  if (!box) return;

  // Panel Struktur digambar ulang tiap polling dataset berjalan. Menggambar
  // dulu dari cache membuat galeri tidak berkedip kosong tiap beberapa detik
  // sementara fetch berikutnya jalan di belakang.
  if (state.previews[id]) drawPreviewGallery(id);

  let data;
  try {
    data = await api('/api/datasets/' + id + '/preview');
  } catch (e) {
    if (!state.previews[id]) box.innerHTML = '';
    return;
  }
  if (!data.scenes || data.scenes.length === 0) {
    delete state.previews[id];
    const emptyHTML = renderPreviewEmpty(id);
    if (box._html !== emptyHTML) { box._html = emptyHTML; box.innerHTML = emptyHTML; }
    return;
  }

  state.previews[id] = data;
  const first = data.scenes[0].scene;
  if (!state.previewScene[id] || !data.scenes.some(s => s.scene === state.previewScene[id])) {
    state.previewScene[id] = first;
  }
  if (!state.previewKind[id]) state.previewKind[id] = 'colored';

  drawPreviewGallery(id);
}

// Label tab galeri per jenis render. Dipetakan eksplisit, bukan lewat
// ternary 'grayscale ? ... : ...': jenis ketiga (composite) sudah ada, dan
// ternary itu akan diam-diam melabelinya "Berwarna".
const PREVIEW_KIND_LABELS = {
  grayscale: 'Grayscale',
  colored: 'Berwarna',
  composite: 'Komposit & Overlay',
};

// Tiga alasan berbeda kenapa galeri bisa kosong, dan ketiganya butuh kalimat
// berbeda -- "belum ada preview" saja membuat user menunggu sesuatu yang tidak
// akan pernah datang kalau sebabnya checkbox yang dimatikan.
function renderPreviewEmpty(id) {
  const ds = state.datasets.find(d => d.dataset_id === id);
  // COG/FUSED, plus nama pra-D14 untuk dataset lama.
  const reachesCog = ds && ds.required_tiers &&
    ['COG', 'FUSED', 'GOLD', 'FUSION'].some(t => ds.required_tiers.includes(t));

  let msg;
  if (ds && ds.generate_preview === false) {
    msg = 'Preview dimatikan untuk dataset ini. Centang "Buat Preview" saat membuat dataset ' +
          'untuk menghasilkannya, atau jalankan ulang render lewat CLI ' +
          'python -m etl.module10_generate_preview.';
  } else if (!reachesCog) {
    msg = 'Preview dirender dari tier COG, sementara dataset ini berhenti sebelum COG. ' +
          'Pilih tier COG atau FUSED untuk mendapatkannya.';
  } else {
    msg = 'Belum ada preview untuk dataset ini. Preview dibuat otomatis setelah tahap ' +
          'COG selesai; dataset yang dibuat sebelum fitur ini ada bisa dirender ulang ' +
          'lewat CLI python -m etl.module10_generate_preview.';
  }

  return '<div class="struct-title preview-title">' +
      '<span class="preview-title-icon">' + ICONS.image + '</span>Preview' +
    '</div>' +
    '<div class="preview-empty">' + escapeHTML(msg) + '</div>';
}

function drawPreviewGallery(id) {
  const box = document.getElementById('preview-' + id);
  const data = state.previews[id];
  if (!box || !data) return;

  const sceneKey = state.previewScene[id];
  const kind = state.previewKind[id];
  const scene = data.scenes.find(s => s.scene === sceneKey) || data.scenes[0];
  // Level pemrosesan jadi dimensi sendiri: satu tanggal bisa punya dua set PNG
  // (RAW dirender dari tier ALIGNED, PROCESSED dari COG) yang isinya berbeda --
  // itu justru alasan preview/{LEVEL}/ ada. Tanpa memilihnya, hanya level
  // default yang pernah terlihat.
  const levels = scene.processing_levels || [];
  const level = levels.includes(state.previewLevel[id])
    ? state.previewLevel[id]
    : (levels[0] || null);
  const levelBlock = (scene.by_level && level && scene.by_level[level]) || null;
  const kindsOf = (levelBlock || scene).kinds || {};
  const block = kindsOf[kind] || { images: [], info: {} };

  // Tiap filter punya baris + label sendiri (Tanggal / Level / Jenis), bukan
  // satu baris pil bercampur: tiga set tombol yang mirip tanpa label membuat
  // user menebak mana yang mengubah apa.
  const filterRow = (label, inner) =>
    '<div class="preview-filter"><span class="preview-filter-label">' + label + '</span>' + inner + '</div>';

  const dateRow = filterRow('Tanggal', data.scenes.length > 1
    ? '<div class="preview-dates">' + data.scenes.map(s =>
        '<button class="preview-date' + (s.scene === scene.scene ? ' active' : '') + '"' +
          ' data-preview-scene="' + escapeHTML(s.scene) + '">' + formatDateKey(s.scene) + '</button>'
      ).join('') + '</div>'
    : '<span class="preview-single-date">' + formatDateKey(scene.scene) + '</span>');

  const levelRow = levels.length > 1
    ? filterRow('Level', '<div class="preview-levels" role="tablist">' + levels.map(l =>
        '<button class="preview-kind' + (l === level ? ' active' : '') + '" role="tab"' +
          ' aria-selected="' + (l === level) + '" data-preview-level="' + l + '">' + l +
        '</button>').join('') + '</div>')
    : '';

  const kindRow = filterRow('Jenis', '<div class="preview-kinds" role="tablist">' +
    data.kinds.map(k =>
      '<button class="preview-kind' + (k === kind ? ' active' : '') + '" role="tab"' +
        ' aria-selected="' + (k === kind) + '" data-preview-kind="' + k + '">' +
        (PREVIEW_KIND_LABELS[k] || k) +
        '<span class="preview-kind-count">' + ((kindsOf[k] || {}).count || 0) + '</span>' +
      '</button>').join('') +
    '</div>');

  const blurb = block.info.purpose
    ? '<p class="preview-blurb">' + escapeHTML(block.info.purpose) + '</p>'
    : '';

  // Kelompokkan per satelit. Komposit RGB lintas-band tapi tetap milik S1,
  // jadi ikut di grupnya sendiri lewat img.source yang sudah dibawa sidecar.
  const groups = {};
  block.images.forEach(img => {
    const src = img.source || String(img.key || '').split('_')[0];
    (groups[src] = groups[src] || []).push(img);
  });
  const groupKeys = Object.keys(groups).sort(
    (a, b) => SOURCE_ORDER_KEYS.indexOf(a) - SOURCE_ORDER_KEYS.indexOf(b)
  );

  // Daftar datar sesuai urutan tampil -- dipakai lightbox untuk navigasi
  // sebelumnya/berikutnya lintas grup satelit.
  const flat = [];
  groupKeys.forEach(src => groups[src].forEach(img => flat.push({ img, src })));

  const closedGroups = state.previewClosed[id] || (state.previewClosed[id] = new Set());

  const cards = block.images.length === 0
    ? '<div class="empty-small">Tidak ada gambar ' + escapeHTML(kind) + ' untuk tanggal ini</div>'
    : groupKeys.map(src =>
        // <details> native: satelit dengan belasan gambar bisa dilipat tanpa
        // mengubah state di luar set `closedGroups` kecil ini.
        '<details class="preview-group" data-preview-group="' + escapeHTML(src) + '"' +
            (closedGroups.has(src) ? '' : ' open') + '>' +
          '<summary class="preview-group-head">' +
            '<span class="struct-swatch" style="background:' + sourceColor(src) + '"></span>' +
            '<span class="preview-group-name">' + escapeHTML(sourceLabel(src)) + '</span>' +
            (level ? '<span class="preview-group-level">' + escapeHTML(level) + '</span>' : '') +
            '<span class="preview-group-count">' + groups[src].length + ' gambar</span>' +
          '</summary>' +
          previewCardsHTML(groups[src], src) +
        '</details>').join('');

  function previewRange(img) {
    return Array.isArray(img.value_range)
      ? img.value_range[0] + ' – ' + img.value_range[1] + (img.units ? ' ' + img.units : '')
      : '';
  }

  function previewCardsHTML(images, src) {
    return '<div class="preview-grid">' + images.map(img => {
        const range = previewRange(img);
        const idx = flat.findIndex(f => f.img === img);
        return '<figure class="preview-card">' +
            // Tombol, bukan <div>: bisa difokus dengan keyboard dan membuka
            // lightbox. Gambar ditampilkan utuh (contain) -- dulu dipotong
            // persegi sehingga citra yang besar/lebar terpotong.
            '<button type="button" class="preview-thumb" data-preview-open="' + idx + '"' +
                ' title="Klik untuk memperbesar">' +
              // loading=lazy + decoding=async: satu dataset bisa punya belasan
              // tanggal x 8 PNG, dan panel ini sering dibuka sekadar untuk
              // melihat angka storage-nya.
              '<img src="' + escapeHTML(img.url) + '" alt="' + escapeHTML(img.label || img.key) + '"' +
                ' loading="lazy" decoding="async">' +
              '<span class="preview-zoom" aria-hidden="true">⤢</span>' +
            '</button>' +
            '<figcaption>' +
              '<span class="preview-label">' + escapeHTML(img.label || img.key) + '</span>' +
              '<span class="preview-tags">' +
                (img.colormap ? '<span class="preview-tag">' + escapeHTML(img.colormap) + '</span>' : '') +
                (range ? '<span class="preview-tag mono">' + escapeHTML(range) + '</span>' : '') +
              '</span>' +
              (img.interpretation
                ? '<span class="preview-note" title="' + escapeHTML(img.interpretation) + '">' +
                    escapeHTML(img.interpretation) + '</span>' : '') +
            '</figcaption>' +
          '</figure>';
    }).join('') + '</div>';
  }

  // Lapisan yang tidak sempat dirender (mis. MODIS/GPM gagal diunduh) tetap
  // disebut: galeri yang diam-diam kekurangan lima gambar akan terbaca
  // sebagai "cuma segini yang ada", bukan "ada yang gagal".
  const missing = (scene.skipped || []).length > 0
    ? '<p class="preview-missing">' + scene.skipped.length + ' lapisan tidak dirender: ' +
        escapeHTML(scene.skipped.map(s => s.key).join(', ')) + '</p>'
    : '';

  const html =
    '<div class="struct-title preview-title">' +
      '<span class="preview-title-icon">' + ICONS.image + '</span>Preview' +
      '<span class="preview-size">' + humanBytes(data.total_size_bytes) + '</span>' +
    '</div>' +
    '<div class="preview-bar">' + dateRow + levelRow + kindRow + '</div>' +
    blurb + cards + missing;
  // Menulis ulang <img loading="lazy"> yang sama membuat gambar kosong sesaat
  // lalu muncul lagi; kalau isinya tidak berubah, biarkan DOM apa adanya.
  if (box._html === html) return;
  box._html = html;
  box.innerHTML = html;

  box.querySelectorAll('[data-preview-scene]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.previewScene[id] = btn.dataset.previewScene;
      drawPreviewGallery(id);
    });
  });
  box.querySelectorAll('[data-preview-level]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.previewLevel[id] = btn.dataset.previewLevel;
      drawPreviewGallery(id);
    });
  });
  box.querySelectorAll('[data-preview-kind]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.previewKind[id] = btn.dataset.previewKind;
      drawPreviewGallery(id);
    });
  });
  box.querySelectorAll('details[data-preview-group]').forEach(d => {
    d.addEventListener('toggle', () => {
      const key = d.dataset.previewGroup;
      if (d.open) closedGroups.delete(key); else closedGroups.add(key);
      // Ingat pilihan lipat di HTML tersimpan, supaya redraw polling yang
      // isinya sama tidak membuka ulang grup yang sudah dilipat user.
      box._html = null;
    });
  });
  box.querySelectorAll('[data-preview-open]').forEach(btn => {
    btn.addEventListener('click', () => {
      openPreviewLightbox(flat.map(f => ({
        url: f.img.url,
        title: f.img.label || f.img.key,
        source: sourceLabel(f.src),
        colormap: f.img.colormap || '',
        range: previewRange(f.img),
        note: f.img.interpretation || '',
      })), Number(btn.dataset.previewOpen));
    });
  });
}

// Lightbox: satu elemen bersama untuk semua dataset, dibuat saat pertama
// dipakai. Gambar preview bisa jauh lebih besar daripada thumbnail-nya, jadi
// user perlu cara melihatnya utuh tanpa membuka tab baru.
let previewLightbox = null;

function openPreviewLightbox(items, index) {
  if (!items.length) return;
  let cur = Math.max(0, Math.min(index, items.length - 1));

  if (!previewLightbox) {
    const el = document.createElement('div');
    el.className = 'lightbox hidden';
    el.setAttribute('role', 'dialog');
    el.setAttribute('aria-modal', 'true');
    el.innerHTML =
      '<div class="lightbox-backdrop" data-lb="close"></div>' +
      '<div class="lightbox-panel">' +
        '<button type="button" class="lightbox-btn lightbox-close" data-lb="close" aria-label="Tutup">✕</button>' +
        '<button type="button" class="lightbox-btn lightbox-nav lightbox-prev" data-lb="prev" aria-label="Sebelumnya">‹</button>' +
        '<div class="lightbox-stage"><img alt=""></div>' +
        '<button type="button" class="lightbox-btn lightbox-nav lightbox-next" data-lb="next" aria-label="Berikutnya">›</button>' +
        '<div class="lightbox-caption"></div>' +
      '</div>';
    document.body.appendChild(el);
    previewLightbox = el;
  }
  const lb = previewLightbox;
  const img = lb.querySelector('.lightbox-stage img');
  const cap = lb.querySelector('.lightbox-caption');

  function show() {
    const it = items[cur];
    img.src = it.url;
    img.alt = it.title;
    cap.innerHTML =
      '<span class="lightbox-title">' + escapeHTML(it.title) + '</span>' +
      '<span class="lightbox-meta">' + escapeHTML(it.source) +
        (it.colormap ? ' · ' + escapeHTML(it.colormap) : '') +
        (it.range ? ' · ' + escapeHTML(it.range) : '') +
        ' · ' + (cur + 1) + '/' + items.length + '</span>' +
      (it.note ? '<span class="lightbox-note">' + escapeHTML(it.note) + '</span>' : '');
    lb.classList.toggle('single', items.length < 2);
  }
  function step(d) { cur = (cur + d + items.length) % items.length; show(); }
  function close() {
    lb.classList.add('hidden');
    document.body.classList.remove('lightbox-open');
    document.removeEventListener('keydown', onKey);
    lb.onclick = null;
    img.removeAttribute('src');
  }
  function onKey(e) {
    if (e.key === 'Escape') close();
    else if (e.key === 'ArrowLeft') step(-1);
    else if (e.key === 'ArrowRight') step(1);
  }
  lb.onclick = e => {
    const t = e.target.closest('[data-lb]');
    if (!t) return;
    if (t.dataset.lb === 'close') close();
    else step(t.dataset.lb === 'next' ? 1 : -1);
  };

  document.addEventListener('keydown', onKey);
  document.body.classList.add('lightbox-open');
  lb.classList.remove('hidden');
  show();
}

function formatDateKey(key) {
  // "20260712" -> "12 Jul 2026". Kunci scene preview selalu YYYYMMDD; kalau
  // suatu saat bukan, tampilkan apa adanya daripada mengarang tanggal.
  if (!/^\d{8}$/.test(key)) return escapeHTML(key);
  const d = new Date(key.slice(0, 4) + '-' + key.slice(4, 6) + '-' + key.slice(6, 8) + 'T00:00:00Z');
  if (isNaN(d)) return escapeHTML(key);
  return d.toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' });
}

// Tata letak: satu kartu per satelit dalam grid responsif. Di dalam kartu,
// tiap laci (tier) satu baris grid berkolom tetap (nama | bar | ukuran, lalu
// info | aksi) supaya angka antar baris sejajar, dikelompokkan per level
// (RAW / PROCESSED) lewat subjudul tipis -- bukan <details> bersarang yang
// dulu membuat indentasi tiga lapis. Kualitas per satelit jadi kaki kartu.
function renderStorageBreakdown(id, storage, quality) {
  if (storage.legacy_layout) {
    return '<div class="struct-title">Struktur penyimpanan</div>'
      + '<div class="empty-small">Dataset ini memakai struktur folder lama dan '
      + 'tidak bisa ditelusuri di sini. Berkasnya tetap utuh di disk. '
      + '<a class="btn-link" href="/api/datasets/' + id + '/download">Unduh semua</a>'
      + '</div>';
  }

  const bySource = {};
  const crossSource = [];
  STORAGE_TIER_ORDER.forEach(tier => {
    const info = storage.tiers[tier.toLowerCase()];
    if (!info || !info.file_count) return;
    const entries = Object.entries(info.sources || {});
    if (!entries.length) { crossSource.push([tier, info]); return; }
    entries.forEach(([src, v]) => {
      const level = TIER_LEVEL[tier.toUpperCase()] || 'RAW';
      bySource[src] = bySource[src] || {};
      (bySource[src][level] = bySource[src][level] || []).push([tier, v]);
    });
  });

  const sourceKeys = SOURCE_ORDER_KEYS.filter(k => bySource[k])
    .concat(Object.keys(bySource).filter(k => !SOURCE_ORDER_KEYS.includes(k)));
  if (!sourceKeys.length && !crossSource.length) {
    return '<div class="struct-title">Struktur penyimpanan</div><div class="empty-small">Belum ada berkas di disk</div>';
  }

  const sum = list => list.reduce((n, [, v]) => n + v.size_bytes, 0);
  const qualBySrc = {};
  ((quality && quality.sources) || []).forEach(q => { qualBySrc[String(q.source).toLowerCase()] = q; });

  function rowHTML(tier, v, src, total, color) {
    const pct = total > 0 ? v.size_bytes / total * 100 : 0;
    const q = '?tier=' + tier.toLowerCase() + (src ? '&source=' + src : '');
    return '<div class="sg-row">' +
      '<span class="sg-name">' + escapeHTML(tier) + '</span>' +
      '<span class="sg-track"><span class="sg-bar" style="width:' + Math.max(pct, 1.5).toFixed(1) + '%;background:' + color + '"></span></span>' +
      '<span class="sg-size">' + humanBytes(v.size_bytes) + '</span>' +
      '<span class="sg-meta">' + v.file_count + ' berkas · ' + v.scene_count + ' scene</span>' +
      '<span class="sg-actions">' +
        '<button class="btn-link" data-tier-files="' + tier.toLowerCase() + '"' + (src ? ' data-source="' + src + '"' : '') + '>Berkas</button>' +
        '<a class="btn-link" href="/api/datasets/' + id + '/download' + q + '">Unduh</a>' +
      '</span>' +
    '</div>';
  }

  function qualityHTML(src) {
    const qd = qualBySrc[src];
    if (!qd) return '';
    const cls = qd.quality_flag === 'GOOD' ? 'ok' : qd.quality_flag === 'POOR' ? 'danger' : 'warn';
    const bands = Object.entries(qd.bands || {})
      .map(([b, v]) => '<span class="qual-band">' + escapeHTML(b) + ' <b>' + v.toFixed(1) + '</b></span>').join('');
    return '<div class="sg-foot">' +
      '<span class="sg-foot-label">Kualitas</span>' +
      '<span class="badge ' + cls + '">' + (qd.quality_score == null ? '-' : qd.quality_score.toFixed(1)) + '</span>' +
      '<span class="qual-kind">' + (qd.kind === 'RADIOMETRIC' ? 'radiometrik' : 'kelengkapan') + '</span>' +
      (bands ? '<span class="qual-bands">' + bands + '</span>' : '') +
    '</div>';
  }

  function cardHTML(title, swatch, total, groups, src) {
    return '<section class="sg-card">' +
      '<header class="sg-head">' +
        '<span class="struct-swatch' + (swatch ? '' : ' struct-swatch-mixed') + '"' + (swatch ? ' style="background:' + swatch + '"' : '') + '></span>' +
        '<span class="sg-title">' + escapeHTML(title) + '</span>' +
        '<span class="sg-total">' + humanBytes(total) + '</span>' +
      '</header>' +
      groups.map(([label, rows]) =>
        (label ? '<div class="sg-level"><span>' + label + '</span><span>' + humanBytes(sum(rows)) + '</span></div>' : '') +
        rows.map(([t, v]) => rowHTML(t, v, src, total, swatch || 'rgba(231,236,245,0.6)')).join('')
      ).join('') +
      (src ? qualityHTML(src) : '') +
    '</section>';
  }

  const cards = sourceKeys.map(src => {
    const levels = bySource[src];
    const groups = ['RAW', 'PROCESSED'].filter(l => levels[l]).map(l => [l, levels[l]]);
    const total = groups.reduce((n, [, rows]) => n + sum(rows), 0);
    return cardHTML(sourceLabel(src), sourceColor(src), total, groups, src);
  });
  if (crossSource.length) {
    cards.push(cardHTML('Lintas-satelit', null, sum(crossSource), [[null, crossSource]], null));
  }

  return '<div class="struct-title">Struktur penyimpanan' +
      '<span class="struct-grand">' + humanBytes(storage.total_size_bytes) + '</span>' +
    '</div>' +
    '<div class="sg-grid">' + cards.join('') + '</div>';
}

// Tanggal YYYYMMDD yang tertanam di kunci scene atau nama berkas. Sejak
// relayout tanggal tidak lagi jadi segmen path, jadi ini satu-satunya cara
// mengelompokkan berkas per tanggal di sisi klien.
function dateFromName(s) {
  const m = /(?:^|[^0-9])(\d{8})(?:[^0-9]|$)/.exec(String(s || ''));
  return m ? m[1] : null;
}

function formatDateKey(k) {
  return k ? k.slice(0, 4) + '-' + k.slice(4, 6) + '-' + k.slice(6, 8) : 'di luar tanggal';
}

async function loadTierFiles(id, tier, source) {
  const box = document.getElementById('structfiles-' + id);
  if (!box) return;
  const key = tier + '/' + (source || '');
  if (box.dataset.tier === key) { box.innerHTML = ''; box.dataset.tier = ''; return; }
  box.dataset.tier = key;
  box.innerHTML = '<div class="empty-small">Memuat berkas…</div>';
  try {
    // Source disaring di server: endpoint sudah menerima ?source=, jadi daun
    // pohon tidak perlu menarik seluruh tier lalu membuang sebagian besarnya.
    const url = '/api/datasets/' + id + '/storage/files/' + tier
      + (source ? '?source=' + encodeURIComponent(source) : '');
    const data = await api(url);
    if (data.scenes.length === 0) {
      box.innerHTML = '<div class="empty-small">Laci ini kosong</div>';
      return;
    }

    // Kelompokkan per tanggal. Source dan tier sudah tetap dari daun yang
    // diklik, jadi kolomnya diganti Tanggal + Scene.
    const byDate = {};
    data.scenes.forEach(sc => {
      sc.files.forEach(f => {
        const d = dateFromName(f.name) || dateFromName(sc.scene);
        (byDate[d || ''] = byDate[d || ''] || []).push([sc, f]);
      });
    });

    const dates = Object.keys(byDate).sort();
    box.innerHTML = '<div class="sg-files-head"><span>Berkas ' + escapeHTML(tier.toUpperCase()) +
      (source ? ' · ' + escapeHTML(sourceLabel(source)) : '') + '</span>' +
      '<button class="btn-link" data-close-files>Tutup</button></div>' +
      '<table class="scene-table"><thead><tr>' +
      '<th>Tanggal</th><th>Scene</th><th>Berkas</th><th>Ukuran</th>' +
      '</tr></thead><tbody>' +
      dates.map(d => byDate[d].map(([sc, f], i) =>
        '<tr>' +
          '<td class="mono small">' + (i === 0 ? escapeHTML(formatDateKey(d)) : '') + '</td>' +
          '<td class="mono small">' + escapeHTML(shortenSceneId(sc.scene)) + '</td>' +
          '<td class="mono small">' + escapeHTML(f.name) + '</td>' +
          '<td>' + f.size_mb.toFixed(1) + ' MB</td>' +
        '</tr>').join('')).join('') +
      '</tbody></table>';
    box.querySelector('[data-close-files]').addEventListener('click', () => { box.innerHTML = ''; box.dataset.tier = ''; });
    box.scrollIntoView({ behavior: 'smooth', block: 'nearest' });
  } catch (e) {
    box.innerHTML = '<div class="empty-small">' + escapeHTML(e.message) + '</div>';
  }
}


// ---------------------------------------------------------------------------
// Penggabungan dataset (/api/merge).
//
// Kenapa ini ada: bbox itu persegi panjang, pulau tidak. AOI sebesar Jawa
// karena itu dipecah jadi beberapa strip supaya porsi lautnya turun
// (DOCS/DECISIONS.md D16) -- dan pemecahan itu menyisakan N stack terpisah per
// tanggal, bukan satu. Panel ini yang menutup lingkarannya.
//
// Aturan yang membentuk UI-nya:
//
// MENAWARKAN, BUKAN MENJALANKAN SENDIRI. Penggabungan menulis berkas besar
// dan lama. Yang tahu apakah empat strip itu memang satu pulau yang sama
// adalah peneliti, bukan kode. Jadi panel ini hanya muncul dan menunggu;
// tidak ada jalur yang menggabungkan tanpa user menekan tombolnya.
//
// Hanya tanggal yang MEMANG BISA digabung yang ditampilkan di sini. Kandidat
// yang terhalang (grid tidak sejajar, dsb.) disaring keluar -- panel ini
// adalah daftar aksi yang bisa diambil, bukan laporan status tiap tanggal.
// ---------------------------------------------------------------------------

async function renderMergePanel() {
  const box = document.getElementById('mergePanel');
  if (!box) return;

  let data;
  try {
    data = await api('/api/merge/candidates');
  } catch (e) {
    box.classList.add('hidden');
    return;
  }

  const mergeableCandidates = (data.candidates || []).filter(c => c.mergeable);

  if (mergeableCandidates.length === 0) {
    // Tidak ada yang bisa digabung adalah keadaan normal (dataset tunggal,
    // strip yang fusion-nya belum jadi, atau semuanya terhalang) -- jangan
    // ributkan.
    box.classList.add('hidden');
    box.innerHTML = '';
    return;
  }

  const rows = mergeableCandidates.map(c => {
    const names = c.dataset_names.map(escapeHTML).join(' + ');
    const shape = c.output_shape[0] && c.output_shape[1]
      ? c.output_shape[1].toLocaleString('id-ID') + ' x ' +
        c.output_shape[0].toLocaleString('id-ID') + ' px'
      : '-';

    let action;
    if (c.already_merged) {
      // Berkas yang digabung sebelum preview ada tidak punya PNG. Merender
      // ulang dari HDF5-nya jauh lebih murah daripada menyuruh user menggabung
      // ulang belasan GB hanya untuk mendapat gambarnya.
      const needsPreview = !(c.preview_images || []).length;
      action =
        '<span class="merge-done">Sudah digabung · ' +
          humanBytes(c.output_size_bytes || 0) + '</span>' +
        (needsPreview
          ? '<button class="btn btn-ghost btn-sm" data-preview-date="' +
              escapeHTML(c.date) + '">Buat Preview</button>'
          : '') +
        '<button class="btn btn-ghost btn-sm" data-merge-date="' +
          escapeHTML(c.date) + '" data-merge-ids="' + c.dataset_ids.join(',') +
          '" data-merge-overwrite="1">Gabung Ulang</button>' +
        '<button class="btn btn-ghost btn-sm btn-danger" data-delete-date="' +
          escapeHTML(c.date) + '" data-delete-size="' +
          (c.output_size_bytes || 0) + '">Hapus</button>';
    } else {
      action =
        '<button class="btn btn-accent btn-sm" data-merge-date="' +
          escapeHTML(c.date) + '" data-merge-ids="' + c.dataset_ids.join(',') +
          '">Gabungkan</button>';
    }

    const notes = [];
    (c.warnings || []).forEach(w => {
      notes.push('<p class="merge-warn">' + escapeHTML(w) + '</p>');
    });

    // Preview cuma ada setelah digabung. Thumbnail-nya dibuka di tab baru
    // dalam ukuran penuh -- di baris panel ini lebarnya cuma beberapa ratus
    // piksel, terlalu kecil untuk memeriksa sambungan antar-strip.
    const shots = (c.preview_images || []).map(img =>
      '<a class="merge-shot" href="' + escapeHTML(img.url) + '" target="_blank" ' +
        'rel="noopener" title="' + escapeHTML(img.file) + '">' +
        '<img src="' + escapeHTML(img.url) + '" loading="lazy" ' +
          'alt="' + escapeHTML(img.file) + '">' +
        '<span>' + escapeHTML(img.file.replace(/\.png$/, '')) + '</span>' +
      '</a>'
    ).join('');

    return (
      '<div class="merge-row">' +
        '<div class="merge-row-main">' +
          '<div class="merge-date">' + escapeHTML(formatDateKey(c.date)) + '</div>' +
          '<div class="merge-sources">' + names + '</div>' +
          '<div class="merge-facts">' +
            '<span>' + c.stack_count + ' stack</span>' +
            '<span>' + shape + '</span>' +
            '<span>' + c.layers.length + ' lapisan</span>' +
            '<span>' + humanBytes(c.input_bytes) + '</span>' +
          '</div>' +
        '</div>' +
        '<div class="merge-row-action">' + action + '</div>' +
        (notes.length ? '<div class="merge-notes">' + notes.join('') + '</div>' : '') +
        (shots ? '<div class="merge-shots">' + shots + '</div>' : '') +
      '</div>'
    );
  }).join('');

  const collapsed = state.mergeCollapsed;
  const html =
    '<div class="merge-head">' +
      '<h4>Gabungkan Dataset</h4>' +
      '<span class="merge-sub">' + mergeableCandidates.length +
        ' tanggal siap digabung</span>' +
      '<button type="button" class="card-collapse-btn' + (collapsed ? ' collapsed' : '') +
        '" data-action="toggle-merge" title="' + (collapsed ? 'Buka panel' : 'Tutup panel') +
        '" aria-label="' + (collapsed ? 'Buka panel' : 'Tutup panel') + '"><span class="chevron"></span></button>' +
    '</div>' +
    '<div class="merge-body' + (collapsed ? ' hidden' : '') + '">' +
      '<p class="merge-note">' + escapeHTML(data.explanation) + '</p>' +
      '<div class="merge-rows">' + rows + '</div>' +
    '</div>';

  if (box._html !== html) {
    box._html = html;
    box.innerHTML = html;
    bindMergePanel(box);
  }
  box.classList.remove('hidden');
}

function formatDateKey(key) {
  if (!key || key.length !== 8) return key || '-';
  return key.slice(6, 8) + '/' + key.slice(4, 6) + '/' + key.slice(0, 4);
}

function bindMergePanel(box) {
  box.querySelectorAll('[data-merge-date]').forEach(btn => {
    btn.addEventListener('click', () => runMerge(btn));
  });
  box.querySelectorAll('[data-preview-date]').forEach(btn => {
    btn.addEventListener('click', () => buildMergePreview(btn));
  });
  box.querySelectorAll('[data-delete-date]').forEach(btn => {
    btn.addEventListener('click', () => deleteMergeResult(btn));
  });
  const toggle = box.querySelector('[data-action="toggle-merge"]');
  if (toggle) toggle.addEventListener('click', () => {
    state.mergeCollapsed = !state.mergeCollapsed;
    box._html = null;
    renderMergePanel();
  });
}

async function deleteMergeResult(btn) {
  const date = btn.dataset.deleteDate;
  const size = Number(btn.dataset.deleteSize) || 0;

  // Selalu dikonfirmasi: berkasnya berukuran giga dan penghapusannya tidak bisa
  // dibatalkan. Yang hilang cuma turunan -- stack sumbernya utuh, jadi tanggal
  // ini bisa digabung ulang -- dan kalimatnya mengatakan itu supaya user tidak
  // mengira sedang membuang hasil pemrosesan.
  if (!window.confirm(
      'Hapus hasil gabungan ' + formatDateKey(date) + ' (' + humanBytes(size) +
      ') beserta preview-nya?\n\n' +
      'Stack fusion sumbernya tidak dihapus, jadi tanggal ini bisa digabung ulang.')) {
    return;
  }

  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Menghapus...';
  try {
    const result = await api('/api/merge/result/' + encodeURIComponent(date),
                             { method: 'DELETE' });
    showToast(result.removed.join(', ') + ' dihapus · ' +
              humanBytes(result.freed_bytes) + ' dibebaskan', 'success');
    box_refreshMerge();
  } catch (err) {
    showToast(err.message, 'error');
    btn.disabled = false;
    btn.textContent = original;
  }
}

async function buildMergePreview(btn) {
  const date = btn.dataset.previewDate;
  const original = btn.textContent;
  btn.disabled = true;
  // Seluruh isi HDF5 didekompresi, jadi ini menit-menitan untuk AOI sebesar
  // Jawa. Tombolnya mengatakan itu, supaya tidak dikira menggantung.
  btn.textContent = 'Merender... (beberapa menit)';
  try {
    const result = await api(
      '/api/merge/preview/' + encodeURIComponent(date) + '/rebuild',
      { method: 'POST' });
    showToast(result.preview_images.length + ' gambar dibuat untuk ' +
              formatDateKey(date), 'success');
    box_refreshMerge();
  } catch (err) {
    showToast(err.message, 'error');
    btn.disabled = false;
    btn.textContent = original;
  }
}

async function runMerge(btn) {
  const date = btn.dataset.mergeDate;
  const ids = btn.dataset.mergeIds.split(',').map(Number);
  const overwrite = btn.dataset.mergeOverwrite === '1';

  // Konfirmasi eksplisit untuk gabung ulang: itu menimpa berkas yang sudah
  // ada. Penggabungan pertama tidak perlu ditanya -- tidak ada yang hilang.
  if (overwrite && !window.confirm(
      'Berkas gabungan untuk ' + formatDateKey(date) +
      ' sudah ada dan akan ditimpa. Lanjutkan?')) {
    return;
  }

  const original = btn.textContent;
  btn.disabled = true;
  btn.textContent = 'Menggabungkan...';
  try {
    const result = await api('/api/merge/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: date, dataset_ids: ids, overwrite: overwrite }),
    });
    showToast(
      result.output_name + ' dibuat · ' + humanBytes(result.output_size_bytes) +
      ' · sumber tidak diubah', 'success');
    box_refreshMerge();
  } catch (err) {
    showToast(err.message, 'error');
    btn.disabled = false;
    btn.textContent = original;
  }
}

function box_refreshMerge() {
  // Panel dibangun ulang supaya baris yang baru digabung berubah jadi
  // "Sudah digabung" tanpa user perlu menyegarkan halaman.
  const box = document.getElementById('mergePanel');
  if (box) box._html = null;
  renderMergePanel();
}
