// Urutan eksekusi tahap per scene. PREVIEW harus ikut walau bukan tier
// lineage: tierCompletionRatios mencocokkan current_stage lewat indexOf di
// sini, dan stage yang tidak terdaftar mengembalikan -1 -- membuat scene
// yang sedang di tahap PREVIEW terbaca belum mencapai GOLD dan ring progres
// mundur sesaat tiap scene melewatinya.
const STAGE_ORDER = ['DOWNLOAD','CROP','LEE_FILTER','QUALITY_ANALYTICS','GOLD_EXPORT','PREVIEW','FUSION','CLEANUP'];
const TIER_ORDER = ['RAW', 'BRONZE', 'SILVER', 'GOLD', 'FUSION'];
const TIER_STAGE = { RAW: 'DOWNLOAD', BRONZE: 'CROP', SILVER: 'LEE_FILTER', GOLD: 'GOLD_EXPORT', FUSION: 'FUSION' };
// Palet tier: lima hue kategorikal, divalidasi terhadap permukaan gelap
// #121A2B (lightness band, chroma floor, pemisahan CVD pasangan bersebelahan,
// dan kontras). SILVER dulu #9FB0C9 yang chroma-nya di bawah ambang (terbaca
// abu-abu) dan cuma berjarak dE 12 dari GOLD -- dua tier bersebelahan yang
// sulit dibedakan bahkan dengan penglihatan warna normal.
const TIER_COLORS = { RAW: '#9070E8', BRONZE: '#C4762E', SILVER: '#4A8CE0', GOLD: '#2FA07E', FUSION: '#B565D8' };

// Palet source untuk panel Struktur. Sengaja jadi satu-satunya dimensi warna
// di panel itu -- tier di sana ditandai teks, bukan warna -- supaya satu hue
// tidak pernah berarti dua hal dalam satu komponen.
const SOURCE_COLORS = { sentinel1: '#5B8DEF', modis: '#2FA07E', gpm: '#C4762E' };
const SOURCE_LABELS = { sentinel1: 'Sentinel-1', modis: 'MODIS', gpm: 'GPM', fusion: 'Fusion', preview: 'Preview' };

// Tier yang punya folder di disk, untuk rincian storage. Beda dari TIER_ORDER
// di atas: itu rantai lineage yang bisa diminta user dan digambar di ring
// progres, sementara PREVIEW adalah turunan (PNG hasil render dari GOLD) yang
// tidak pernah ada di required_tiers tapi tetap memakan disk dan tetap harus
// muncul di rincian. Urutannya mengikuti urutan eksekusi pipeline.
const STORAGE_TIER_ORDER = ['RAW', 'BRONZE', 'SILVER', 'GOLD', 'PREVIEW', 'FUSION'];

// Tier lintas-source: tidak bisa dipecah per sensor, jadi dikeluarkan dari
// legenda source supaya tidak terbaca sebagai sensor keempat.
const SOURCELESS_TIERS = ['fusion', 'preview'];
const ACTIVE_STATUSES = new Set(['QUEUED','PREPARING','DOWNLOADING','PROCESSING','PAUSED','CLEANUP','DELETING']);

const state = {
  datasets: [], progress: {}, logs: {}, pollTimer: null, livePollTimer: null,
  openScenes: new Set(), openStructure: new Set(),
  // Galeri preview per dataset: payload /api/datasets/{id}/preview, plus
  // tanggal dan jenis yang sedang dipilih (bertahan saat panel digambar ulang
  // oleh polling).
  previews: {}, previewScene: {}, previewKind: {},
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

function buildRingSVG(ratios, size) {
  size = size || 96;
  const tiers = TIER_ORDER;
  const cx = size / 2, cy = size / 2;
  const baseR = size * 0.12;
  const step = size * 0.09;
  let circles = '';
  tiers.forEach((t, i) => {
    const r = baseR + step * i;
    const circumference = 2 * Math.PI * r;
    const ratio = ratios[t] || 0;
    const dash = circumference * ratio;
    circles += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + TIER_COLORS[t] + '" stroke-opacity="0.16" stroke-width="4"></circle>';
    circles += '<circle cx="' + cx + '" cy="' + cy + '" r="' + r + '" fill="none" stroke="' + TIER_COLORS[t] + '" stroke-width="4" stroke-linecap="round" stroke-dasharray="' + dash + ' ' + (circumference - dash) + '" transform="rotate(-90 ' + cx + ' ' + cy + ')"></circle>';
  });
  return '<svg viewBox="0 0 ' + size + ' ' + size + '" width="' + size + '" height="' + size + '">' + circles + '</svg>';
}

function tierCompletionRatios(scenes, requiredTiers) {
  const tiers = TIER_ORDER;
  const ratios = {};
  const total = scenes.length;
  tiers.forEach(t => {
    if (!requiredTiers.includes(t)) { ratios[t] = 0; return; }
    if (total === 0) { ratios[t] = 0; return; }
    const stageIdx = STAGE_ORDER.indexOf(TIER_STAGE[t]);
    let reached = 0;
    scenes.forEach(s => {
      const curIdx = STAGE_ORDER.indexOf(s.current_stage);
      if (curIdx > stageIdx) reached++;
      else if (curIdx === stageIdx && s.stage_status === 'COMPLETED') reached++;
    });
    ratios[t] = reached / total;
  });
  return ratios;
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

document.getElementById('tierAll').addEventListener('change', (e) => {
  document.querySelectorAll('.tier-check').forEach(c => c.checked = e.target.checked);
});

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
}

function clearRegionSelection() {
  state.selectedRegionId = null;
  document.querySelectorAll('.region-card').forEach(c => c.classList.remove('selected'));
  clearMapPreview();
}

async function loadRegions(options) {
  const opts = options || {};
  const grid = document.getElementById('regionGrid');
  try {
    const q = state.locationQuery.trim();
    const result = await api('/api/regions' + (q ? '?q=' + encodeURIComponent(q) : ''));
    state.regions = result.items;
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
document.getElementById('previewOptIcon').innerHTML = ICONS.image;
loadRegions();

document.getElementById('createForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const tiers = Array.from(document.querySelectorAll('.tier-check:checked')).map(c => c.value);
  if (tiers.length === 0) { showToast('Pilih minimal satu tier data', 'error'); return; }
  const regionId = state.selectedRegionId;
  const dateStart = document.getElementById('fDateStart').value;
  const dateEnd = document.getElementById('fDateEnd').value;
  const name = document.getElementById('fName').value.trim();
  if (!regionId) { showToast('Pilih dulu lokasi dari daftar', 'error'); return; }
  if (!dateStart || !dateEnd || !name) { showToast('Lengkapi tanggal dan nama dataset', 'error'); return; }
  const qs = {};
  const cloud = document.getElementById('fMinCloud').value;
  const qual = document.getElementById('fMinQuality').value;
  const res = document.getElementById('fResolution').value;
  if (cloud !== '') qs.min_cloud_cover = Number(cloud);
  if (qual !== '') qs.min_quality_score = Number(qual);
  if (res !== '') qs.resolution_m = Number(res);
  const body = {
    region_id: regionId, date_start: dateStart, date_end: dateEnd, tiers: tiers, name: name,
    description: document.getElementById('fDescription').value.trim() || null,
    quality_settings: Object.keys(qs).length ? qs : null,
    // Field tersendiri, bukan bagian quality_settings: ini sakelar tahap
    // pipeline, bukan ambang mutu data.
    generate_preview: document.getElementById('fGeneratePreview').checked,
  };
  const submitBtn = document.getElementById('createSubmit');
  submitBtn.disabled = true; submitBtn.textContent = 'Membuat...';
  try {
    const result = await api('/api/datasets', { method: 'POST', body: JSON.stringify(body) });
    showToast('Dataset dibuat (status: ' + result.status + ')', 'success');
    e.target.reset();
    document.querySelectorAll('.tier-check').forEach(c => c.checked = c.value === 'FUSION');
    // reset() sudah mengembalikannya ke atribut `checked` di HTML; ditulis
    // ulang di sini supaya default-nya tidak diam-diam berubah kalau markup-nya
    // suatu saat diedit.
    document.getElementById('fGeneratePreview').checked = true;
    clearRegionSelection();
    switchTab('datasets');
  } catch (err) {
    showToast(err.message, 'error');
  } finally {
    submitBtn.disabled = false; submitBtn.textContent = 'Buat Dataset';
  }
});

async function loadDatasets() {
  try {
    const result = await api('/api/datasets?limit=50');
    state.datasets = result.items;
    await refreshProgress();
  } catch (err) { showToast(err.message, 'error'); }
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
  }
  renderDatasets();
}
function startDatasetPolling() {
  stopDatasetPolling();
  state.pollTimer = setInterval(async () => { await loadDatasetsQuiet(); await refreshProgress(); }, 2000);
}
function stopDatasetPolling() { if (state.pollTimer) clearInterval(state.pollTimer); state.pollTimer = null; }
document.getElementById('refreshDatasets').addEventListener('click', loadDatasets);

function renderDatasets() {
  const container = document.getElementById('datasetList');
  if (state.datasets.length === 0) {
    container.innerHTML = '<div class="empty">Belum ada dataset. Buat satu di tab Buat Dataset.</div>';
    return;
  }
  container.innerHTML = '';
  state.datasets.forEach(ds => container.appendChild(renderDatasetCard(ds)));
}

function renderDatasetCard(ds) {
  const el = document.createElement('div');
  el.className = 'card';
  const prog = state.progress[ds.dataset_id];
  const scenes = prog ? prog.scenes : [];
  const ratios = tierCompletionRatios(scenes, ds.required_tiers);
  const ringHTML = buildRingSVG(ratios);
  const statusClass = statusToClass(ds.status);
  const canPause = ['QUEUED', 'PREPARING', 'DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canResume = ds.status === 'PAUSED';
  const canRetry = ds.status === 'FAILED';
  const canCancel = ['DOWNLOADING', 'PROCESSING'].includes(ds.status);
  const canDownload = ds.total_size_bytes > 0;
  const spinning = ACTIVE_STATUSES.has(ds.status) && ds.status !== 'PAUSED';
  el.innerHTML =
    '<div class="card-head">' +
      '<div class="card-ring' + (spinning ? ' spinning' : '') + '">' + ringHTML + '</div>' +
      '<div class="card-info">' +
        '<div class="card-title-row">' +
          '<span class="card-name">' + escapeHTML(ds.name) + '</span>' +
          '<span class="badge ' + statusClass + '">' + ds.status + '</span>' +
        '</div>' +
        '<div class="card-meta">' + escapeHTML(ds.location_label || '-') + ' &middot; ' + ds.date_start + ' - ' + ds.date_end + '</div>' +
        '<div class="card-tiers">' + ds.required_tiers.map(t => '<span class="chip" style="--chip-color:' + TIER_COLORS[t] + '">' + t + '</span>').join('') + '</div>' +
      '</div>' +
    '</div>' +
    '<div class="card-stats">' +
      '<div><span class="stat-num">' + ds.total_scenes + '</span><span class="stat-label">scene</span></div>' +
      '<div><span class="stat-num">' + ds.completed_scenes + '</span><span class="stat-label">selesai</span></div>' +
      '<div><span class="stat-num">' + ds.failed_scenes + '</span><span class="stat-label">gagal</span></div>' +
      '<div><span class="stat-num">' + humanBytes(ds.total_size_bytes) + '</span><span class="stat-label">ukuran</span></div>' +
    '</div>' +
    renderLogPanel(ds.dataset_id) +
    '<div class="card-actions">' +
      (canPause ? '<button class="btn btn-warn" data-action="pause">Jeda</button>' : '') +
      (canResume ? '<button class="btn btn-accent" data-action="resume">Lanjutkan</button>' : '') +
      (canRetry ? '<button class="btn btn-accent" data-action="retry">Coba lagi</button>' : '') +
      (canCancel ? '<button class="btn btn-danger" data-action="cancel">Batalkan</button>' : '') +
      (canDownload ? '<a class="btn btn-ghost" href="/api/datasets/' + ds.dataset_id + '/download">Unduh</a>' : '') +
      '<button class="btn btn-danger" data-action="delete">Hapus</button>' +
      '<button class="btn btn-ghost" data-action="toggle-scenes">Detail</button>' +
      (canDownload ? '<button class="btn btn-ghost" data-action="toggle-structure">Struktur</button>' : '') +
    '</div>' +
    '<div class="card-scenes' + (state.openScenes.has(ds.dataset_id) ? '' : ' hidden') + '" id="scenes-' + ds.dataset_id + '"></div>' +
    '<div class="card-structure' + (state.openStructure.has(ds.dataset_id) ? '' : ' hidden') + '" id="structure-' + ds.dataset_id + '"></div>';
  el.querySelectorAll('[data-action]').forEach(btn => btn.addEventListener('click', () => handleCardAction(btn.dataset.action, ds.dataset_id)));
  if (state.openScenes.has(ds.dataset_id)) renderSceneTable(el.querySelector('.card-scenes'), ds.dataset_id);
  if (state.openStructure.has(ds.dataset_id)) renderStructurePanel(el.querySelector('.card-structure'), ds.dataset_id);
  return el;
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
  if (status === 'RUNNING') return 'log-progress';
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

function renderSceneTable(box, id) {
  const prog = state.progress[id];
  if (!prog || prog.scenes.length === 0) { box.innerHTML = '<div class="empty-small">Belum ada scene</div>'; return; }
  box.innerHTML = '<table class="scene-table"><thead><tr><th>Scene</th><th>Tahap</th><th>Status</th><th>Catatan</th></tr></thead><tbody>' +
    prog.scenes.map(s => '<tr><td class="mono">' + escapeHTML(s.product_identifier) + '</td><td>' + (s.current_stage || '-') + '</td><td><span class="badge ' + statusToClass(s.stage_status) + '">' + s.stage_status + '</span></td><td class="mono small">' + escapeHTML(s.last_error || '') + '</td></tr>').join('') +
    '</tbody></table>';
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

async function loadLive() {
  try {
    const live = await api('/api/live');
    renderLive(live);
  } catch (err) {
    document.getElementById('sourceList').innerHTML = '<div class="empty-small">' + escapeHTML(err.message) + '</div>';
  }
}
function startLivePolling() { stopLivePolling(); state.livePollTimer = setInterval(loadLive, 5000); }
function stopLivePolling() { if (state.livePollTimer) clearInterval(state.livePollTimer); state.livePollTimer = null; }

function renderLive(live) {
  document.getElementById('liveToggle').checked = live.enabled;
  document.getElementById('liveStatusText').textContent = live.status;
  document.getElementById('liveSize').textContent = humanBytes(live.total_size_bytes);
  document.getElementById('liveChecked').textContent = live.last_checked_at ? new Date(live.last_checked_at).toLocaleString('id-ID') : 'Belum pernah';
  document.getElementById('liveDownload').href = '/api/datasets/' + live.dataset_id + '/download';
  const src = document.getElementById('sourceList');
  src.innerHTML = live.sources.map(s =>
    '<div class="source-row">' +
      '<span class="dot ' + (s.enabled ? 'ok' : 'muted') + '"></span>' +
      '<span class="source-name">' + s.source_name + '</span>' +
      '<span class="source-meta">cek: ' + (s.last_check ? new Date(s.last_check).toLocaleString('id-ID') : '-') + '</span>' +
      '<span class="source-meta">ambil: ' + (s.last_ingest ? new Date(s.last_ingest).toLocaleString('id-ID') : '-') + '</span>' +
    '</div>'
  ).join('');
  loadLiveScenes();
}

async function loadLiveScenes() {
  try {
    const scenes = await api('/api/live/scenes?limit=20');
    const box = document.getElementById('liveScenes');
    if (scenes.length === 0) { box.innerHTML = '<div class="empty-small">Belum ada data</div>'; return; }
    box.innerHTML = '<table class="scene-table"><thead><tr><th>Tanggal</th><th>Tier</th><th>Ukuran</th></tr></thead><tbody>' +
      scenes.map(s => '<tr><td>' + new Date(s.scene_date).toLocaleString('id-ID') + '</td><td><span class="chip" style="--chip-color:' + (TIER_COLORS[s.tier] || '#35D0C0') + '">' + s.tier + '</span></td><td>' + s.size_mb.toFixed(1) + ' MB</td></tr>').join('') +
      '</tbody></table>';
  } catch (e) {}
}

document.getElementById('liveToggle').addEventListener('change', async (e) => {
  try { await api('/api/live/toggle', { method: 'POST', body: JSON.stringify({ enabled: e.target.checked }) }); showToast(e.target.checked ? 'Live diaktifkan' : 'Live dinonaktifkan', 'success'); }
  catch (err) { showToast(err.message, 'error'); e.target.checked = !e.target.checked; }
});

document.getElementById('liveClearBtn').addEventListener('click', () => document.getElementById('clearModal').classList.remove('hidden'));
document.getElementById('clearCancel').addEventListener('click', () => document.getElementById('clearModal').classList.add('hidden'));
document.getElementById('clearConfirm').addEventListener('click', async () => {
  try { const r = await api('/api/live/clear', { method: 'POST' }); showToast('Dikosongkan: ' + r.deleted_count + ' file', 'success'); loadLive(); }
  catch (err) { showToast(err.message, 'error'); }
  document.getElementById('clearModal').classList.add('hidden');
});

document.getElementById('backfillForm').addEventListener('submit', async (e) => {
  e.preventDefault();
  const ds = document.getElementById('bfStart').value;
  const de = document.getElementById('bfEnd').value;
  if (!ds || !de) { showToast('Isi kedua tanggal', 'error'); return; }
  try {
    const r = await api('/api/live/backfill', { method: 'POST', body: JSON.stringify({ date_start: ds, date_end: de }) });
    showToast('Backfill dimulai (job ' + r.job_id + ')', 'success');
    e.target.reset();
  } catch (err) { showToast(err.message, 'error'); }
});

document.addEventListener('keydown', (e) => {
  if (e.key === 'Escape') {
    document.getElementById('deleteModal').classList.add('hidden');
    document.getElementById('cancelModal').classList.add('hidden');
    document.getElementById('clearModal').classList.add('hidden');
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
  box.innerHTML = '<div class="empty-small">Memuat struktur…</div>';

  let storage;
  try {
    storage = await api('/api/datasets/' + id + '/storage/summary');
  } catch (e) {
    box.innerHTML = '<div class="empty-small">' + escapeHTML(e.message) + '</div>';
    return;
  }

  // Kualitas opsional: dataset yang belum sempat lewat tahap analitik tetap
  // harus bisa menampilkan rincian storage-nya.
  let quality = { sources: [] };
  try { quality = await api('/api/quality/dataset/' + id + '/by-source'); } catch (e) {}

  box.innerHTML =
    renderStorageBreakdown(id, storage) +
    renderQualityBySource(quality) +
    '<div class="struct-files" id="structfiles-' + id + '"></div>' +
    '<div class="preview-section" id="preview-' + id + '"></div>';

  box.querySelectorAll('[data-tier-files]').forEach(btn => {
    btn.addEventListener('click', () => loadTierFiles(id, btn.dataset.tierFiles));
  });

  // Galeri di-fetch terpisah dan tidak di-await: rincian storage sudah bisa
  // dibaca sementara daftar preview masih jalan, dan dataset yang tier
  // preview-nya kosong tidak menahan apa pun.
  renderPreviewGallery(id);
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
    box.innerHTML = renderPreviewEmpty(id);
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

// Tiga alasan berbeda kenapa galeri bisa kosong, dan ketiganya butuh kalimat
// berbeda -- "belum ada preview" saja membuat user menunggu sesuatu yang tidak
// akan pernah datang kalau sebabnya checkbox yang dimatikan.
function renderPreviewEmpty(id) {
  const ds = state.datasets.find(d => d.dataset_id === id);
  const reachesGold = ds && ds.required_tiers &&
    (ds.required_tiers.includes('GOLD') || ds.required_tiers.includes('FUSION'));

  let msg;
  if (ds && ds.generate_preview === false) {
    msg = 'Preview dimatikan untuk dataset ini. Centang "Buat Preview" saat membuat dataset ' +
          'untuk menghasilkannya, atau jalankan ulang render lewat CLI ' +
          'python -m etl.module10_generate_preview.';
  } else if (!reachesGold) {
    msg = 'Preview dirender dari tier GOLD, sementara dataset ini berhenti sebelum GOLD. ' +
          'Pilih tier GOLD atau FUSION untuk mendapatkannya.';
  } else {
    msg = 'Belum ada preview untuk dataset ini. Preview dibuat otomatis setelah tahap ' +
          'GOLD selesai; dataset yang dibuat sebelum fitur ini ada bisa dirender ulang ' +
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
  const block = scene.kinds[kind] || { images: [], info: {} };

  const dateTabs = data.scenes.length > 1
    ? '<div class="preview-dates">' + data.scenes.map(s =>
        '<button class="preview-date' + (s.scene === scene.scene ? ' active' : '') + '"' +
          ' data-preview-scene="' + escapeHTML(s.scene) + '">' + formatDateKey(s.scene) + '</button>'
      ).join('') + '</div>'
    : '<span class="preview-single-date">' + formatDateKey(scene.scene) + '</span>';

  const kindTabs = '<div class="preview-kinds" role="tablist">' +
    data.kinds.map(k =>
      '<button class="preview-kind' + (k === kind ? ' active' : '') + '" role="tab"' +
        ' aria-selected="' + (k === kind) + '" data-preview-kind="' + k + '">' +
        (k === 'grayscale' ? 'Grayscale' : 'Berwarna') +
        '<span class="preview-kind-count">' + ((scene.kinds[k] || {}).count || 0) + '</span>' +
      '</button>').join('') +
    '</div>';

  const blurb = block.info.purpose
    ? '<p class="preview-blurb">' + escapeHTML(block.info.purpose) + '</p>'
    : '';

  const cards = block.images.length === 0
    ? '<div class="empty-small">Tidak ada gambar ' + escapeHTML(kind) + ' untuk tanggal ini</div>'
    : '<div class="preview-grid">' + block.images.map(img => {
        const range = Array.isArray(img.value_range)
          ? img.value_range[0] + ' – ' + img.value_range[1] + (img.units ? ' ' + img.units : '')
          : '';
        return '<figure class="preview-card">' +
            '<div class="preview-thumb">' +
              // loading=lazy + decoding=async: satu dataset bisa punya belasan
              // tanggal x 8 PNG, dan panel ini sering dibuka sekadar untuk
              // melihat angka storage-nya.
              '<img src="' + escapeHTML(img.url) + '" alt="' + escapeHTML(img.label || img.key) + '"' +
                ' loading="lazy" decoding="async">' +
            '</div>' +
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

  // Lapisan yang tidak sempat dirender (mis. MODIS/GPM gagal diunduh) tetap
  // disebut: galeri yang diam-diam kekurangan lima gambar akan terbaca
  // sebagai "cuma segini yang ada", bukan "ada yang gagal".
  const missing = (scene.skipped || []).length > 0
    ? '<p class="preview-missing">' + scene.skipped.length + ' lapisan tidak dirender: ' +
        escapeHTML(scene.skipped.map(s => s.key).join(', ')) + '</p>'
    : '';

  box.innerHTML =
    '<div class="struct-title preview-title">' +
      '<span class="preview-title-icon">' + ICONS.image + '</span>Preview' +
      '<span class="preview-size">' + humanBytes(data.total_size_bytes) + '</span>' +
    '</div>' +
    '<div class="preview-bar">' + dateTabs + kindTabs + '</div>' +
    blurb + cards + missing;

  box.querySelectorAll('[data-preview-scene]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.previewScene[id] = btn.dataset.previewScene;
      drawPreviewGallery(id);
    });
  });
  box.querySelectorAll('[data-preview-kind]').forEach(btn => {
    btn.addEventListener('click', () => {
      state.previewKind[id] = btn.dataset.previewKind;
      drawPreviewGallery(id);
    });
  });
}

function formatDateKey(key) {
  // "20260712" -> "12 Jul 2026". Kunci scene preview selalu YYYYMMDD; kalau
  // suatu saat bukan, tampilkan apa adanya daripada mengarang tanggal.
  if (!/^\d{8}$/.test(key)) return escapeHTML(key);
  const d = new Date(key.slice(0, 4) + '-' + key.slice(4, 6) + '-' + key.slice(6, 8) + 'T00:00:00Z');
  if (isNaN(d)) return escapeHTML(key);
  return d.toLocaleDateString('id-ID', { day: 'numeric', month: 'short', year: 'numeric', timeZone: 'UTC' });
}

function renderStorageBreakdown(id, storage) {
  const tiers = STORAGE_TIER_ORDER.map(t => [t, storage.tiers[t.toLowerCase()]]).filter(([, v]) => v && v.file_count > 0);
  if (tiers.length === 0) return '<div class="empty-small">Belum ada berkas di disk</div>';

  const maxBytes = Math.max.apply(null, tiers.map(([, v]) => v.size_bytes));
  const usedSources = Object.keys(storage.sources).filter(s => !SOURCELESS_TIERS.includes(s));

  const legend = usedSources.length > 1
    ? '<div class="struct-legend">' + usedSources.map(src =>
        '<span class="struct-legend-item">' +
          '<span class="struct-swatch" style="background:' + sourceColor(src) + '"></span>' +
          escapeHTML(sourceLabel(src)) +
          '<span class="struct-legend-size">' + humanBytes(storage.sources[src].size_bytes) + '</span>' +
        '</span>').join('') +
      '</div>'
    : '';

  const rows = tiers.map(([tier, info]) => {
    const widthPct = maxBytes > 0 ? (info.size_bytes / maxBytes) * 100 : 0;
    const entries = Object.entries(info.sources);

    // Tier fusion dan preview tidak punya pecahan source -- isinya justru
    // gabungan ketiganya, jadi digambar sebagai satu batang netral.
    const segments = entries.length > 0
      ? entries.map(([src, v]) => {
          const share = info.size_bytes > 0 ? (v.size_bytes / info.size_bytes) * 100 : 0;
          return '<span class="struct-seg" style="flex:' + share + ' 1 0;background:' + sourceColor(src) + '"' +
            ' title="' + escapeHTML(sourceLabel(src) + ' · ' + tier + ' · ' + humanBytes(v.size_bytes) + ' · ' + v.file_count + ' berkas') + '"></span>';
        }).join('')
      : '<span class="struct-seg struct-seg-mixed" style="flex:1 1 0" title="' +
          escapeHTML('Gabungan semua source · ' + humanBytes(info.size_bytes)) + '"></span>';

    const chips = entries.map(([src, v]) =>
      '<a class="struct-chip" style="--src-color:' + sourceColor(src) + '"' +
        ' href="/api/datasets/' + id + '/download?tier=' + tier.toLowerCase() + '&source=' + src + '"' +
        ' title="' + escapeHTML('Unduh ' + sourceLabel(src) + ' ' + tier) + '">' +
        escapeHTML(sourceLabel(src)) + ' <span class="struct-chip-size">' + humanBytes(v.size_bytes) + '</span>' +
      '</a>').join('');

    return '<div class="struct-row">' +
        '<div class="struct-head">' +
          '<span class="struct-tier">' + tier + '</span>' +
          '<span class="struct-total">' + humanBytes(info.size_bytes) + '</span>' +
          '<span class="struct-count">' + info.file_count + ' berkas · ' + info.scene_count + ' scene</span>' +
          '<button class="btn-link" data-tier-files="' + tier.toLowerCase() + '">Berkas</button>' +
          '<a class="btn-link" href="/api/datasets/' + id + '/download?tier=' + tier.toLowerCase() + '">Unduh</a>' +
        '</div>' +
        '<div class="struct-track"><div class="struct-bar" style="width:' + widthPct + '%">' + segments + '</div></div>' +
        (chips ? '<div class="struct-chips">' + chips + '</div>' : '') +
      '</div>';
  }).join('');

  return '<div class="struct-title">Storage per tier &amp; source</div>' + legend +
    '<div class="struct-rows">' + rows + '</div>';
}

function renderQualityBySource(quality) {
  if (!quality.sources || quality.sources.length === 0) return '';
  const items = quality.sources.map(q => {
    const bands = Object.entries(q.bands)
      .map(([b, v]) => '<span class="qual-band">' + escapeHTML(b) + ' <b>' + v.toFixed(1) + '</b></span>')
      .join('');
    // RADIOMETRIC dan COVERAGE bukan skala yang sama -- labelnya ikut
    // ditampilkan supaya tidak dibaca sebagai angka yang sebanding.
    return '<div class="qual-row">' +
        '<span class="qual-src">' + escapeHTML(sourceLabel(q.source.toLowerCase())) + '</span>' +
        '<span class="badge ' + (q.quality_flag === 'GOOD' ? 'ok' : q.quality_flag === 'POOR' ? 'danger' : 'warn') + '">' +
          (q.quality_score == null ? '-' : q.quality_score.toFixed(1)) +
        '</span>' +
        '<span class="qual-kind">' + (q.kind === 'RADIOMETRIC' ? 'radiometrik' : 'kelengkapan') + '</span>' +
        '<span class="qual-bands">' + bands + '</span>' +
      '</div>';
  }).join('');
  return '<div class="struct-title">Kualitas per source</div><div class="qual-rows">' + items + '</div>';
}

async function loadTierFiles(id, tier) {
  const box = document.getElementById('structfiles-' + id);
  if (!box) return;
  if (box.dataset.tier === tier) { box.innerHTML = ''; box.dataset.tier = ''; return; }
  box.dataset.tier = tier;
  box.innerHTML = '<div class="empty-small">Memuat berkas…</div>';
  try {
    const data = await api('/api/datasets/' + id + '/storage/files/' + tier);
    if (data.scenes.length === 0) { box.innerHTML = '<div class="empty-small">Tier ini kosong</div>'; return; }
    box.innerHTML = '<table class="scene-table"><thead><tr><th>Source</th><th>Scene</th><th>Berkas</th><th>Ukuran</th></tr></thead><tbody>' +
      data.scenes.map(sc => sc.files.map((f, i) =>
        '<tr>' +
          '<td>' + (i === 0 ? '<span class="struct-swatch" style="background:' + sourceColor(sc.source) + '"></span>' + escapeHTML(sourceLabel(sc.source || 'fusion')) : '') + '</td>' +
          '<td class="mono small">' + (i === 0 ? escapeHTML(shortenSceneId(sc.scene)) : '') + '</td>' +
          '<td class="mono small">' + escapeHTML(f.name) + '</td>' +
          '<td>' + f.size_mb.toFixed(1) + ' MB</td>' +
        '</tr>').join('')).join('') +
      '</tbody></table>';
  } catch (e) {
    box.innerHTML = '<div class="empty-small">' + escapeHTML(e.message) + '</div>';
  }
}
