# etl/dataset_merge.py
"""
Menyatukan stack fusion beberapa dataset bersebelahan jadi satu stack utuh.

MASALAH YANG DISELESAIKAN
Sebuah bbox adalah persegi panjang, sedangkan pulau tidak. Jawa membentang
miring -- ujung baratnya di utara, ujung timurnya di selatan -- sehingga
persegi panjang terkecil yang memuatnya ikut memuat 60,4% laut (DOCS/
DECISIONS.md D16). Mempersempit bbox tidak menolong: extent daratan mengisi
100% bbox-nya, jadi tepi mana pun yang dipotong akan membuang daratan. Satu-
satunya jalan mengurangi laut adalah memecah AOI jadi beberapa strip yang
masing-masing mengikuti kemiringan pulau lebih rapat.

Pemecahan itu menyelesaikan masalah laut tapi menerbitkan masalah baru:
deep learning engineer menerima N stack terpisah per tanggal, bukan satu.
Model yang perlu konteks melintasi batas strip tidak bisa dilayani begitu.
Modul ini menutup lingkarannya.

KENAPA INI MENEMPEL, BUKAN RESAMPLE
Strip dibuat dengan batas yang sudah dikunci ke `datasets.fusion_grid` induknya
(D16), jadi origin dan ukuran piksel setiap strip identik dan selisih origin
antar-strip selalu kelipatan BULAT satu piksel. Akibatnya piksel strip A
bersambung dengan piksel strip B tanpa geser setengah piksel, dan penyatuannya
cuma soal menaruh blok di offset yang benar.

Itu bukan kemewahan. Resample akan menginterpolasi nilai backscatter SAR --
besaran fisis dalam dB yang rata-ratanya tidak bermakna di batas darat/air.
Karena itu modul ini MENOLAK menggabungkan grid yang tidak sejajar alih-alih
diam-diam meresamplenya: lihat `GridMismatch`. Lebih baik gagal terang-terangan
daripada menyerahkan angka yang sudah berubah tanpa ada yang tahu.

BATASAN YANG DISENGAJA
- Hanya stack FUSION (HDF5) yang digabung. Perantara per-satelit tidak:
  ukurannya berkali lipat dan bukan deliverable.
- Himpunan lapisan harus sama persis. Kalau satu strip punya lapisan yang
  tidak dimiliki yang lain, penggabungan ditolak, bukan diisi nodata --
  lapisan yang hilang di separuh peta adalah jebakan diam buat konsumen.
- Satu tanggal digabung dari stack tanggal yang sama saja.
"""
from __future__ import annotations

import logging
import math
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

import h5py
import numpy as np

logger = logging.getLogger(__name__)

MODULE = "MERGE"

# Toleransi ukuran piksel. Dua strip yang dipaku ke grid yang sama punya
# ukuran piksel identik bit-per-bit; toleransi kecil ini cuma menjaga dari
# galat pembulatan saat nilainya bolak-balik lewat JSON/atribut HDF5.
RES_RTOL = 1e-9

# Seberapa jauh offset piksel boleh meleset dari bilangan bulat sebelum
# dianggap tidak sejajar. 0,01 piksel = 0,9 mm di grid 9,1e-05 derajat --
# jauh di bawah apa pun yang bisa muncul dari pembulatan yang sah, dan jauh
# di bawah setengah piksel yang akan merusak nilai.
ALIGN_TOL_PX = 0.01

# Tulis keluaran per blok baris supaya union Jawa penuh (31922 x 103248
# float32 = 13 GB per lapisan) tidak pernah berada di RAM sekaligus.
ROW_BLOCK = 512

HDF5_CHUNK_MAX = 256
MODIS_NODATA_U8 = 255


class GridMismatch(ValueError):
    """Grid tidak sejajar, jadi penggabungan akan butuh resample."""


class LayerMismatch(ValueError):
    """Himpunan lapisan antar-stack tidak sama."""


@dataclass(frozen=True)
class GridSignature:
    """Identitas grid sebuah stack: apa yang harus sama supaya bisa ditempel.

    `crs` dan ukuran piksel harus sama persis. Origin TIDAK perlu sama --
    justru harus beda, karena strip menempati tempat yang berbeda. Yang harus
    benar adalah selisih origin-nya kelipatan bulat satu piksel, dan itu
    diperiksa `phase`.
    """

    crs: str
    res_x: float
    res_y: float
    # Sisa bagi origin terhadap ukuran piksel. Dua grid dengan fase sama
    # dijamin selisih origin-nya kelipatan bulat piksel, berapa pun jaraknya.
    phase_x: float
    phase_y: float

    def compatible_with(self, other: "GridSignature") -> bool:
        if self.crs != other.crs:
            return False
        if not math.isclose(self.res_x, other.res_x, rel_tol=RES_RTOL):
            return False
        if not math.isclose(self.res_y, other.res_y, rel_tol=RES_RTOL):
            return False
        return (
            _phase_close(self.phase_x, other.phase_x, self.res_x)
            and _phase_close(self.phase_y, other.phase_y, self.res_y)
        )

    def key(self) -> tuple:
        """Kunci pengelompokan kasar. Dibulatkan supaya grid yang sama tidak
        terpecah gara-gara bit terakhir; kecocokan sesungguhnya tetap
        diputuskan `compatible_with`."""
        return (
            self.crs,
            round(self.res_x, 12),
            round(self.res_y, 12),
            round(self.phase_x / self.res_x, 6),
            round(self.phase_y / self.res_y, 6),
        )


def _phase_close(a: float, b: float, res: float) -> bool:
    """Fase itu melingkar: 0 dan res adalah titik yang sama."""
    d = abs(a - b) % res
    return min(d, res - d) <= ALIGN_TOL_PX * res


@dataclass
class StackInfo:
    """Satu stack fusion yang jadi calon bahan penggabungan."""

    dataset_id: int
    dataset_name: str
    path: Path
    date_key: str
    height: int
    width: int
    transform: tuple[float, float, float, float, float, float]
    crs: str
    layers: tuple[str, ...]
    fusion_strategy: str | None
    size_bytes: int

    @property
    def west(self) -> float:
        return self.transform[2]

    @property
    def north(self) -> float:
        return self.transform[5]

    @property
    def res_x(self) -> float:
        return abs(self.transform[0])

    @property
    def res_y(self) -> float:
        return abs(self.transform[4])

    @property
    def east(self) -> float:
        return self.west + self.width * self.res_x

    @property
    def south(self) -> float:
        return self.north - self.height * self.res_y

    def signature(self) -> GridSignature:
        return GridSignature(
            crs=self.crs,
            res_x=self.res_x,
            res_y=self.res_y,
            phase_x=self.west % self.res_x,
            phase_y=self.north % self.res_y,
        )


@dataclass
class MergeCandidate:
    """Satu tanggal yang bisa digabung dari beberapa dataset."""

    date_key: str
    stacks: list[StackInfo]
    # Diisi `describe()`; ditaruh di sini supaya API tidak perlu menghitung
    # ulang hal yang sudah dihitung saat pemeriksaan kelayakan.
    out_height: int = 0
    out_width: int = 0
    blocked_reason: str | None = None
    warnings: list[str] = field(default_factory=list)

    @property
    def mergeable(self) -> bool:
        return self.blocked_reason is None

    @property
    def dataset_ids(self) -> list[int]:
        return [s.dataset_id for s in self.stacks]

    @property
    def input_bytes(self) -> int:
        return sum(s.size_bytes for s in self.stacks)


# ---------------------------------------------------------------------------
# Membaca stack yang ada di disk
# ---------------------------------------------------------------------------

def read_stack_info(path: Path, dataset_id: int, dataset_name: str) -> StackInfo | None:
    """Baca atribut grid sebuah stack HDF5. None kalau berkasnya bukan stack
    yang bisa dipakai -- termasuk stack versi lama yang belum menulis
    `transform`, yang tanpa itu tidak bisa ditempatkan di peta sama sekali."""
    try:
        with h5py.File(path, "r") as h:
            attrs = h.attrs
            if "transform" not in attrs or "crs" not in attrs:
                logger.warning(
                    "[%s] %s dilewati: tidak punya atribut transform/crs "
                    "(stack versi lama)", MODULE, path.name,
                )
                return None
            transform = tuple(float(v) for v in attrs["transform"][:6])
            layers = tuple(sorted(_as_str_list(attrs.get("layers", list(h.keys())))))
            height = int(attrs["height"]) if "height" in attrs else 0
            width = int(attrs["width"]) if "width" in attrs else 0
            if not height or not width:
                first = h[next(iter(h.keys()))]
                height, width = int(first.shape[0]), int(first.shape[1])
            strategy = attrs.get("fusion_strategy")
            return StackInfo(
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                path=path,
                date_key=_date_from_name(path.name),
                height=height,
                width=width,
                transform=transform,
                crs=_as_str(attrs["crs"]),
                layers=layers,
                fusion_strategy=_as_str(strategy) if strategy is not None else None,
                size_bytes=path.stat().st_size,
            )
    except (OSError, KeyError, ValueError) as exc:
        logger.warning("[%s] %s tidak terbaca: %s", MODULE, path.name, exc)
        return None


def _as_str(v) -> str:
    return v.decode() if isinstance(v, bytes) else str(v)


def _as_str_list(v) -> list[str]:
    return [_as_str(x) for x in v]


def _date_from_name(name: str) -> str:
    """`fusion_20251201_cooccurrence_processed.h5` -> `20251201`."""
    for part in name.split("_"):
        if len(part) == 8 and part.isdigit():
            return part
    return ""


# ---------------------------------------------------------------------------
# Kelayakan
# ---------------------------------------------------------------------------

def check_mergeable(stacks: list[StackInfo]) -> MergeCandidate:
    """Tentukan apakah sekumpulan stack bisa ditempel, dan kalau tidak, kenapa.

    Alasan penolakan ditulis untuk dibaca manusia di UI, bukan cuma kode galat:
    yang memutuskan meneruskan atau tidak adalah peneliti, dan keputusan itu
    butuh tahu apa yang salah.
    """
    date_key = stacks[0].date_key if stacks else ""
    cand = MergeCandidate(date_key=date_key, stacks=list(stacks))

    if len(stacks) < 2:
        cand.blocked_reason = "Perlu minimal dua stack untuk digabung."
        return cand

    base = stacks[0]
    base_sig = base.signature()

    for s in stacks[1:]:
        if not base_sig.compatible_with(s.signature()):
            cand.blocked_reason = (
                f"Grid {s.dataset_name} tidak sejajar dengan {base.dataset_name}. "
                "Penggabungan dibatalkan karena akan butuh resample, dan resample "
                "mengubah nilai backscatter."
            )
            return cand
        if s.layers != base.layers:
            missing = set(base.layers) ^ set(s.layers)
            cand.blocked_reason = (
                f"Himpunan lapisan berbeda antara {base.dataset_name} dan "
                f"{s.dataset_name}: {', '.join(sorted(missing))}."
            )
            return cand

    strategies = {s.fusion_strategy for s in stacks}
    if len(strategies) > 1:
        cand.blocked_reason = (
            "Strategi fusion berbeda antar-stack: "
            + ", ".join(sorted(str(x) for x in strategies))
            + ". Menggabungkannya akan menghasilkan satu berkas yang separuhnya "
            "dihitung dengan aturan berbeda."
        )
        return cand

    height, width, _ = _union_grid(stacks)
    cand.out_height, cand.out_width = height, width

    if _overlap_pairs(stacks):
        cand.warnings.append(
            "Ada strip yang saling tumpang tindih; di daerah itu yang dipakai "
            "adalah stack dengan piksel valid terbanyak."
        )
    return cand


def _union_grid(stacks: list[StackInfo]) -> tuple[int, int, tuple[float, float]]:
    """Ukuran dan origin grid gabungan."""
    west = min(s.west for s in stacks)
    north = max(s.north for s in stacks)
    east = max(s.east for s in stacks)
    south = min(s.south for s in stacks)
    res_x, res_y = stacks[0].res_x, stacks[0].res_y
    width = int(round((east - west) / res_x))
    height = int(round((north - south) / res_y))
    return height, width, (west, north)


def _offset_of(stack: StackInfo, origin: tuple[float, float]) -> tuple[int, int]:
    """Posisi stack di dalam grid gabungan, dalam piksel.

    Menolak offset yang bukan bilangan bulat: itu tanda grid tidak sejajar,
    dan menempel di offset pecahan akan menggeser seluruh strip.
    """
    west, north = origin
    col = (stack.west - west) / stack.res_x
    row = (north - stack.north) / stack.res_y
    for name, v in (("kolom", col), ("baris", row)):
        if abs(v - round(v)) > ALIGN_TOL_PX:
            raise GridMismatch(
                f"{stack.dataset_name}: offset {name} {v:.4f} piksel bukan "
                "bilangan bulat, grid tidak sejajar."
            )
    return int(round(row)), int(round(col))


def _overlap_pairs(stacks: list[StackInfo]) -> list[tuple[str, str]]:
    out = []
    for i, a in enumerate(stacks):
        for b in stacks[i + 1:]:
            if a.west < b.east and b.west < a.east and a.south < b.north and b.south < a.north:
                out.append((a.dataset_name, b.dataset_name))
    return out


def _nodata_for(dtype: np.dtype):
    return MODIS_NODATA_U8 if dtype == np.uint8 else np.nan


def _valid_fraction(path: Path, layer: str) -> float:
    """Berapa bagian piksel yang bukan nodata. Dipakai mengurutkan stack di
    daerah tumpang tindih -- yang datanya paling utuh yang menang, bukan yang
    kebetulan diproses belakangan."""
    try:
        with h5py.File(path, "r") as h:
            if layer not in h:
                return 0.0
            ds = h[layer]
            # Sampel baris, bukan seluruh raster: untuk mengurutkan cukup
            # perkiraan, dan membaca 13 GB demi satu angka urutan tidak masuk akal.
            step = max(1, ds.shape[0] // 64)
            sample = ds[::step]
            if sample.dtype == np.uint8:
                return float((sample != MODIS_NODATA_U8).mean())
            return float(np.isfinite(sample).mean())
    except (OSError, KeyError):
        return 0.0


# ---------------------------------------------------------------------------
# Penggabungan
# ---------------------------------------------------------------------------

def merge_stacks(
    stacks: list[StackInfo],
    out_path: Path,
    *,
    progress=None,
) -> Path:
    """Tempel beberapa stack jadi satu berkas HDF5 di grid gabungan.

    Ditulis per blok baris supaya union sebesar Jawa tidak pernah berada di
    RAM sekaligus: satu lapisan float32 di grid Jawa penuh berukuran 13 GB,
    sedangkan mesin yang menjalankan ini punya jauh lebih sedikit.
    """
    cand = check_mergeable(stacks)
    if not cand.mergeable:
        raise ValueError(cand.blocked_reason)

    height, width, origin = _union_grid(stacks)
    layers = stacks[0].layers
    offsets = {s.dataset_id: _offset_of(s, origin) for s in stacks}

    # Yang paling utuh ditulis TERAKHIR supaya menimpa yang lebih bolong di
    # daerah tumpang tindih -- setara `method="first"` di s1_mosaic, tapi
    # tanpa perlu menahan mask di memori.
    order = sorted(stacks, key=lambda s: _valid_fraction(s.path, layers[0]))

    out_path.parent.mkdir(parents=True, exist_ok=True)
    res_x, res_y = stacks[0].res_x, stacks[0].res_y
    west, north = origin

    with h5py.File(out_path, "w") as out:
        for li, layer in enumerate(layers):
            with h5py.File(stacks[0].path, "r") as probe:
                dtype = probe[layer].dtype
                layer_attrs = dict(probe[layer].attrs)
            nodata = _nodata_for(dtype)
            dst = out.create_dataset(
                layer,
                shape=(height, width),
                dtype=dtype,
                chunks=(min(HDF5_CHUNK_MAX, height), min(HDF5_CHUNK_MAX, width)),
                compression="gzip",
                shuffle=True,
                fillvalue=nodata,
            )
            for key, value in layer_attrs.items():
                dst.attrs[key] = value
            dst.attrs["merged_from"] = [s.dataset_name for s in stacks]

            for s in order:
                row0, col0 = offsets[s.dataset_id]
                with h5py.File(s.path, "r") as src:
                    sds = src[layer]
                    for r in range(0, s.height, ROW_BLOCK):
                        r1 = min(r + ROW_BLOCK, s.height)
                        block = sds[r:r1]
                        target = dst[row0 + r:row0 + r1, col0:col0 + s.width]
                        # Jangan biarkan nodata sumber menghapus piksel valid
                        # yang sudah ditulis tetangganya di daerah tumpang tindih.
                        if dtype == np.uint8:
                            keep = block != MODIS_NODATA_U8
                        else:
                            keep = np.isfinite(block)
                        target[keep] = block[keep]
                        dst[row0 + r:row0 + r1, col0:col0 + s.width] = target
                if progress:
                    progress(li, len(layers), s.dataset_name, layer)

        out.attrs["layers"] = list(layers)
        out.attrs["height"] = height
        out.attrs["width"] = width
        out.attrs["crs"] = stacks[0].crs
        out.attrs["transform"] = [res_x, 0.0, west, 0.0, -res_y, north]
        out.attrs["grid_bbox"] = [
            float(west), float(north - height * res_y),
            float(west + width * res_x), float(north),
        ]
        out.attrs["aoi_bbox"] = list(out.attrs["grid_bbox"])
        out.attrs["grid_offset_row_col"] = [0, 0]
        out.attrs["processing_level"] = "PROCESSED"
        out.attrs["processing_datetime"] = datetime.now(timezone.utc).isoformat()
        if stacks[0].fusion_strategy:
            out.attrs["fusion_strategy"] = stacks[0].fusion_strategy
        # Provenance: dari mana potongannya datang, dan di piksel mana
        # masing-masing ditaruh. Tanpa ini berkas gabungan tidak bisa
        # menjelaskan dirinya, dan D16 jadi tidak bisa ditelusuri dari data.
        out.attrs["merged"] = True
        out.attrs["merged_from_datasets"] = [s.dataset_name for s in stacks]
        out.attrs["merged_from_dataset_ids"] = [s.dataset_id for s in stacks]
        out.attrs["merged_offsets_row_col"] = [
            list(offsets[s.dataset_id]) for s in stacks
        ]
        out.attrs["merged_date"] = stacks[0].date_key

    logger.info(
        "[%s] %s: %d stack -> %dx%d, %d lapisan, %s",
        MODULE, stacks[0].date_key, len(stacks), height, width, len(layers),
        out_path.name,
    )
    return out_path


# ---------------------------------------------------------------------------
# Penemuan kandidat
#
# UI perlu bisa bertanya "apa yang bisa digabung?" tanpa tahu apa-apa soal
# grid. Bagian ini menjawabnya dari apa yang ADA DI DISK, bukan dari niat yang
# tercatat di database: stack yang barisnya ada tapi berkasnya hilang bukan
# kandidat, dan itu cuma ketahuan dengan melihat.
# ---------------------------------------------------------------------------

def collect_stacks(datasets: list[dict]) -> list[StackInfo]:
    """Semua stack fusion yang terbaca dari daftar dataset.

    `datasets` cukup berisi `dataset_id` dan `name` -- sengaja tidak menerima
    objek ORM supaya modul ini bisa diuji tanpa database.
    """
    from etl import folder_manager as fm

    out: list[StackInfo] = []
    for d in datasets:
        did, name = d["dataset_id"], d["name"]
        fusion_dir = fm.get_dataset_root(did, name) / "fusion"
        if not fusion_dir.is_dir():
            continue
        for path in sorted(fusion_dir.rglob("*.h5")):
            info = read_stack_info(path, did, name)
            if info is not None and info.date_key:
                out.append(info)
    return out


def find_candidates(datasets: list[dict]) -> list[MergeCandidate]:
    """Kelompokkan stack jadi kandidat penggabungan, satu per tanggal per grid.

    Dua stack masuk kelompok yang sama kalau grid-nya sejajar DAN tanggalnya
    sama. Tanggal yang cuma dipunyai satu dataset tidak muncul sebagai
    kandidat -- tidak ada yang perlu digabung di sana.
    """
    stacks = collect_stacks(datasets)

    buckets: dict[tuple, list[StackInfo]] = {}
    for s in stacks:
        buckets.setdefault((s.signature().key(), s.date_key), []).append(s)

    candidates = []
    for (_, date_key), group in sorted(buckets.items(), key=lambda kv: kv[0][1]):
        if len(group) < 2:
            continue
        group.sort(key=lambda s: s.dataset_id)
        candidates.append(check_mergeable(group))
    return candidates


def describe_candidates(datasets: list[dict]) -> dict:
    """Bentuk siap-JSON untuk API/UI.

    Kandidat yang TERHALANG tetap dikembalikan, lengkap dengan alasannya.
    Menyembunyikannya akan membuat UI diam soal data yang hampir bisa
    digabung, dan justru itu yang perlu dilihat peneliti.
    """
    cands = find_candidates(datasets)
    names = {d["dataset_id"]: d["name"] for d in datasets}
    return {
        "candidate_count": len(cands),
        "mergeable_count": sum(1 for c in cands if c.mergeable),
        "candidates": [
            {
                "date": c.date_key,
                "mergeable": c.mergeable,
                "blocked_reason": c.blocked_reason,
                "warnings": c.warnings,
                "dataset_ids": c.dataset_ids,
                "dataset_names": [names.get(i, str(i)) for i in c.dataset_ids],
                "stack_count": len(c.stacks),
                "layers": list(c.stacks[0].layers) if c.stacks else [],
                "output_shape": [c.out_height, c.out_width],
                "input_bytes": c.input_bytes,
            }
            for c in cands
        ],
    }
