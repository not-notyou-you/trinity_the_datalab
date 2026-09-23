# api/routes/merge.py
"""
Endpoint penggabungan stack fusion lintas dataset.

Kenapa router sendiri, bukan di bawah /api/datasets/{id}: penggabungan tidak
dimiliki satu dataset mana pun. Menggantungnya di salah satu id akan memaksa
UI memilih "dataset siapa" untuk operasi yang subjeknya justru sekumpulan
dataset.

Alurnya sengaja dua langkah -- LIHAT dulu, baru IZINKAN. Penggabungan menulis
berkas besar dan memakan waktu lama, dan yang tahu apakah empat strip itu
memang satu pulau yang sama adalah peneliti, bukan kode. Karena itu
`GET /candidates` tidak pernah menulis apa pun, dan `POST /run` tidak pernah
menebak kandidat mana yang dimaksud: id-nya harus disebut pemanggil.
"""
from __future__ import annotations

import logging
import shutil
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import FileResponse

from api.deps import get_db
from etl import dataset_merge as dm
from etl import folder_manager as fm
from etl.database_client import DatabaseClient
from etl.dataset_manager import DatasetManager

logger = logging.getLogger(__name__)
router = APIRouter()

# Hasil gabungan tidak boleh mendarat di folder salah satu dataset sumber --
# itu akan membuatnya terhitung dua kali di rincian storage dan ikut terhapus
# kalau dataset itu dihapus, padahal isinya berasal dari beberapa dataset.
MERGED_DIRNAME = "merged"


def _all_datasets(db: DatabaseClient) -> list[dict]:
    # Kuncinya "items", bukan "datasets" -- lihat DatasetManager.list_datasets,
    # yang mengembalikan {total, limit, offset, items}. Membaca kunci yang salah
    # di sini tidak melempar apa pun: ia cuma mengembalikan daftar kosong, dan
    # panel penggabungan diam seolah memang tidak ada yang bisa digabung.
    listing = DatasetManager(db).list_datasets(limit=500)
    return [
        {"dataset_id": d["dataset_id"], "name": d["name"]}
        for d in listing.get("items", [])
    ]


def _merged_root() -> Path:
    # fm.DATA_ROOT menunjuk ke data/datasets/. Hasil gabungan duduk SEJAJAR
    # dengan folder itu (data/merged/), bukan di dalamnya: apa pun yang ada di
    # data/datasets/ akan terbaca sebagai folder dataset oleh pemindai struktur
    # dan penghitung storage.
    return fm.DATA_ROOT.parent / MERGED_DIRNAME


def _checked_date_key(date_key: str) -> str:
    """Tolak apa pun yang bukan YYYYMMDD.

    Ini yang membuat `date_key` aman dipakai menyusun path yang DIHAPUS: tanggal
    delapan digit tidak bisa berisi `..` atau pemisah path, jadi tidak ada
    bentuk masukan yang bisa menunjuk keluar dari folder merged.
    """
    if len(date_key) != 8 or not date_key.isdigit():
        raise HTTPException(400, "Format tanggal harus YYYYMMDD.")
    return date_key


def _merged_path(date_key: str) -> Path:
    return _merged_root() / f"merged_{date_key}.h5"


def _preview_images(date_key: str) -> list[dict]:
    """PNG preview milik satu tanggal gabungan, siap ditaruh di <img src>.

    Daftar kosong kalau belum digabung atau render-nya gagal -- keduanya bukan
    error: HDF5-nya tetap sah tanpa preview, dan UI cuma perlu tahu tidak ada
    gambar untuk ditampilkan.
    """
    preview_dir = dm.preview_dir_for(_merged_path(date_key))
    if not preview_dir.is_dir():
        return []
    return [
        {
            "file": p.name,
            "url": f"/api/merge/preview/{date_key}/{p.name}",
            "size_bytes": p.stat().st_size,
        }
        for p in sorted(preview_dir.glob("*.png"))
    ]


@router.delete("/result/{date_key}", summary="Hapus hasil gabungan satu tanggal")
async def delete_merge_result(date_key: str, preview_only: bool = False) -> dict:
    """Hapus berkas gabungan satu tanggal beserta preview-nya.

    Yang dihapus HANYA turunan: berkas di data/merged/. Stack fusion sumber di
    folder dataset tidak disentuh sama sekali, jadi tanggal ini selalu bisa
    digabung ulang -- itulah yang membuat penghapusan di sini aman dilakukan
    untuk mengosongkan disk, tidak seperti menghapus dataset.

    `preview_only=true` menyisakan HDF5-nya dan cuma membuang PNG, untuk
    memaksa render ulang dari nol.
    """
    date_key = _checked_date_key(date_key)
    out_path = _merged_path(date_key)
    preview_dir = dm.preview_dir_for(out_path)

    removed: list[str] = []
    freed = 0

    if preview_dir.is_dir():
        freed += sum(p.stat().st_size for p in preview_dir.rglob("*") if p.is_file())
        shutil.rmtree(preview_dir)
        removed.append(preview_dir.name + "/")

    if not preview_only and out_path.exists():
        freed += out_path.stat().st_size
        out_path.unlink()
        removed.append(out_path.name)

    if not removed:
        raise HTTPException(404, f"Tidak ada hasil gabungan untuk {date_key}.")

    logger.info("[MERGE] hapus %s: %s (%d byte)", date_key, ", ".join(removed), freed)
    return {
        "status": "DELETED",
        "date": date_key,
        "removed": removed,
        "freed_bytes": freed,
        "sources_untouched": True,
    }


@router.get("/preview/{date_key}", summary="Daftar preview satu tanggal gabungan")
async def list_merge_previews(date_key: str) -> dict:
    return {"date": date_key, "images": _preview_images(date_key)}


@router.post("/preview/{date_key}/rebuild", summary="Buat preview dari hasil gabungan")
async def rebuild_merge_preview(date_key: str) -> dict:
    """Render ulang PNG dari berkas gabungan yang sudah ada.

    Untuk berkas yang digabung sebelum preview ada, atau yang render-nya gagal.
    Lambat (seluruh isi HDF5 didekompresi), tapi tetap jauh lebih murah
    daripada menggabung ulang -- dan tidak menyentuh HDF5-nya sama sekali.
    """
    out_path = _merged_path(date_key)
    if not out_path.exists():
        raise HTTPException(404, f"{out_path.name} belum ada; gabungkan dulu.")

    try:
        dm.previews_from_merged(out_path)
    except (OSError, ValueError, KeyError) as exc:
        logger.exception("[MERGE] preview %s gagal dirender", date_key)
        raise HTTPException(500, f"Preview gagal dibuat: {exc}")

    return {"date": date_key, "preview_images": _preview_images(date_key)}


@router.get(
    "/preview/{date_key}/{filename}",
    summary="Satu PNG preview hasil gabungan",
    response_class=FileResponse,
)
async def get_merge_preview(date_key: str, filename: str) -> FileResponse:
    """Kirim satu PNG dari folder preview tanggal ini.

    Nama berkas dicocokkan ke isi folder, bukan cuma dibersihkan: hanya PNG yang
    memang ada di sana yang boleh keluar, jadi tidak ada bentuk `filename` apa
    pun yang bisa menunjuk ke luar folder itu.
    """
    preview_dir = dm.preview_dir_for(_merged_path(date_key))
    match = next(
        (p for p in preview_dir.glob("*.png") if p.name == filename), None
    ) if preview_dir.is_dir() else None
    if match is None:
        raise HTTPException(404, f"Preview {filename} tidak ada untuk {date_key}.")
    return FileResponse(
        match,
        media_type="image/png",
        headers={"Cache-Control": "no-cache"},
    )


@router.get("/candidates", summary="Dataset mana yang bisa digabung")
async def list_merge_candidates(db: DatabaseClient = Depends(get_db)) -> dict:
    """Tanggal-tanggal yang punya stack di lebih dari satu dataset, beserta
    apakah grid-nya memang bisa ditempel.

    Selalu 200, termasuk saat tidak ada kandidat sama sekali: tidak adanya
    dataset yang bisa digabung adalah keadaan normal, bukan error, dan UI
    perlu membedakan keduanya.

    Kandidat yang terhalang ikut dikembalikan lengkap dengan alasannya --
    menyembunyikannya membuat UI diam soal data yang hampir bisa digabung.
    """
    datasets = _all_datasets(db)
    result = dm.describe_candidates(datasets)

    for c in result["candidates"]:
        existing = _merged_path(c["date"])
        c["output_name"] = existing.name
        c["already_merged"] = existing.exists()
        c["output_size_bytes"] = existing.stat().st_size if existing.exists() else None
        c["preview_images"] = _preview_images(c["date"]) if existing.exists() else []

    result["explanation"] = (
        "Penggabungan hanya menempel, tidak meresample: strip yang grid-nya "
        "tidak sejajar ditolak, bukan dipaksakan. Data sumber tidak diubah "
        "maupun dihapus."
    )
    return result


@router.post("/run", summary="Gabungkan stack untuk satu tanggal")
async def run_merge(
    payload: dict,
    db: DatabaseClient = Depends(get_db),
) -> dict:
    """Jalankan penggabungan untuk satu tanggal, atas izin eksplisit pemanggil.

    `payload`: {"date": "20251201", "dataset_ids": [27, 28], "overwrite": false}

    `dataset_ids` wajib disebut walau kandidatnya sudah jelas dari tanggal:
    daftar kandidat bisa berubah antara saat UI menampilkannya dan saat user
    menekan tombol (job lain selesai, stack baru muncul). Menyebut id membuat
    yang digabung persis yang dilihat user, bukan apa pun yang kebetulan ada
    saat tombol ditekan.
    """
    date_key = str(payload.get("date") or "").strip()
    dataset_ids = payload.get("dataset_ids") or []
    overwrite = bool(payload.get("overwrite", False))

    if not date_key:
        raise HTTPException(400, "Field 'date' wajib diisi, format YYYYMMDD.")
    if len(dataset_ids) < 2:
        raise HTTPException(400, "Perlu minimal dua dataset_ids untuk digabung.")

    datasets = _all_datasets(db)
    known = {d["dataset_id"] for d in datasets}
    unknown = [i for i in dataset_ids if i not in known]
    if unknown:
        raise HTTPException(404, f"Dataset tidak ditemukan: {unknown}")

    wanted = [d for d in datasets if d["dataset_id"] in set(dataset_ids)]
    stacks = [
        s for s in dm.collect_stacks(wanted)
        if s.date_key == date_key and s.dataset_id in set(dataset_ids)
    ]
    if len(stacks) < 2:
        raise HTTPException(
            400,
            f"Hanya {len(stacks)} stack ditemukan untuk tanggal {date_key} di "
            "dataset yang diminta. Kemungkinan stack-nya belum jadi atau sudah "
            "dihapus sejak daftar kandidat dibuat.",
        )

    check = dm.check_mergeable(stacks)
    if not check.mergeable:
        raise HTTPException(400, check.blocked_reason)

    out_path = _merged_path(date_key)
    if out_path.exists() and not overwrite:
        raise HTTPException(
            409,
            f"{out_path.name} sudah ada. Kirim overwrite=true kalau memang mau "
            "ditimpa.",
        )

    try:
        dm.merge_stacks(stacks, out_path)
    except (ValueError, OSError) as exc:
        logger.exception("[MERGE] gagal menggabungkan %s", date_key)
        raise HTTPException(500, f"Penggabungan gagal: {exc}")

    return {
        "status": "MERGED",
        "date": date_key,
        "dataset_ids": [s.dataset_id for s in stacks],
        "dataset_names": [s.dataset_name for s in stacks],
        "output_path": str(out_path),
        "output_name": out_path.name,
        "output_shape": [check.out_height, check.out_width],
        "output_size_bytes": out_path.stat().st_size,
        "input_size_bytes": check.input_bytes,
        "layers": list(stacks[0].layers),
        "preview_images": _preview_images(date_key),
        "warnings": check.warnings,
        "sources_untouched": True,
    }
