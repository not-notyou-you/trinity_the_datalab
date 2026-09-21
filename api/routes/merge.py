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
from pathlib import Path

from fastapi import APIRouter, Depends, HTTPException

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
    listing = DatasetManager(db).list_datasets(limit=500)
    return [
        {"dataset_id": d["dataset_id"], "name": d["name"]}
        for d in listing.get("datasets", [])
    ]


def _merged_root() -> Path:
    # fm.DATA_ROOT menunjuk ke data/datasets/. Hasil gabungan duduk SEJAJAR
    # dengan folder itu (data/merged/), bukan di dalamnya: apa pun yang ada di
    # data/datasets/ akan terbaca sebagai folder dataset oleh pemindai struktur
    # dan penghitung storage.
    return fm.DATA_ROOT.parent / MERGED_DIRNAME


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

    merged_dir = _merged_root()
    for c in result["candidates"]:
        out_name = f"merged_{c['date']}.h5"
        existing = merged_dir / out_name
        c["output_name"] = out_name
        c["already_merged"] = existing.exists()
        c["output_size_bytes"] = existing.stat().st_size if existing.exists() else None

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

    out_dir = _merged_root()
    out_path = out_dir / f"merged_{date_key}.h5"
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
        "warnings": check.warnings,
        "sources_untouched": True,
    }
