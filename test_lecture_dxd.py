"""
Test de lecture d'un fichier .dxd — SDK officiel DWDataReader (vendored).

Valide l'extraction des canaux/mesures directement via la DLL officielle
DWDataReaderLib64.dll (dossier DWDataReader_v5_0_8/), en ctypes brut, sans
dépendance tierce (pas de pip install dwdatareader/numpy/pandas).

Objectif : confirmer que les valeurs extraites correspondent à ce que
DewesoftX afficherait pour ce même fichier, avant d'intégrer cette lecture
dans ingestion_dewesoft_dxd.py (Plan B de récupération de données).

Usage :
    python test_lecture_dxd.py [chemin_vers_fichier.dxd]

    Si aucun chemin n'est fourni, utilise le premier fichier .dxd trouvé
    dans le dossier data_retrait/.
"""

import ctypes
import glob
import os
import sys
from datetime import datetime, timedelta, timezone

sys.stdout.reconfigure(encoding="utf-8")

BASE = os.path.dirname(os.path.abspath(__file__))
SDK_PYTHON_DIR = os.path.join(BASE, "DWDataReader_v5_0_8", "examples", "Python")
sys.path.insert(0, SDK_PYTHON_DIR)

from DWDataReaderHeader import (  # noqa: E402
    READER_HANDLE,
    DWChannel,
    DWFileInfo,
    DWMeasurementInfo,
    check_error,
    decode_bytes,
    load_library,
)

# Epoch Delphi (TDateTime) — utilisé par DewesoftX pour start_store_time.
EPOCH_DELPHI = datetime(1899, 12, 30, tzinfo=timezone.utc)

MAX_ECHANTILLONS_AFFICHES = 10


def resoudre_fichier_test(argv: list[str]) -> str:
    """Déterminer le fichier .dxd à tester : argument CLI ou 1er fichier de data_retrait/."""
    if len(argv) > 1:
        return argv[1]

    dossier_retrait = os.path.join(BASE, "data_retrait")
    candidats = sorted(glob.glob(os.path.join(dossier_retrait, "*.dxd")))
    if not candidats:
        print(f"❌ Aucun fichier .dxd trouvé dans {dossier_retrait}")
        sys.exit(1)
    return candidats[0]


def main() -> None:
    """Point d'entrée : lit un fichier .dxd et affiche le détail de chaque canal."""
    chemin = resoudre_fichier_test(sys.argv)
    print("=" * 60)
    print("  MurMetric — Test de lecture .dxd (SDK DWDataReader officiel)")
    print("=" * 60)
    print(f"\nFichier testé : {chemin}\n")

    lib = load_library(os.path.join(SDK_PYTHON_DIR, "DWDataReaderLib64.dll"))

    ver_major, ver_minor, ver_patch = ctypes.c_int(), ctypes.c_int(), ctypes.c_int()
    check_error(
        lib,
        lib.DWGetVersionEx(
            ctypes.byref(ver_major), ctypes.byref(ver_minor), ctypes.byref(ver_patch)
        ),
    )
    print(f"DWDataReader version : {ver_major.value}.{ver_minor.value}.{ver_patch.value}")

    reader = READER_HANDLE()
    check_error(lib, lib.DWICreateReader(ctypes.byref(reader)))

    try:
        c_filename = ctypes.c_char_p(chemin.encode())
        file_info = DWFileInfo(0, 0, 0)
        check_error(lib, lib.DWIOpenDataFile(reader, c_filename, ctypes.byref(file_info)))

        mesure_info = DWMeasurementInfo(0, 0, 0, 0)
        check_error(lib, lib.DWIGetMeasurementInfo(reader, ctypes.byref(mesure_info)))
        debut = EPOCH_DELPHI + timedelta(days=mesure_info.start_store_time)

        print(f"\nDébut d'enregistrement : {debut.isoformat()}")
        print(f"Durée totale           : {mesure_info.duration} s")
        print(f"Taux d'échantillonnage : {mesure_info.sample_rate} Hz")

        ch_count = ctypes.c_int()
        check_error(lib, lib.DWIGetChannelListCount(reader, ctypes.byref(ch_count)))
        print(f"\nNombre de canaux : {ch_count.value}")

        channel_list = (DWChannel * ch_count.value)()
        check_error(lib, lib.DWIGetChannelList(reader, channel_list))

        for i in range(ch_count.value):
            ch = channel_list[i]
            nom = decode_bytes(ch.name)
            unite = decode_bytes(ch.unit)

            sample_cnt = ctypes.c_longlong()
            check_error(
                lib, lib.DWIGetScaledSamplesCount(reader, ch.index, ctypes.byref(sample_cnt))
            )

            print(
                f"\n--- Canal {ch.index} : {nom!r} ({unite}) — "
                f"{sample_cnt.value} échantillon(s) ---"
            )

            if sample_cnt.value == 0:
                continue

            samples = (ctypes.c_double * sample_cnt.value)()
            timestamps = (ctypes.c_double * sample_cnt.value)()
            check_error(
                lib,
                lib.DWIGetScaledSamples(reader, ch.index, 0, sample_cnt, samples, timestamps),
            )

            nb_affiches = min(sample_cnt.value, MAX_ECHANTILLONS_AFFICHES)
            for j in range(nb_affiches):
                horodatage_absolu = debut + timedelta(seconds=timestamps[j])
                print(
                    f"   t={timestamps[j]:.3f}s ({horodatage_absolu.strftime('%H:%M:%S.%f')}) "
                    f"→ {samples[j]:.6f} {unite}"
                )
            if sample_cnt.value > nb_affiches:
                print(f"   ... ({sample_cnt.value - nb_affiches} échantillon(s) supplémentaire(s))")

        check_error(lib, lib.DWICloseDataFile(reader))
    finally:
        check_error(lib, lib.DWIDestroyReader(reader))

    print("\n✅ Lecture terminée.")


if __name__ == "__main__":
    main()
