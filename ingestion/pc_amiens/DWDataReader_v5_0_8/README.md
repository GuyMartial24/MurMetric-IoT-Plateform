# SDK DWDataReader (non fourni dans ce repo)

`ingestion_dewesoft_dxd.py` (script d'ingestion PC Amiens) lit les fichiers
`.dxd` exportés par DeweSoftX via le SDK officiel **DWDataReader** de
DEWESoft — un binaire propriétaire tiers, pas distribuable dans un repo
public. Ce dossier doit rester **co-localisé** avec ce script (chemin
résolu relativement à `__file__`, pas au répertoire courant).

## Installation

1. Télécharger "DWDataReader" (v5.0.8 ou compatible) depuis
   [dewesoft.com/download/developer-downloads](https://dewesoft.com/download/developer-downloads).
2. Extraire l'archive ici, à la racine du repo, sous le nom
   `DWDataReader_v5_0_8/` — le script attend précisément :
   ```
   DWDataReader_v5_0_8/
     binaries/DWDataReaderLib64.dll   (ou .so / .dylib selon l'OS)
     examples/Python/DWDataReaderHeader.py
   ```
3. Aucune dépendance pip supplémentaire — le script charge la DLL en
   `ctypes` brut.

Sans ce SDK, `ingestion_dewesoft_dxd.py` échoue au démarrage avec un message
explicite indiquant où placer les fichiers (voir le bloc `try/except
ImportError` en tête du script).
