# fx-data
Données M1 Dukascopy (bid + ask + volume), 8 paires FX, 2016–2025, un Parquet par mois dans `data/PAIRE/`.
Heures en UTC. Prix entiers (×1e5, ×1e3 pour les paires JPY). Colonnes : t, bo bh bl bc bv (bid), ao ah al ac av (ask).
Téléchargement : `.github/workflows/dukascopy.yml` (GitHub Actions), script `scripts/dkm1.py`.
