#!/bin/bash

LIMIT=5000

echo "=== STARTING MEEYLAND ETL PIPELINE (BATCH: $LIMIT) ==="

python craw/auto/meeyland_step0_recreate.py --limit $LIMIT && \
python craw/auto/meeyland_step1_mergekhuvuc.py --limit $LIMIT && \
python craw/auto/meeyland_step2_normalize_price.py --limit $LIMIT && \
python craw/auto/meeyland_step3_normalize_size.py --limit $LIMIT && \
python craw/auto/meeyland_step4_normalize_type.py --limit $LIMIT && \
python craw/auto/meeyland_step5_group_median.py --limit $LIMIT && \
python craw/auto/meeyland_step6_normalize_date.py --limit $LIMIT && \
python craw/auto/step7_apply_land_price.py --domain meeyland --batch-size $LIMIT

echo "=== FINISHED MEEYLAND ETL PIPELINE ==="
