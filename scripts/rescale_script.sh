
#USE: sh rescale_script.sh 

DIM=8
SCALE_FACTOR=0.5

MIN_MAX=$(awk -v dim="$DIM" -f min_max.awk dataset.txt)

awk -v dim="$DIM" -v min_max="$MIN_MAX" -v scale_factor="$SCALE_FACTOR" -f rescale.awk dataset.txt