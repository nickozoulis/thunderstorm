
# Example use: sh norm_scipt.sh /path_to_/raw_dataset.txt /path_to/name_of_trimmed_dataset.txt > /path_to_/name_of_normalized_dataset.txt 

# Script variables
RAW_DATASET=$1
DATASET=$2

# Find how many lines the raw_dataset contains, so as to trim from 2 to this range.
lines=$(wc -l $RAW_DATASET)
line=($lines) # bash array to split up a string on spaces 

# From the raw dataset, skip the first row and keep only the 12th to 19th column points
sed -n 2,${line}p $RAW_DATASET | awk '{printf $12 " " $13 " " $14 " " $15 " " $16 " " $17 " " $18 " " $19 "\n"}' > $DATASET

# --- Normalization ---

# Dimensions of data to be normalized
DIM=8 

# 1st data pass, find the mean of each dimension and save it as a string separated by spaces in variable means
means=$(awk -v dim="$DIM" -f mean.awk $DATASET)

# 2nd data pass, find the standard deviation of each dimension and save it as a string separated by spaces in variable sdev
sdev=$(awk -v dim="$DIM" -v means="$means" -f standard_deviation.awk $DATASET)

# 3rd data pass, foreach data row, calculate the standard score based on mean and standard deviation, eg: (x - mean) / sdev
awk -v dim="$DIM" -v means="$means" -v sdev="$sdev" -f normalize.awk $DATASET

