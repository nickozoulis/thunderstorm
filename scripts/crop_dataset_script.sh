
# Example use: sh crop_dataset_script.sh /path_to_/raw_dataset.txt /path_to/name_of_cropped_dataset.txt 
# Script variables
RAW_DATASET=$1
DATASET=$2

# Find how many lines the raw_dataset contains, so as to trim from 2 to this range.
lines=$(wc -l $RAW_DATASET)
line=($lines) # bash array to split up a string on spaces 

# From the raw dataset, skip the first row and keep only the 12th to 19th column points
sed -n 2,${line}p $RAW_DATASET | awk '{printf $12 " " $13 " " $14 " " $15 " " $16 " " $17 " " $18 " " $19 "\n"}' > $DATASET