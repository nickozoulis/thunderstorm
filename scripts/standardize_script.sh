
# Example use: sh standardize_scipt.sh /path_to/name_of_trimmed_dataset.txt > /path_to_/name_of_standardized_dataset.txt 

# Dataset path
DATASET=$2

# Dimensions of data to be normalized
DIM=8 

# 1st data pass, find the mean of each dimension and save it as a string separated by spaces in variable means
means=$(awk -v dim="$DIM" -f mean.awk $DATASET)

# 2nd data pass, find the standard deviation of each dimension and save it as a string separated by spaces in variable sdev
sdev=$(awk -v dim="$DIM" -v means="$means" -f standard_deviation.awk $DATASET)

# 3rd data pass, foreach data row, calculate the standard score based on mean and standard deviation, eg: (x - mean) / sdev
awk -v dim="$DIM" -v means="$means" -v sdev="$sdev" -f standardize.awk $DATASET

