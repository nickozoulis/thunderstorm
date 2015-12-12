
{	
	# Only for the first line, to initialize min and max arrays.
	if (NR==1) {
		for (i=1; i<=dim; i++) {
			min[i]=$i;
			max[i]=$i;
		}
	}

	for (i=1; i<=dim; i++) {
		if (min[i] > $i) {
			min[i] = $i;
		}
		if (max[i] < $i) {
			max[i] = $i;
		}
	}

}

# The end block is executed only once, after the last line of the input of the awk script.
END {
	for (i=1; i<=dim; i++) {
		printf min[i] " ";
	}
	for (i=1; i<=dim; i++) {
		printf max[i] " ";
	}
}