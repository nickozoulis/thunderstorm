BEGIN {
	split(min_max, minmax, " ");
	
	# minmax contains min followed by max values.
	# So below fill the appropriate array.
	for (i=1; i<=dim; i++) {
		min[i]=minmax[i];
	}
	for (i=dim+1; i<=2*dim; i++) {
		max[i-dim]=minmax[i];
	}
}

{	
	for (i=1; i<=dim; i++) {
		printf (($i - min[i]) / (max[i] - min[i])) - scale_factor " ";
	}
	printf "\n"
}

