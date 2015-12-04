BEGIN {
	split(means, m, " ");
}

{	
	for (i=1; i<=dim; i++) {
		sumSquaredDiff[i]+=($i-m[i])^2;
	}
	counter+=1;
}

# The end block is executed only once, after the last line of the input of the awk script.
END {
	for (i=1; i<=dim; i++) {
		printf sqrt(sumSquaredDiff[i]/counter) " ";
	}
}