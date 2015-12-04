{	
	for (i=1; i<=dim; i++) {
		sum[i]+=$i;
	}

	for (i=1; i<=dim; i++) {
		total[i]+=1;
	}
}

# The end block is executed only once, after the last line of the input of the awk script.
END {
	for (i=1; i<=dim; i++) {
		mean[i]=sum[i]/total[i];
	}	

	for (i=1; i<=dim; i++) {
		printf mean[i] " ";
	}

}