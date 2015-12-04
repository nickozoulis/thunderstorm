BEGIN {
	split(means, m, " ");
	split(sdev, sd, " ");
}

{	
	for (i=1; i<=dim; i++) {
		printf ($i - m[i])/sd[i] " ";
	}
	printf "\n"
}

