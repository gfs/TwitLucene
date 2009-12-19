#!/usr/local/bin/perl
open (MYFILE, 'output.xml');
my $first = 1;
my $lines = 0;
my $lineMult = 10000;
my $lineBreak = $lineMult*7;
my $fileNum = 0;
open (OUTFILE, ">myOutput$fileNum.xml");
while (<MYFILE>) {
	if ($first==0){
		if ($lines==0){
			print OUTFILE "<add>\n";
		}
		chomp;
		# print "$_\n";
		print OUTFILE "$_\n";
		$lines+=1;
		if ($lines % $lineBreak == 0){
			$lines=0;
			$fileNum+=1;
			print OUTFILE "</add>\n";
			close (OUTFILE);
			open (OUTFILE, ">myOutput$fileNum.xml");
		}
	}
	else{
		chomp;
		$first=0;
	}
}
close (OUTFILE);
close (MYFILE);
