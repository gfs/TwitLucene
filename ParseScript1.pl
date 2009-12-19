#!/usr/local/bin/perl
open (FILE, 'rawdata.txt');
my $records = 0;
my $flushLevel = 100000;
my $fileNum = 0;
my $ready = 0;
my $lines = 0;

my $id="";
my $text="";
my $date="";
my $name="";
my $lang="";

# open my $outf, '>', "myOutput$fileNum.xml";

while (<FILE>) {
	if ($lines==0){
		# print $outf "<add>\n";
		$lines++;
	}
	
	chomp;
	my $line = $_;
	if($line =~ m/^link:/){
		$line =~ s/^.*statuses\/(.*)$/$1/;
		$id = $line;
	}
	elsif($line =~ m/^title:/){
		$line =~ s/^.*:[ ]*(.*)$/$1/;
		$line =~ s/\&/$1(and)$2/g;
		$line =~ s/\</$1(lt)$2/g;
		$line =~ s/\>/$1(gt)$2/g;
		$text = $line;
	}
	elsif($line =~ m/^pubDate:/){
		$line =~ s/^.*:[ ]+(.*)$/$1/;
		$date = $line;
	}
	elsif($line =~ m/^author name:/){
		$line =~ s/^.*:[ ]*(.*)$/$1/;
		$name = $line;
	}
	elsif($line =~ m/^lang:/){
		$line =~ s/^.*:[ ]*(.*)$/$1/;
		$lang = $line;
		$ready = 1;
	}
	
	if($ready==1){
		$records++;
		$out=<<END;
<doc>
	<field name="id">$id</field>
	<field name="text">$text</field>
	<field name="date">$date</field>
	<field name="name">$name</field>
	<field name="lang">$lang</field>
</doc>
END
		print $out;
		# print $outf $out;
		$ready=0;
	}
	# if ($records % $flushLevel == 0){
	# 	$fileNum+=1;
	# 	print $outf "</add>\n";
	# 	$|++;
	# 	open my $outnew, '>', "myOutput$fileNum.xml";
	# 	close($outf);
	# 	$outf=$outnew;
	# 	$lines=0;
	# }
}
# close (OUTFILE);
close (FILE);
