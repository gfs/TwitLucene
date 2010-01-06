#!/usr/local/bin/perl
open (FILE, 'rawdata.txt');
my $records = 0;
my $flushLevel = 100000;
my $fileNum = 0;
my $fileStr=sprintf("%04d", $fileNum);
my $ready = 0;
my $lines = 0;

my $id="";
my $text="";
my $date="";
my $name="";
my $lang="";

open my $outf, '>', "myOutput$fileStr.xml";

while (<FILE>) {
	if ($lines==0){
		# print "<add>\n";
		print $outf "<add>\n";
#		print $fileNum;
#		print ": ";
		$lines++;
	}
	
	chomp;
	my $line = $_;
	if($line =~ m/^link:/){
		if($line =~ m/^.*statuses\/[\d]*$/){
			$line =~ s/^.*statuses\/([\d]*)$/$1/;		
		}
		else{
			$line = 0;		
		}
		# $line =~ s/[^\d]//g;
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
		$line =~ s/\&/$1(and)$2/g;
		$line =~ s/\</$1(lt)$2/g;
		$line =~ s/\>/$1(gt)$2/g;
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
#		print "$records\n";
		# print ".";
		print $outf $out;
		$ready=0;
	}
	if ($records > $flushLevel){
		$fileNum+=1;
		$fileStr=sprintf("%04d", $fileNum);
		print $outf "</add>\n";
		$|++;
		open my $outnew, '>', "myOutput$fileStr.xml";
		close($outf);
		$outf=$outnew;
		$lines=0;
		$records=0;
	}
}
close (FILE);
print $outf "</add>\n";
close($outf);
