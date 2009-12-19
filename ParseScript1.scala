object ParseSpinn3r{
	import java.io.{BufferedReader,FileReader,BufferedWriter,FileWriter}
	
	class Tweet{
		var id:String=""
		var author:String=""
		var text:String=""
		var date:String=""
		var lang:String=""
		var ready:Boolean=false
	}
	
	def main(args: Array[String]){
		val fileReader = new BufferedReader(new FileReader(args(0)))
		val fileWriter = new BufferedWriter(new FileWriter("output.xml"))
		var CurrentTweet = new Tweet
		var go=true
		var line=""
		fileWriter.write("<add>\n");
		while(go){
			line = fileReader.readLine
			if(line!=null){
				if(line.compareTo("----")==0){
					if(CurrentTweet.ready){
						fileWriter.write("<doc>\n")
						fileWriter.write("\t<field name=\"id\">"+CurrentTweet.id+"</field>\n")
						fileWriter.write("\t<field name=\"author\">"+CurrentTweet.author+"</field>\n")
						fileWriter.write("\t<field name=\"date\">"+CurrentTweet.date+"</field>\n")
						fileWriter.write("\t<field name=\"lang\">"+CurrentTweet.lang+"</field>\n")
						fileWriter.write("\t<field name=\"text\">"+CurrentTweet.text+"</field>\n")
						fileWriter.write("</doc>\n")
						fileWriter.flush
					}
					CurrentTweet=new Tweet
				}
				else{
					try{
						(line.split(":")(0)) match{
							case ("link") =>
								CurrentTweet.id=line.substring(line.indexOf("statuses/")+9)
							case ("title") =>
								CurrentTweet.text=line.substring(line.indexOf(":")+1).trim.replaceAll("&","and").replaceAll("<","(lt)").replaceAll(">","(gt)");
							case ("published") =>
								CurrentTweet.date=line.substring(line.indexOf(":")+1).trim
							case ("author name") =>
								CurrentTweet.author=line.substring(line.indexOf(":")+1).trim
							case ("lang") =>
								CurrentTweet.lang=line.substring(line.indexOf(":")+1).trim
								CurrentTweet.ready=true
							case trash:String =>
						}
					}
					catch{
						case e:Exception=>
							// println(line)
					}
				}
			}
			else{
				go=false
			}
		}
		fileWriter.write("</add>\n")
		fileWriter.flush
		fileWriter.close
	}
}