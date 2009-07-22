package com.gstocco.TwitLucene
/**
* Takes a JSON file as input and creates a lucene index named after the file name with "_index" appended.  Uses multiple actors concurrently parsing.
*/
object ParseJSON {
	import java.io.{BufferedReader,FileReader}
	import java.util.Date

	import org.apache.lucene.analysis.Analyzer
	import org.apache.lucene.document._
	import org.apache.lucene.index.{IndexWriter,Term,IndexReader}
	import org.apache.lucene.search.{Searcher,Query,TermQuery,IndexSearcher}
	import org.apache.lucene.queryParser.QueryParser;

	import org.codehaus.jackson._

	import org.joda.time.DateTime
	import org.joda.time.format.{DateTimeFormat,DateTimeFormatter}

	import scala.io.Source
	import scala.actors._
	import scala.actors.Actor._


	/**
	* Constant. 5 actors seems to be the current magic number as of Scala 2.7.5.final.  Any more actors and throughput drops.
	*/
	val ACTORS = 5
	var debug=false
	/**
	* Constant. Sets how often the program will commit progress to the index. Set to every 10,000 records.
	*/
	val COMMIT_LEVEL = 10000
	val dateTimeFmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")

	var dt:DateTime = new DateTime
	val fmt:DateTimeFormatter = DateTimeFormat.forPattern("yyyy-MM-dd")
	var mid:DateTime = dt.plusDays(1)
	mid=mid.minus(mid.getMillisOfDay-100)//Give the HttpServer a little time to start getting data from the twitzter
	dt=dt.minus(dt.getMillisOfDay)

	def main(args: Array[String]){
		val date = new Date()
		val factory: JsonFactory = new JsonFactory()
		var file = "notaFile"
		var prepend=""
		var perpetual = false
		if(args.length < 2){
			println("Please provide an argument.  Valid arguments are: stream prepend OR single filename. Add -d for debug messages.")
			} else if(args.length >= 2){
				args(0) match{
					case "stream" => println("Reading from "+args(1)+"."+fmt.print(dt)+" in the current directory.")
						file=fmt.print(dt)
						prepend=args(1)+"."
						perpetual=true
					case "single" => println("Reading from "+args(1)+".")
						file=args(1)
						prepend=""
						perpetual=false
				}
				if(args.length == 3){
					if(args(2)=="-d")
					debug=true
				}
				val writer = new IndexWriter("theFullIndex", new TweetAnalyzer(),IndexWriter.MaxFieldLength.UNLIMITED)
				val reader = new ReadActor(date,file,writer,perpetual,prepend)
				reader.start
				for(i <- 1 to ACTORS){
					val pActor:ParseActor = new ParseActor(reader,writer,factory)
					pActor.start
				}
			}
		}

		class ReadActor(date:Date,file:String,writer:IndexWriter,perpetual:Boolean,prepend:String) extends Actor{
			var bufReader = new BufferedReader(new FileReader(prepend+file))
			var innerPerpetual=perpetual
			def act(){
				var i=0
				var actorsRemain = ACTORS
				loop{
					react{
						case ("Gimme",actor:Actor) =>
						var line:String = bufReader.readLine
						if (line != null){
							if (!line.trim.isEmpty){
								i=i+1
								if(i%COMMIT_LEVEL==0){
									if(debug){
										print(".")
										writer.commit
									}
								}
								actor ! line
							}
							else{
								actor ! "again"
							}
						}
						else if (innerPerpetual){
							actor ! "again"
							if(mid.compareTo(new DateTime)<=0){//next day, and we ran out of lines
								try{
									bufReader = new BufferedReader(new FileReader(prepend+fmt.print(mid)))
									dt=mid
									mid=dt.plusDays(1)
								}
								catch{
									case _ => println("Couldn't open the next days' file, time to crash?")
									innerPerpetual=false
								}
							}
						} 
						else {
							actor ! "Die"
							actorsRemain -= 1
							if (actorsRemain == 0){
								writer.close
								val date2 = new Date
								println("Time elapsed: "+(date2.getTime - date.getTime)+" ms")
							}
						}

					}
				}
			}
		}

		class ParseActor(parser:ReadActor,writer:IndexWriter,factory:JsonFactory) extends Actor{
			def act(){	
				loop{
					parser ! ("Gimme",this)
					var doc = new Document
					react{
						case "Die" =>
						this.exit
						case "again" =>	
						//Do nothing, allow it to ask for gimme again
						case x:String =>	
						try {
							val jp:JsonParser = factory.createJsonParser(x);
							jp.nextToken()
							while(jp.nextToken() != JsonToken.END_OBJECT){
								val fieldName = jp.getCurrentName
								jp.nextToken()
								fieldName match{
									case "user" =>
									while(jp.nextToken () != JsonToken.END_OBJECT){
										val nameField = jp.getCurrentName
										jp.nextToken()
										nameField match{
											case "screen_name" =>
											doc.add(new Field("screen_name", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
											case "location" =>
											doc.add(new Field("location", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
											case "id" =>
											doc.add(new Field("user_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
											case _ =>
										}
									}
									case "id" =>
									doc.add(new Field("id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
									case "text" =>
									doc.add(new Field("text", jp.getText(), Field.Store.YES, Field.Index.ANALYZED))
									if(jp.getText.indexOf('?')!=(-1)){
										doc.add(new Field("isQuestion","t",Field.Store.YES, Field.Index.NOT_ANALYZED))
										} else{
											doc.add(new Field("isQuestion","f",Field.Store.YES, Field.Index.NOT_ANALYZED))
										}
										if(jp.getText.indexOf('!')!=(-1)){
											doc.add(new Field("isBang","t",Field.Store.YES, Field.Index.NOT_ANALYZED))
											} else{
												doc.add(new Field("isBang","f",Field.Store.YES, Field.Index.NOT_ANALYZED))
											}
											if(jp.getText.indexOf('#')!=(-1)){
												doc.add(new Field("isHash","t",Field.Store.YES, Field.Index.NOT_ANALYZED))
												} else{
													doc.add(new Field("isHash","f",Field.Store.YES, Field.Index.NOT_ANALYZED))
												}
												case "in_reply_to_user_id" =>
												doc.add(new Field("in_reply_to_user_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case "in_reply_to_screen_name" =>
												doc.add(new Field("in_reply_to_screen_name", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case "created_at" =>
												val time: DateTime = try { dateTimeFmt.parseDateTime(jp.getText())}
												catch { case _ => throw new Exception("Could not parse time.")}
												doc.add(new Field("created_at", time.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
												case "in_reply_to_status_id" =>
												doc.add(new Field("in_reply_to_status_id", jp.getText(), Field.Store.YES, Field.Index.NOT_ANALYZED))
												case _ =>
											}								
										}
										writer.addDocument(doc)
										} catch {
											case e: Exception =>
											// e.printStackTrace
											println("Bad Data? This line threw an exception:"+x)
										}
									}
								}
							}
						}
					}