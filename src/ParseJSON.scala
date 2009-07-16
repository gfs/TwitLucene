package com.stocco.TwitLucene

object ParseJSON {
	import org.codehaus.jackson._
	import scala.io.Source
	import scala.actors._
	import scala.actors.Actor._
	import java.io.File
	import java.util.Date
	import org.apache.lucene.analysis.Analyzer
	import org.apache.lucene.document._
	import org.apache.lucene.index.{IndexWriter,Term,IndexReader}
	import org.apache.lucene.search.{Searcher,Query,TermQuery,IndexSearcher}
	import org.apache.lucene.queryParser.QueryParser;
	import org.joda.time.DateTime
	import org.joda.time.format.DateTimeFormat
	
	val ACTORS = 5
	val COMMIT_LEVEL = 10000
	val INDEX_DIR = new File("index");
	val dateTimeFmt = DateTimeFormat.forPattern("EEE MMM dd HH:mm:ss Z YYYY")
	
	def main(args: Array[String]){
		val date = new Date()
		val factory: JsonFactory = new JsonFactory();		
		val lines = Source.fromFile(args(0),"UTF-8").getLines

		val writer = new IndexWriter(args(0)+"_index", new TweetAnalyzer(), true, IndexWriter.MaxFieldLength.LIMITED)
		val reader = new ReadActor(date,lines,writer)
		reader.start
		for(i <- 1 to ACTORS){
			val pActor:ParseActor = new ParseActor(reader,writer,factory)
			pActor.start
		}
	}
	
	def RecurseJSON(doc:Document,input:List[_],pre:String){
		for (elem <- input){
			val (a,b) = elem
			b match{
				case x:List[_] =>
					RecurseJSON(doc,x,"user_")
				case null =>
				case _ =>
					a match{
						case "text" =>
							doc.add(new Field(pre+a, b.toString, Field.Store.YES, Field.Index.ANALYZED))
						case "created_at" =>
							val time: DateTime = try { dateTimeFmt.parseDateTime(b.toString)}
							catch { case _ => throw new Exception("Could not parse time.")}
							doc.add(new Field(pre+a, time.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
						case _ =>
							doc.add(new Field(pre+a, b.toString, Field.Store.NO, Field.Index.NO))
					}
			}
		}
	}
	
	class ReadActor(date:Date,lines:Iterator[_],writer:IndexWriter) extends Actor{
		def act(){
			var i=0
			var actorsRemain = ACTORS
			loop{
				react{
					case ("Gimme",actor:Actor) =>
						i=i+1
						if(i%COMMIT_LEVEL==0){
							print(".")
							writer.commit
						}
						if(lines.hasNext){
							var line:String = lines.next.toString
					 		if (!line.trim.isEmpty){
								actor ! line
								} else{
									actor ! "again"
								}
						} else{
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
								case e: NullPointerException => 
								case e: Exception =>
									e.printStackTrace
						}
					case null =>
				}
			}
		}
	}
}