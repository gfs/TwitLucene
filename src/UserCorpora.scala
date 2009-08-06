package com.gstocco.TwitLucene

import scala.collection.mutable.HashMap

import org.apache.lucene.index.{IndexWriter,IndexReader}
import org.apache.lucene.document._

object UserCorpora{
	def main(args:Array[String]){
		val reader:IndexReader = IndexReader.open("theFullIndex")
		val hash = new HashMap[String,(String,String,int)]
		for(i <- 0 to reader.maxDoc){
			if(!reader.isDeleted(i)){
				val doc:Document = reader.document(i)
				val uId = doc.getField("user_id").stringValue
				if(hash.contains(uId)){
					val tmp = hash.get(doc.getField("user_id").stringValue).get
					hash.update(uId,(doc.getField("screen_name").stringValue,tmp._2+" "+doc.getField("text").stringValue,tmp._3+1))
				}
				else{
					hash.put(uId,(doc.getField("screen_name").stringValue,doc.getField("text").stringValue,1))
				}
			}
		}
		val list = hash.toList
		println("starting corpora index")
		val writer = new IndexWriter("theCorporaIndex", new TweetAnalyzer(),IndexWriter.MaxFieldLength.UNLIMITED)
		list.foreach{(elem) =>
			val doc = new Document
			doc.add(new Field("user_id", elem._1, Field.Store.YES, Field.Index.NOT_ANALYZED))
			doc.add(new Field("screen_name", elem._2._1, Field.Store.YES, Field.Index.NOT_ANALYZED))
			doc.add(new Field("text", elem._2._2, Field.Store.YES, Field.Index.ANALYZED))
			doc.add(new Field("num_docs", elem._2._3.toString, Field.Store.YES, Field.Index.NOT_ANALYZED))
			writer.addDocument(doc)
		}
		writer.close()
	}
}