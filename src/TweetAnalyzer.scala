package com.gstocco.TwitLucene

import org.apache.lucene.analysis.Analyzer

class TweetAnalyzer extends Analyzer{
	import java.io.Reader
	
	import org.apache.lucene.analysis.{StopFilter,CharTokenizer,TokenStream}
	
	val STOP_WORDS = Array("0", "1", "2", "3", "4", "5", "6", "7", "8",
    "9", "000", "$",
    "a", "b", "c", "d", "e", "f", "g", "h", "i",
    "j", "k", "l", "m", "n", "o", "p", "q", "r",
    "s", "t", "u", "v", "w", "x", "y", "z")
	
	val STOP_SET = StopFilter.makeStopSet(STOP_WORDS)
	
	def tokenStream(fieldName:String, reader:Reader):TokenStream={
		return new StopFilter(new TweetTokenizer(reader),STOP_SET)
	}
	
	class TweetTokenizer(reader:Reader) extends CharTokenizer(reader:Reader){
		override def isTokenChar(c: char):boolean={
			c match{
				case '@' =>
					return true
				case '#' =>
					return true
				case x:char =>
					return Character.isLetter(x)
			}
		}
	}
}