package com.manolo.flink;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.twitter.TwitterSource;
import org.apache.flink.util.Collector;

import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.json.JsonObject;

public class TwitterIntoCouchbase {
	
	private static String lang  = null; 
	
	public static void main(String[] args) throws Exception {
		
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);
		env.addSource(new TwitterSource(params.getProperties()))
			.name("Twitter Source")
			.flatMap(new TweetToJsonDocument())
			.addSink(new CouchbaseSink(params.get("clusterUri"), params.get("user"), params.get("password"), params.get("bucketName")))
			.name("Couchbase Sink");
		
		env.execute("Ingest data from Twitter into Couchbase");
	}
	
	public static class TweetToJsonDocument implements FlatMapFunction<String, JsonDocument> {

		@Override
		public void flatMap(String tweet, Collector<JsonDocument> out) throws Exception {
			
			JsonObject jsonTweet = JsonObject.fromJson(tweet);
			
			if(((lang == null || lang.isEmpty()) || lang.equals(jsonTweet.get("lang"))) && !jsonTweet.containsKey("delete") && jsonTweet.containsKey("text")  && jsonTweet.containsKey("user")) {
				
				JsonObject tweetUser = jsonTweet.getObject("user");
				JsonObject tweetShrinkUser = JsonObject.create()
						.put("id", tweetUser.get("id"))
						.put("name", tweetUser.get("name"))
						.put("url", tweetUser.get("url"));
				
				Map<String, List<String>> mhr = getHashTagsAndReferences(jsonTweet.getString("text"));
				
				JsonObject shrinkTweet = JsonObject.create()
						.put("created_at", jsonTweet.get("created_at"))
						.put("source", jsonTweet.get("source"))
						.put("retweet_count", jsonTweet.get("retweet_count"))
						.put("in_reply_to_user_id", jsonTweet.get("in_reply_to_user_id"))
						.put("id", jsonTweet.get("id"))
						.put("timestamp_ms", jsonTweet.get("timestamp_ms"))
						.put("text", jsonTweet.get("text"))
						.put("lang", jsonTweet.get("lang"))
						.put("hashtags", mhr.get("hashtags"))
						.put("references", mhr.get("references"))
						.put("user", tweetShrinkUser);
				
				String key = "tw::" + jsonTweet.get("id");
				out.collect(JsonDocument.create(key, shrinkTweet));
	
			}
		}

		private Map<String, List<String>> getHashTagsAndReferences(String text) {
			Map<String, List<String>> result = new HashMap<String, List<String>>();
			List<String> hashtags = new ArrayList<String>();
			List<String> references = new ArrayList<String>();
			StringTokenizer parts = new StringTokenizer(text, " .:,;?¿");
			while(parts.hasMoreTokens()) {
				String part = parts.nextToken();
				if(part.startsWith("#"))
					hashtags.add(part.substring(1));
				else if(part.startsWith("@"))
					references.add(part.substring(1));
			}
			result.put("hashtags", hashtags);
			result.put("references", references);
			return result;
		}
	}
}
