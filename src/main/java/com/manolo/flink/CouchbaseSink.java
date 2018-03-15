package com.manolo.flink;

import java.util.Arrays;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class CouchbaseSink extends RichSinkFunction<JsonDocument> {

	private static Bucket bucket = null;

	private String clusterUri; // comma-separated list of server dns names or IPs
	private String user;
	private String password;
	private String bucketName;

	public CouchbaseSink(String clusterUri, String user, String password, String bucketName) {
		super();
		this.clusterUri = clusterUri;
		this.user = user;
		this.password = password;
		this.bucketName = bucketName;
	}

	@Override
	public void invoke(JsonDocument doc) throws Exception {
		bucket.upsert(doc);
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		initBucket();
	}

	private void initBucket() {
		if(bucket == null || bucket.isClosed()) {
			try {
				Cluster cluster = CouchbaseCluster.create(Arrays.asList(clusterUri));
				cluster.authenticate(user, password);
				bucket = cluster.openBucket(bucketName);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
