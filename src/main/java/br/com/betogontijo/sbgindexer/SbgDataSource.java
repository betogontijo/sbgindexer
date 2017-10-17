package br.com.betogontijo.sbgindexer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.Map;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.gridfs.GridFSBucket;
import com.mongodb.client.gridfs.GridFSBuckets;
import com.mongodb.client.gridfs.GridFSUploadStream;

import br.com.betogontijo.sbgcrawler.SbgDocument;
import br.com.betogontijo.sbgindexer.mongo.collections.SbgTrieNode;

public class SbgDataSource {
	@SuppressWarnings("rawtypes")
	MongoCollection<Map> documentsDb;

	@SuppressWarnings("rawtypes")
	MongoCollection<Map> vocabularyDb;

	MongoClient mongoClient;

	MongoDatabase database;

	SbgDataSource() {
		try {
			Properties properties = new Properties();
			properties.load(ClassLoader.getSystemResourceAsStream("sbgindexer.properties"));
			mongoClient = new MongoClient(properties.getProperty("mongodb.host"));
			database = mongoClient.getDatabase(properties.getProperty("mongodb.database"));
			documentsDb = database.getCollection(properties.getProperty("mongodb.colletion.documents"), Map.class);
			vocabularyDb = database.getCollection(properties.getProperty("mongodb.colletion.vocabulary"), Map.class);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@SuppressWarnings("unchecked")
	SbgDocument getDocument(int value) {
		BasicDBObject object = new BasicDBObject("_id", value);
		Map<String, Object> documentMap = documentsDb.find(object).first();
		if (documentMap == null) {
			return null;
		}
		return new SbgDocument(documentMap, null);
	}

	void addWord(SbgTrieNode root) {
		// SbgTrieNode currentRoot = new SbgTrieNode(true);
		GridFSBucket gridFSBucket = GridFSBuckets.create(database);
		GridFSUploadStream uploadStream = gridFSBucket.openUploadStream("vocabulary");
		uploadStream.write(convertToBytes(root));
		uploadStream.close();
	}
	// https://stackoverflow.com/questions/22555103/mongodb-gridfs-file-insert-java
	// http://mongodb.github.io/mongo-java-driver/3.2/driver/reference/gridfs/

	private byte[] convertToBytes(Object object) {
		try (ByteArrayOutputStream bos = new ByteArrayOutputStream(); ObjectOutput out = new ObjectOutputStream(bos)) {
			out.writeObject(object);
			return bos.toByteArray();
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
	}
}
