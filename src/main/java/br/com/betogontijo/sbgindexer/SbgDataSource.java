package br.com.betogontijo.sbgindexer;

import java.io.IOException;
import java.util.Map;
import java.util.Properties;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import br.com.betogontijo.sbgcrawler.SbgDocument;

public class SbgDataSource {
	@SuppressWarnings("rawtypes")
	MongoCollection<Map> documentsDb;

	@SuppressWarnings("rawtypes")
	MongoCollection<Map> vocabularyDb;

	MongoClient mongoClient;

	SbgDataSource() {
		try {
			Properties properties = new Properties();
			properties.load(ClassLoader.getSystemResourceAsStream("sbgindexer.properties"));
			mongoClient = new MongoClient(properties.getProperty("mongodb.host"));
			MongoDatabase database = mongoClient.getDatabase(properties.getProperty("mongodb.database"));
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
//		SbgTrieNode currentRoot = new SbgTrieNode(true);
		vocabularyDb.insertOne(root);
	}
}
