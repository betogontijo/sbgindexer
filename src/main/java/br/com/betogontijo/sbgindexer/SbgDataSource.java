package br.com.betogontijo.sbgindexer;

import br.com.betogontijo.sbgbeans.crawler.documents.SbgDocument;
import br.com.betogontijo.sbgbeans.crawler.repositories.SbgDocumentRepository;
import br.com.betogontijo.sbgbeans.indexer.documents.Node;
import br.com.betogontijo.sbgbeans.indexer.repositories.NodeRepository;

public class SbgDataSource {

	SbgDocumentRepository documentRepository;

	NodeRepository nodeRepository;

	SbgDataSource(SbgDocumentRepository documentRepository, NodeRepository nodeRepository) {
		this.documentRepository = documentRepository;
		this.nodeRepository = nodeRepository;
	}

	SbgDocument getDocument(int id) {
		return documentRepository.findById(id);
	}

	void addWord(Node root) {
		nodeRepository.upsertNode(root);
	}
	// https://stackoverflow.com/questions/22555103/mongodb-gridfs-file-insert-java
	// http://mongodb.github.io/mongo-java-driver/3.2/driver/reference/gridfs/

	// private byte[] convertToBytes(Object object) {
	// try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
	// ObjectOutput out = new ObjectOutputStream(bos)) {
	// out.writeObject(object);
	// return bos.toByteArray();
	// } catch (IOException e) {
	// e.printStackTrace();
	// return null;
	// }
	// }
}
