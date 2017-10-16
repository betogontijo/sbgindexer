package br.com.betogontijo.sbgindexer;

/**
 * Hello world!
 *
 */
public class App {
	public static void main(String[] args) {
		SbgReader sbgReader = new SbgReader();
		sbgReader.read();
		sbgReader.getTrie().find("batata");
	}
}
