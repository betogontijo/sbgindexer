package br.com.betogontijo.sbgindexer;

import java.util.ArrayList;
import java.util.List;

import br.com.betogontijo.sbgcrawler.SbgMap;

public class InvertedList extends SbgMap<String, Object> {

	/**
	 * 
	 */
	private static final long serialVersionUID = -177755135157763349L;

	protected static final String docRefList = "docRefList";
	protected static final String occurrencesList = "occurrencesList";
	// private List<Integer> docRefList;
	// private List<List<Integer>> occurrencesList;

	public InvertedList(Integer docRef, List<Integer> occur) {
		setDocRefList(new ArrayList<Integer>());
		setOccurrencesList(new ArrayList<List<Integer>>());
	}

	@SuppressWarnings("unchecked")
	private List<Integer> getDocRefList() {
		return (List<Integer>) get(InvertedList.docRefList);
	}

	private void setDocRefList(List<Integer> docRefList) {
		put(InvertedList.docRefList, docRefList);
	}

	@SuppressWarnings("unchecked")
	private List<List<Integer>> getOccurrencesList() {
		return (List<List<Integer>>) get(InvertedList.occurrencesList);
	}

	private void setOccurrencesList(List<List<Integer>> occurrencesList) {
		put(InvertedList.occurrencesList, occurrencesList);
	}

	public void addInvertedItem(Integer docRef, List<Integer> occurrences) {
		getDocRefList().add(docRef);
		getOccurrencesList().add(occurrences);
	}

}
