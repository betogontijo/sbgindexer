package br.com.betogontijo.sbgindexer;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.ListIterator;
import java.util.RandomAccess;
import java.util.Vector;

/**
 * Resizable-array implementation of the <tt>List</tt> interface. Implements all
 * optional list operations, and permits all elements, including <tt>null</tt>.
 * In addition to implementing the <tt>List</tt> interface, this class provides
 * methods to manipulate the size of the array that is used internally to store
 * the list. (This class is roughly equivalent to <tt>Vector</tt>, except that
 * it is unsynchronized.)
 *
 * <p>
 * The <tt>size</tt>, <tt>isEmpty</tt>, <tt>get</tt>, <tt>set</tt>,
 * <tt>iterator</tt>, and <tt>listIterator</tt> operations run in constant time.
 * The <tt>add</tt> operation runs in <i>amortized constant time</i>, that is,
 * adding n elements requires O(n) time. All of the other operations run in
 * linear time (roughly speaking). The constant factor is low compared to that
 * for the <tt>LinkedList</tt> implementation.
 *
 * <p>
 * Each <tt>IntList</tt> instance has a <i>capacity</i>. The capacity is the
 * size of the array used to store the elements in the list. It is always at
 * least as large as the list size. As elements are added to an IntList, its
 * capacity grows automatically. The details of the growth policy are not
 * specified beyond the fact that adding an element has constant amortized time
 * cost.
 *
 * <p>
 * An application can increase the capacity of an <tt>IntList</tt> instance
 * before adding a large number of elements using the <tt>ensureCapacity</tt>
 * operation. This may reduce the amount of incremental reallocation.
 *
 * <p>
 * <strong>Note that this implementation is not synchronized.</strong> If
 * multiple threads access an <tt>IntList</tt> instance concurrently, and at
 * least one of the threads modifies the list structurally, it <i>must</i> be
 * synchronized externally. (A structural modification is any operation that
 * adds or deletes one or more elements, or explicitly resizes the backing
 * array; merely setting the value of an element is not a structural
 * modification.) This is typically accomplished by synchronizing on some object
 * that naturally encapsulates the list.
 *
 * If no such object exists, the list should be "wrapped" using the
 * {@link Collections#synchronizedList Collections.synchronizedList} method.
 * This is best done at creation time, to prevent accidental unsynchronized
 * access to the list:
 * 
 * <pre>
 *   List list = Collections.synchronizedList(new IntList(...));
 * </pre>
 *
 * <p>
 * <a name="fail-fast"> The iterators returned by this class's
 * {@link #iterator() iterator} and {@link #listIterator(int) listIterator}
 * methods are <em>fail-fast</em>:</a> if the list is structurally modified at
 * any time after the iterator is created, in any way except through the
 * iterator's own {@link ListIterator#remove() remove} or
 * {@link ListIterator#add(Object) add} methods, the iterator will throw a
 * {@link ConcurrentModificationException}. Thus, in the face of concurrent
 * modification, the iterator fails quickly and cleanly, rather than risking
 * arbitrary, non-deterministic behavior at an undetermined time in the future.
 *
 * <p>
 * Note that the fail-fast behavior of an iterator cannot be guaranteed as it
 * is, generally speaking, impossible to make any hard guarantees in the
 * presence of unsynchronized concurrent modification. Fail-fast iterators throw
 * {@code ConcurrentModificationException} on a best-effort basis. Therefore, it
 * would be wrong to write a program that depended on this exception for its
 * correctness: <i>the fail-fast behavior of iterators should be used only to
 * detect bugs.</i>
 *
 * <p>
 * This class is a member of the
 * <a href="{@docRoot}/../technotes/guides/collections/index.html"> Java
 * Collections Framework</a>.
 *
 * @author Josh Bloch
 * @author Neal Gafter
 * @see Collection
 * @see List
 * @see LinkedList
 * @see Vector
 * @since 1.2
 */

public class IntList implements RandomAccess, Cloneable, java.io.Serializable {
	private static final long serialVersionUID = 8683452581122892189L;

	/**
	 * Default initial capacity.
	 */
	private static final int DEFAULT_CAPACITY = 10;

	/**
	 * Shared empty array instance used for empty instances.
	 */
	private static final int[] EMPTY_ELEMENTDATA = {};

	/**
	 * Shared empty array instance used for default sized empty instances. We
	 * distinguish this from EMPTY_ELEMENTDATA to know how much to inflate when
	 * first element is added.
	 */
	private static final int[] DEFAULTCAPACITY_EMPTY_ELEMENTDATA = {};

	/**
	 * The array buffer into which the elements of the IntList are stored. The
	 * capacity of the IntList is the length of this array buffer. Any empty IntList
	 * with elementData == DEFAULTCAPACITY_EMPTY_ELEMENTDATA will be expanded to
	 * DEFAULT_CAPACITY when the first element is added.
	 */
	transient int[] elementData; // non-private to simplify nested class access

	/**
	 * The size of the IntList (the number of elements it contains).
	 *
	 * @serial
	 */
	private int size;

	/**
	 * Constructs an empty list with the specified initial capacity.
	 *
	 * @param initialCapacity
	 *            the initial capacity of the list
	 * @throws IllegalArgumentException
	 *             if the specified initial capacity is negative
	 */
	public IntList(int initialCapacity) {
		if (initialCapacity > 0) {
			this.elementData = new int[initialCapacity];
		} else if (initialCapacity == 0) {
			this.elementData = EMPTY_ELEMENTDATA;
		} else {
			throw new IllegalArgumentException("Illegal Capacity: " + initialCapacity);
		}
	}

	/**
	 * Constructs an empty list with an initial capacity of ten.
	 */
	public IntList() {
		this.elementData = DEFAULTCAPACITY_EMPTY_ELEMENTDATA;
	}

	/**
	 * Trims the capacity of this <tt>IntList</tt> instance to be the list's current
	 * size. An application can use this operation to minimize the storage of an
	 * <tt>IntList</tt> instance.
	 */
	public void trimToSize() {
		if (size < elementData.length) {
			elementData = (size == 0) ? EMPTY_ELEMENTDATA : Arrays.copyOf(elementData, size);
		}
	}

	/**
	 * Increases the capacity of this <tt>IntList</tt> instance, if necessary, to
	 * ensure that it can hold at least the number of elements specified by the
	 * minimum capacity argument.
	 *
	 * @param minCapacity
	 *            the desired minimum capacity
	 */
	public void ensureCapacity(int minCapacity) {
		int minExpand = (elementData != DEFAULTCAPACITY_EMPTY_ELEMENTDATA)
				// any size if not default element table
				? 0
				// larger than default for default empty table. It's already
				// supposed to be at default size.
				: DEFAULT_CAPACITY;

		if (minCapacity > minExpand) {
			ensureExplicitCapacity(minCapacity);
		}
	}

	private void ensureCapacityInternal(int minCapacity) {
		if (elementData == DEFAULTCAPACITY_EMPTY_ELEMENTDATA) {
			minCapacity = Math.max(DEFAULT_CAPACITY, minCapacity);
		}

		ensureExplicitCapacity(minCapacity);
	}

	private void ensureExplicitCapacity(int minCapacity) {
		// overflow-conscious code
		if (minCapacity - elementData.length > 0)
			grow(minCapacity);
	}

	/**
	 * The maximum size of array to allocate. Some VMs reserve some header words in
	 * an array. Attempts to allocate larger arrays may result in OutOfMemoryError:
	 * Requested array size exceeds VM limit
	 */
	private static final int MAX_ARRAY_SIZE = Integer.MAX_VALUE - 8;

	/**
	 * Increases the capacity to ensure that it can hold at least the number of
	 * elements specified by the minimum capacity argument.
	 *
	 * @param minCapacity
	 *            the desired minimum capacity
	 */
	private void grow(int minCapacity) {
		// overflow-conscious code
		int oldCapacity = elementData.length;
		int newCapacity = oldCapacity + (oldCapacity >> 1);
		if (newCapacity - minCapacity < 0)
			newCapacity = minCapacity;
		if (newCapacity - MAX_ARRAY_SIZE > 0)
			newCapacity = hugeCapacity(minCapacity);
		// minCapacity is usually close to size, so this is a win:
		elementData = Arrays.copyOf(elementData, newCapacity);
	}

	private static int hugeCapacity(int minCapacity) {
		if (minCapacity < 0) // overflow
			throw new OutOfMemoryError();
		return (minCapacity > MAX_ARRAY_SIZE) ? Integer.MAX_VALUE : MAX_ARRAY_SIZE;
	}

	/**
	 * Returns the number of elements in this list.
	 *
	 * @return the number of elements in this list
	 */
	public int size() {
		return size;
	}

	/**
	 * Returns <tt>true</tt> if this list contains no elements.
	 *
	 * @return <tt>true</tt> if this list contains no elements
	 */
	public boolean isEmpty() {
		return size == 0;
	}

	/**
	 * Returns <tt>true</tt> if this list contains the specified element. More
	 * formally, returns <tt>true</tt> if and only if this list contains at least
	 * one element <tt>e</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;e==null&nbsp;:&nbsp;o.equals(e))</tt>.
	 *
	 * @param o
	 *            element whose presence in this list is to be tested
	 * @return <tt>true</tt> if this list contains the specified element
	 */
	public boolean contains(int o) {
		return indexOf(o) >= 0;
	}

	/**
	 * Returns the index of the first occurrence of the specified element in this
	 * list, or -1 if this list does not contain the element. More formally, returns
	 * the lowest index <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>, or
	 * -1 if there is no such index.
	 */
	public int indexOf(int o) {
		for (int i = 0; i < size; i++)
			if (o == elementData[i])
				return i;
		return -1;
	}

	/**
	 * Returns the index of the last occurrence of the specified element in this
	 * list, or -1 if this list does not contain the element. More formally, returns
	 * the highest index <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt>, or
	 * -1 if there is no such index.
	 */
	public int lastIndexOf(int o) {
		for (int i = size - 1; i >= 0; i--)
			if (o == elementData[i])
				return i;
		return -1;
	}

	/**
	 * Returns a shallow copy of this <tt>IntList</tt> instance. (The elements
	 * themselves are not copied.)
	 *
	 * @return a clone of this <tt>IntList</tt> instance
	 */
	public Object clone() {
		try {
			IntList v = (IntList) super.clone();
			v.elementData = Arrays.copyOf(elementData, size);
			return v;
		} catch (CloneNotSupportedException e) {
			// this shouldn't happen, since we are Cloneable
			throw new InternalError(e);
		}
	}

	/**
	 * Returns an array containing all of the elements in this list in proper
	 * sequence (from first to last element).
	 *
	 * <p>
	 * The returned array will be "safe" in that no references to it are maintained
	 * by this list. (In other words, this method must allocate a new array). The
	 * caller is thus free to modify the returned array.
	 *
	 * <p>
	 * This method acts as bridge between array-based and collection-based APIs.
	 *
	 * @return an array containing all of the elements in this list in proper
	 *         sequence
	 */
	public int[] toArray() {
		return Arrays.copyOf(elementData, size);
	}

	/**
	 * Returns an array containing all of the elements in this list in proper
	 * sequence (from first to last element); the runtime type of the returned array
	 * is that of the specified array. If the list fits in the specified array, it
	 * is returned therein. Otherwise, a new array is allocated with the runtime
	 * type of the specified array and the size of this list.
	 *
	 * <p>
	 * If the list fits in the specified array with room to spare (i.e., the array
	 * has more elements than the list), the element in the array immediately
	 * following the end of the collection is set to <tt>null</tt>. (This is useful
	 * in determining the length of the list <i>only</i> if the caller knows that
	 * the list does not contain any null elements.)
	 *
	 * @param a
	 *            the array into which the elements of the list are to be stored, if
	 *            it is big enough; otherwise, a new array of the same runtime type
	 *            is allocated for this purpose.
	 * @return an array containing the elements of the list
	 * @throws ArrayStoreException
	 *             if the runtime type of the specified array is not a supertype of
	 *             the runtime type of every element in this list
	 * @throws NullPointerException
	 *             if the specified array is null
	 */
	public int[] toArray(int[] a) {
		if (a.length < size)
			// Make a new array of a's runtime type, but my contents:
			return Arrays.copyOf(elementData, size);
		System.arraycopy(elementData, 0, a, 0, size);
		if (a.length > size)
			a[size] = 0;
		return a;
	}

	// Positional Access Operations
	int elementData(int index) {
		return elementData[index];
	}

	/**
	 * Returns the element at the specified position in this list.
	 *
	 * @param index
	 *            index of the element to return
	 * @return the element at the specified position in this list
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public int get(int index) {
		rangeCheck(index);

		return elementData(index);
	}

	/**
	 * Replaces the element at the specified position in this list with the
	 * specified element.
	 *
	 * @param index
	 *            index of the element to replace
	 * @param element
	 *            element to be stored at the specified position
	 * @return the element previously at the specified position
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public int set(int index, int element) {
		rangeCheck(index);

		int oldValue = elementData(index);
		elementData[index] = element;
		return oldValue;
	}

	/**
	 * Appends the specified element to the end of this list.
	 *
	 * @param e
	 *            element to be appended to this list
	 * @return <tt>true</tt> (as specified by {@link Collection#add})
	 */
	public boolean add(int e) {
		ensureCapacityInternal(size + 1); // Increments modCount!!
		elementData[size++] = e;
		return true;
	}

	/**
	 * Inserts the specified element at the specified position in this list. Shifts
	 * the element currently at that position (if any) and any subsequent elements
	 * to the right (adds one to their indices).
	 *
	 * @param index
	 *            index at which the specified element is to be inserted
	 * @param element
	 *            element to be inserted
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public void add(int index, int element) {
		rangeCheckForAdd(index);

		ensureCapacityInternal(size + 1); // Increments modCount!!
		System.arraycopy(elementData, index, elementData, index + 1, size - index);
		elementData[index] = element;
		size++;
	}

	/**
	 * Removes the element at the specified position in this list. Shifts any
	 * subsequent elements to the left (subtracts one from their indices).
	 *
	 * @param index
	 *            the index of the element to be removed
	 * @return the element that was removed from the list
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 */
	public int remove(int index, boolean justAflag) {
		rangeCheck(index);

		int oldValue = elementData(index);

		int numMoved = size - index - 1;
		if (numMoved > 0)
			System.arraycopy(elementData, index + 1, elementData, index, numMoved);
		elementData[--size] = 0; // clear to let GC do its work

		return oldValue;
	}

	/**
	 * Removes the first occurrence of the specified element from this list, if it
	 * is present. If the list does not contain the element, it is unchanged. More
	 * formally, removes the element with the lowest index <tt>i</tt> such that
	 * <tt>(o==null&nbsp;?&nbsp;get(i)==null&nbsp;:&nbsp;o.equals(get(i)))</tt> (if
	 * such an element exists). Returns <tt>true</tt> if this list contained the
	 * specified element (or equivalently, if this list changed as a result of the
	 * call).
	 *
	 * @param o
	 *            element to be removed from this list, if present
	 * @return <tt>true</tt> if this list contained the specified element
	 */
	public boolean remove(int o) {
		for (int index = 0; index < size; index++)
			if (o == elementData[index]) {
				fastRemove(index);
				return true;
			}
		return false;
	}

	/*
	 * Private remove method that skips bounds checking and does not return the
	 * value removed.
	 */
	private void fastRemove(int index) {
		int numMoved = size - index - 1;
		if (numMoved > 0)
			System.arraycopy(elementData, index + 1, elementData, index, numMoved);
		elementData[--size] = 0; // clear to let GC do its work
	}

	/**
	 * Removes all of the elements from this list. The list will be empty after this
	 * call returns.
	 */
	public void clear() {
		// clear to let GC do its work
		for (int i = 0; i < size; i++)
			elementData[i] = 0;

		size = 0;
	}

	/**
	 * Appends all of the elements in the specified collection to the end of this
	 * list, in the order that they are returned by the specified collection's
	 * Iterator. The behavior of this operation is undefined if the specified
	 * collection is modified while the operation is in progress. (This implies that
	 * the behavior of this call is undefined if the specified collection is this
	 * list, and this list is nonempty.)
	 *
	 * @param c
	 *            collection containing elements to be added to this list
	 * @return <tt>true</tt> if this list changed as a result of the call
	 * @throws NullPointerException
	 *             if the specified collection is null
	 */
	public boolean addAll(int[] c) {
		int numNew = c.length;
		ensureCapacityInternal(size + numNew); // Increments modCount
		System.arraycopy(c, 0, elementData, size, numNew);
		size += numNew;
		return numNew != 0;
	}

	/**
	 * Inserts all of the elements in the specified collection into this list,
	 * starting at the specified position. Shifts the element currently at that
	 * position (if any) and any subsequent elements to the right (increases their
	 * indices). The new elements will appear in the list in the order that they are
	 * returned by the specified collection's iterator.
	 *
	 * @param index
	 *            index at which to insert the first element from the specified
	 *            collection
	 * @param c
	 *            collection containing elements to be added to this list
	 * @return <tt>true</tt> if this list changed as a result of the call
	 * @throws IndexOutOfBoundsException
	 *             {@inheritDoc}
	 * @throws NullPointerException
	 *             if the specified collection is null
	 */
	public boolean addAll(int index, int[] c) {
		rangeCheckForAdd(index);

		int numNew = c.length;
		ensureCapacityInternal(size + numNew); // Increments modCount

		int numMoved = size - index;
		if (numMoved > 0)
			System.arraycopy(elementData, index, elementData, index + numNew, numMoved);

		System.arraycopy(c, 0, elementData, index, numNew);
		size += numNew;
		return numNew != 0;
	}

	/**
	 * Removes from this list all of the elements whose index is between
	 * {@code fromIndex}, inclusive, and {@code toIndex}, exclusive. Shifts any
	 * succeeding elements to the left (reduces their index). This call shortens the
	 * list by {@code (toIndex - fromIndex)} elements. (If
	 * {@code toIndex==fromIndex}, this operation has no effect.)
	 *
	 * @throws IndexOutOfBoundsException
	 *             if {@code fromIndex} or {@code toIndex} is out of range
	 *             ({@code fromIndex < 0 ||
	 *          fromIndex >= size() ||
	 *          toIndex > size() ||
	 *          toIndex < fromIndex})
	 */
	protected void removeRange(int fromIndex, int toIndex) {
		int numMoved = size - toIndex;
		System.arraycopy(elementData, toIndex, elementData, fromIndex, numMoved);

		// clear to let GC do its work
		int newSize = size - (toIndex - fromIndex);
		for (int i = newSize; i < size; i++) {
			elementData[i] = 0;
		}
		size = newSize;
	}

	/**
	 * Checks if the given index is in range. If not, throws an appropriate runtime
	 * exception. This method does *not* check if the index is negative: It is
	 * always used immediately prior to an array access, which throws an
	 * ArrayIndexOutOfBoundsException if index is negative.
	 */
	private void rangeCheck(int index) {
		if (index >= size)
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	/**
	 * A version of rangeCheck used by add and addAll.
	 */
	private void rangeCheckForAdd(int index) {
		if (index > size || index < 0)
			throw new IndexOutOfBoundsException(outOfBoundsMsg(index));
	}

	/**
	 * Constructs an IndexOutOfBoundsException detail message. Of the many possible
	 * refactorings of the error handling code, this "outlining" performs best with
	 * both server and client VMs.
	 */
	private String outOfBoundsMsg(int index) {
		return "Index: " + index + ", Size: " + size;
	}
}
