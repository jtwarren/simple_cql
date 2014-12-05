package simpledb;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     * */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         * */
        public final Type fieldType;
        
        /**
         * The name of the field
         * */
        public final String fieldName;

        public TDItem(Type t, String n) {
            this.fieldName = n;
            this.fieldType = t;
        }

        public String toString() {
            return fieldName + "(" + fieldType + ")";
        }
    }

    /**
     * @return
     *        An iterator which iterates over all the field TDItems
     *        that are included in this TupleDesc
     * */
    public Iterator<TDItem> iterator() {
        return Arrays.asList(tupleItems).iterator();
    }

    private static final long serialVersionUID = 1L;
    private TDItem[] tupleItems;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields. If time stamp is present
     * it must be the last element of the type array.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     * @param fieldAr
     *            array specifying the names of the fields. Note that names may
     *            be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
    	// First verify that the lengths of the two lists are equal
    	if (typeAr.length != fieldAr.length) {
    		throw new IllegalArgumentException();
    	}
    	
    	int n = typeAr.length;
    	tupleItems = new TDItem[n];
    	for (int i = 0; i < n; i++) {
    		tupleItems[i] = new TDItem(typeAr[i], fieldAr[i]);
    	}
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     * 
     * @param typeAr
     *            array specifying the number of and types of fields in this
     *            TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
    	int n = typeAr.length;
    	tupleItems = new TDItem[n];
    	for (int i = 0; i < n; i++) {
    		tupleItems[i] = new TDItem(typeAr[i], "");
    	}
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        return tupleItems.length;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     * 
     * @param i
     *            index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        if (i >= tupleItems.length) {
        	throw new NoSuchElementException();
        }
        if (tupleItems[i].fieldName.equals("")) {
        	return null;
        }
        return tupleItems[i].fieldName;
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     * 
     * @param i
     *            The index of the field to get the type of. It must be a valid
     *            index.
     * @return the type of the ith field
     * @throws NoSuchElementException
     *             if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
    	if (i >= tupleItems.length) {
        	throw new NoSuchElementException();
        }
        return tupleItems[i].fieldType;
    }

    /**
     * Find the index of the field with a given name.
     * 
     * @param name
     *            name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException
     *             if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
    	if (name == null) {
    		throw new NoSuchElementException();
    	}
        for (int i = 0; i < tupleItems.length; i++) {
        	if (name.equals(tupleItems[i].fieldName)) {
        		return i;
        	}
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     *         Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
    	int size = 0;
        for (TDItem tupleItem : tupleItems) {
        	size += tupleItem.fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     * 
     * @param td1
     *            The TupleDesc with the first fields of the new TupleDesc
     * @param td2
     *            The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        Type[] typeAr = new Type[td1.numFields() + td2.numFields()];
        String[] fieldAr = new String[td1.numFields() + td2.numFields()];
        
        for (int i = 0; i < td1.numFields(); i++) {
        	TDItem tupleItem = td1.tupleItems[i];
        	typeAr[i] = tupleItem.fieldType;
        	fieldAr[i] = tupleItem.fieldName;
        }
        
        for (int i = 0; i < td2.numFields(); i++) {
        	TDItem tupleItem = td2.tupleItems[i];
        	typeAr[td1.numFields() + i] = tupleItem.fieldType;
        	fieldAr[td1.numFields() + i] = tupleItem.fieldName;
        }
        
        return new TupleDesc(typeAr, fieldAr);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they are the same size and if the n-th
     * type in this TupleDesc is equal to the n-th type in td.
     * 
     * @param o
     *            the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */
    @Override
    public boolean equals(Object o) {
        if (o == null) {
        	return false;
        }

        if (this.getClass() != o.getClass()) {
            return false;
        }

        TupleDesc other = (TupleDesc) o;
        if (this.numFields() != other.numFields()) {
        	return false;
        }
        
        int n = this.numFields();
        for (int i = 0; i < n; i++) {
        	if (!this.tupleItems[i].fieldType.equals(other.tupleItems[i].fieldType)) {
        		return false;
        	}
        }
        return true;
    }

    @Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.hashCode(tupleItems);
		return result;
	}

	/**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     * 
     * @return String describing this descriptor.
     */
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < numFields(); i++) {
        	String tupleString = tupleItems[i].toString();
        	if (i == 0) {
        		sb.append(tupleString);
        	} else {
        		sb.append(", " + tupleString);
        	}
        }
        return sb.toString();
    }
}
