package simpledb.storage;

import simpledb.common.Type;

import java.io.Serializable;
import java.util.*;

/**
 * TupleDesc describes the schema of a tuple.
 */
public class TupleDesc implements Serializable {

    /**
     * A help class to facilitate organizing the information of each field
     */
    public static class TDItem implements Serializable {

        private static final long serialVersionUID = 1L;

        /**
         * The type of the field
         */
        public final Type fieldType;

        /**
         * The name of the field
         */
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
     * @return An iterator which iterates over all the field TDItems
     * that are included in this TupleDesc
     */
    public Iterator<TDItem> iterator() {
        // some code goes here
        return new TupleItr();
    }

    private class TupleItr implements Iterator<TDItem> {
        private int index = 0;

        @Override
        public boolean hasNext() {
            if (index < num) {
                return true;
            }
            return false;
        }

        @Override
        public TDItem next() {
            if (index < num) {
                return elementData[index++];
            }
            return null;
        }
    }

    private static final long serialVersionUID = 1L;

    private TDItem[] elementData;
    private int num;

    /**
     * Create a new TupleDesc with typeAr.length fields with fields of the
     * specified types, with associated named fields.
     *
     * @param typeAr  array specifying the number of and types of fields in this
     *                TupleDesc. It must contain at least one entry.
     * @param fieldAr array specifying the names of the fields. Note that names may
     *                be null.
     */
    public TupleDesc(Type[] typeAr, String[] fieldAr) {
        // some code goes here
        num = typeAr.length;
        int len = fieldAr.length;
        elementData = new TDItem[num];
        for (int i = 0; i < num; i++) {
            elementData[i] = new TDItem(typeAr[i], i < len ? fieldAr[i] : "");
        }
    }

    /**
     * Constructor. Create a new tuple desc with typeAr.length fields with
     * fields of the specified types, with anonymous (unnamed) fields.
     *
     * @param typeAr array specifying the number of and types of fields in this
     *               TupleDesc. It must contain at least one entry.
     */
    public TupleDesc(Type[] typeAr) {
        // some code goes here
        num = typeAr.length;
        elementData = new TDItem[num];
        for (int i = 0; i < num; i++) {
            elementData[i] = new TDItem(typeAr[i], "");
        }
    }

    /**
     * @return the number of fields in this TupleDesc
     */
    public int numFields() {
        // some code goes here
        return num;
    }

    /**
     * Gets the (possibly null) field name of the ith field of this TupleDesc.
     *
     * @param i index of the field name to return. It must be a valid index.
     * @return the name of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public String getFieldName(int i) throws NoSuchElementException {
        // some code goes here
        return i < num ? elementData[i].fieldName : "";
    }

    /**
     * Gets the type of the ith field of this TupleDesc.
     *
     * @param i The index of the field to get the type of. It must be a valid
     *          index.
     * @return the type of the ith field
     * @throws NoSuchElementException if i is not a valid field reference.
     */
    public Type getFieldType(int i) throws NoSuchElementException {
        // some code goes here
        return i < num ? elementData[i].fieldType : null;
    }

    /**
     * Find the index of the field with a given name.
     *
     * @param name name of the field.
     * @return the index of the field that is first to have the given name.
     * @throws NoSuchElementException if no field with a matching name is found.
     */
    public int fieldNameToIndex(String name) throws NoSuchElementException {
        // some code goes here
        for (int i = 0; i < num; i++) {
            if (name.equals(elementData[i])) {
                return i;
            }
        }
        throw new NoSuchElementException();
    }

    /**
     * @return The size (in bytes) of tuples corresponding to this TupleDesc.
     * Note that tuples from a given TupleDesc are of a fixed size.
     */
    public int getSize() {
        // some code goes here
        int size = 0;
        for (int i = 0; i < num; i++) {
            size += elementData[i].fieldType.getLen();
        }
        return size;
    }

    /**
     * Merge two TupleDescs into one, with td1.numFields + td2.numFields fields,
     * with the first td1.numFields coming from td1 and the remaining from td2.
     *
     * @param td1 The TupleDesc with the first fields of the new TupleDesc
     * @param td2 The TupleDesc with the last fields of the TupleDesc
     * @return the new TupleDesc
     */
    public static TupleDesc merge(TupleDesc td1, TupleDesc td2) {
        // some code goes here
        int n1 = td1.num, n2 = td2.num, mergedNum = n1 + n2;
        Type[] types = new Type[mergedNum];
        String[] fields = new String[mergedNum];
        int p = 0;
        for (int i = 0; i < n1; i++) {
            types[p] = td1.elementData[i].fieldType;
            fields[p++] = td1.elementData[i].fieldName;
        }
        for (int i = 0; i < n2; i++) {
            types[p] = td2.elementData[i].fieldType;
            fields[p++] = td2.elementData[i].fieldName;
        }
        return new TupleDesc(types, fields);
    }

    /**
     * Compares the specified object with this TupleDesc for equality. Two
     * TupleDescs are considered equal if they have the same number of items
     * and if the i-th type in this TupleDesc is equal to the i-th type in o
     * for every i.
     *
     * @param o the Object to be compared for equality with this TupleDesc.
     * @return true if the object is equal to this TupleDesc.
     */

    public boolean equals(Object o) {
        // some code goes here
        if (o == null) {
            return false;
        }
        if (!(o instanceof TupleDesc)) {
            return false;
        }
        if (o == this) {
            return true;
        }
        TupleDesc desc = (TupleDesc) o;
        if (desc.num != this.num) {
            return false;
        }
        for (int i = 0; i < this.num; i++) {
            if (this.elementData[i].fieldType != desc.elementData[i].fieldType) {
                return false;
            }
        }
        return true;
    }

    public int hashCode() {
        // If you want to use TupleDesc as keys for HashMap, implement this so
        // that equal objects have equals hashCode() results
        throw new UnsupportedOperationException("unimplemented");
    }

    /**
     * Returns a String describing this descriptor. It should be of the form
     * "fieldType[0](fieldName[0]), ..., fieldType[M](fieldName[M])", although
     * the exact format does not matter.
     *
     * @return String describing this descriptor.
     */
    public String toString() {
        // some code goes here
        StringJoiner sb = new StringJoiner(",");
        for (int i = 0; i < num; i++) {
            sb.add(elementData[i].fieldType.toString() + "(" + elementData[i].fieldName + ")");
        }
        return sb.toString();
    }
}
