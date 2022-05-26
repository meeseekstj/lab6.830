package simpledb.storage;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Iterator;
import java.util.StringJoiner;

/**
 * Tuple maintains information about the contents of a tuple. Tuples have a
 * specified schema specified by a TupleDesc object and contain Field objects
 * with the data for each field.
 */
public class Tuple implements Serializable {

    private static final long serialVersionUID = 1L;
    private TupleDesc tupleDesc;
    private RecordId recordId;
    private Field[] fields;

    /**
     * Create a new tuple with the specified schema (type).
     *
     * @param td the schema of this tuple. It must be a valid TupleDesc
     *           instance with at least one field.
     */
    public Tuple(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
        fields = new Field[td.numFields()];
    }

    /**
     * @return The TupleDesc representing the schema of this tuple.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return tupleDesc;
    }

    /**
     * @return The RecordId representing the location of this tuple on disk. May
     * be null.
     */
    public RecordId getRecordId() {
        // some code goes here
        return recordId;
    }

    /**
     * Set the RecordId information for this tuple.
     *
     * @param rid the new RecordId for this tuple.
     */
    public void setRecordId(RecordId rid) {
        // some code goes here
        recordId = rid;
    }

    /**
     * Change the value of the ith field of this tuple.
     *
     * @param i index of the field to change. It must be a valid index.
     * @param f new value for the field.
     */
    public void setField(int i, Field f) {
        // some code goes here
        if (i < fields.length) {
            fields[i] = f;
        }
    }

    /**
     * @param i field index to return. Must be a valid index.
     * @return the value of the ith field, or null if it has not been set.
     */
    public Field getField(int i) {
        // some code goes here
        if (i < fields.length) {
            return fields[i];
        }
        return null;
    }

    /**
     * Returns the contents of this Tuple as a string. Note that to pass the
     * system tests, the format needs to be as follows:
     * <p>
     * column1\tcolumn2\tcolumn3\t...\tcolumnN
     * <p>
     * where \t is any whitespace (except a newline)
     */
    public String toString() {
        // some code goes here
//        throw new UnsupportedOperationException("Implement this");
        StringJoiner sj = new StringJoiner(" ");
        for (int i = 0; i < fields.length; i++) {
            sj.add(fields[i].toString());
        }
        return sj.toString();
    }

    /**
     * @return An iterator which iterates over all the fields of this tuple
     */
    public Iterator<Field> fields() {
        // some code goes here
        return new Iterator<Field>() {
            private int index = 0;

            @Override
            public boolean hasNext() {
                if (index < fields.length) {
                    return true;
                }
                return false;
            }

            @Override
            public Field next() {
                if (index < fields.length) {
                    return fields[index++];
                }
                return null;
            }
        };
    }

    /**
     * reset the TupleDesc of this tuple (only affecting the TupleDesc)
     */
    public void resetTupleDesc(TupleDesc td) {
        // some code goes here
        tupleDesc = td;
    }

    public static Tuple mergeTuple(Tuple t1, Tuple t2,TupleDesc td) {
        Tuple tuple = new Tuple(td);
        int i = 0;
        Iterator<Field> fields1 = t1.fields();
        while (fields1.hasNext()) {
            tuple.setField(i++, fields1.next());
        }
        Iterator<Field> fields2 = t2.fields();
        while (fields2.hasNext()) {
            tuple.setField(i++, fields2.next());
        }
        return tuple;
    }
}
