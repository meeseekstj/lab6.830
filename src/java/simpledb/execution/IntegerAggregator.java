package simpledb.execution;

import simpledb.common.DbException;
import simpledb.common.Type;
import simpledb.storage.*;
import simpledb.transaction.TransactionAbortedException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;

    private int gbfield;
    private Type gbfieldtype;
    private int afield;
    private Op what;
    private Map<Field, List<Tuple>> group;
    private List<Tuple> noGroup;

    /**
     * Aggregate constructor
     *
     * @param gbfield     the 0-based index of the group-by field in the tuple, or
     *                    NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null
     *                    if there is no grouping
     * @param afield      the 0-based index of the aggregate field in the tuple
     * @param what        the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.what = what;
        if (gbfield == NO_GROUPING)
            noGroup = new ArrayList<>();
        else
            group = new ConcurrentHashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     *
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        if (gbfield == NO_GROUPING) {
            noGroup.add(tup);
        } else {
            Field field = tup.getField(gbfield);
            List<Tuple> od = group.getOrDefault(field, new ArrayList<>());
            od.add(tup);
            group.put(field, od);
        }
    }

    /**
     * Create a OpIterator over group aggregate results.
     *
     * @return a OpIterator whose tuples are the pair (groupVal, aggregateVal)
     * if using group, or a single (aggregateVal) if no grouping. The
     * aggregateVal is determined by the type of aggregate specified in
     * the constructor.
     */
    public OpIterator iterator() {
        // some code goes here
//        throw new UnsupportedOperationException("please implement me for lab2");
        TupleDesc td;
        List<Tuple> tups = new ArrayList<>();
        if (gbfield == NO_GROUPING) {
            String[] names = new String[]{"aggregateVal"};
            Type[] types = new Type[]{Type.INT_TYPE};
            td = new TupleDesc(types, names);
            Tuple tuple = new Tuple(td);
            tuple.setField(0, operateOneGroup(noGroup, what, afield));
            tups.add(tuple);
        } else {
            String[] names = new String[]{"groupVal", "aggregateVal"};
            Type[] types = new Type[]{gbfieldtype, Type.INT_TYPE};
            td = new TupleDesc(types, names);
            for (Map.Entry<Field, List<Tuple>> entry : group.entrySet()) {
                Tuple tuple = new Tuple(td);
                tuple.setField(0, entry.getKey());
                tuple.setField(1, operateOneGroup(entry.getValue(), what, afield));
                tups.add(tuple);
            }

        }
        return new TupleIterator(td, tups);
    }

    private Field operateOneGroup(List<Tuple> tups, Op what, int afield) {
        Integer result = null;
        switch (what) {
            case MIN:
                result = tups.stream().map(x -> {
                    IntField field = (IntField) x.getField(afield);
                    return field.getValue();
                }).min(Integer::compareTo).get();
                break;
            case MAX:
                result = tups.stream().map(x -> {
                    IntField field = (IntField) x.getField(afield);
                    return field.getValue();
                }).max(Integer::compareTo).get();
                break;
            case SUM:
                result = tups.stream().map(x -> {
                    IntField field = (IntField) x.getField(afield);
                    return field.getValue();
                }).reduce(Integer::sum).get();
                break;
            case AVG:
                result = tups.stream().map(x -> {
                    IntField field = (IntField) x.getField(afield);
                    return field.getValue();
                }).reduce(Integer::sum).get();
                result /= tups.size();
                break;
            case COUNT:
                result = tups.size();
                break;
            case SC_AVG:
                throw new UnsupportedOperationException("sc_avg");
            case SUM_COUNT:
                throw new UnsupportedOperationException("sum_count");
            default:
                throw new UnsupportedOperationException("unsupport operator");
        }
        return new IntField(result);
    }

}
