package simpledb;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    
    private int gbfield;
    private Type gbfieldtype;
    private Op what;
    
    private HashMap<Field, Integer> groups;

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        if (what != Op.COUNT)
        	throw new IllegalArgumentException("Illegal argument!");
        this.what = what;
        
        groups = new HashMap<Field, Integer>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
    	Field groupField = null;
        if (gbfield != NO_GROUPING)
        	groupField = tup.getField(gbfield);
        if (!groups.containsKey(groupField)) {
        	groups.put(groupField, 0);
        }
        int curAggrValue = groups.get(groupField);
        if (what == Op.COUNT) {
        	// Update count as necessary
        	groups.put(groupField, curAggrValue + 1);
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
    	List<Tuple> aggregatedTupleResults = new ArrayList<Tuple>();
    	
    	TupleDesc td;
    	if (gbfield == NO_GROUPING) {
    		td = new TupleDesc(new Type[] { Type.INT_TYPE });
    	} else {
    		td = new TupleDesc(new Type[] { gbfieldtype, Type.INT_TYPE });
    	}
    	
    	for (Field f : groups.keySet()) {
    		int aggrField = groups.get(f);
    		
    		Tuple tuple = new Tuple(td);
    		if (gbfield == NO_GROUPING) {
    			tuple.setField(0, new IntField(aggrField));
    		} else {
    			tuple.setField(0, f);
    			tuple.setField(1, new IntField(aggrField));
    		}
    		aggregatedTupleResults.add(tuple);
    	}
    	
    	return new AggregateIterator(aggregatedTupleResults, td);
    }

}
