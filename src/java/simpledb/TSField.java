package simpledb;

import java.io.*;

/**
 * Instance of Field that stores a single integer.
 */
public class TSField implements Field {
    
	private static final long serialVersionUID = 1L;
	
	private final int value;

    public int getValue() {
        return value;
    }

    /**
     * Constructor.
     *
     * @param ts The value of this field.
     */
    public TSField(int ts) {
        value = ts;
    }

    public String toString() {
        return Integer.toString(value);
    }

    public int hashCode() {
        return value;
    }

    public boolean equals(Object field) {
        return ((TSField) field).value == value;
    }

    public void serialize(DataOutputStream dos) throws IOException {
        dos.writeInt(value);
    }

    /**
     * Compare the specified field to the value of this Field.
     * Return semantics are as specified by Field.compare
     *
     * @throws IllegalCastException if val is not an IntField
     * @see Field#compare
     */
    public boolean compare(Predicate.Op op, Field val) {

        TSField tsVal = (TSField) val;

        switch (op) {
        case EQUALS:
            return value == tsVal.value;
        case NOT_EQUALS:
            return value != tsVal.value;

        case GREATER_THAN:
            return value > tsVal.value;

        case GREATER_THAN_OR_EQ:
            return value >= tsVal.value;

        case LESS_THAN:
            return value < tsVal.value;

        case LESS_THAN_OR_EQ:
            return value <= tsVal.value;
        }

        return false;
    }

    /**
     * Return the Type of this field.
     * @return Type.INT_TYPE
     */
	public Type getType() {
		return Type.TS_TYPE;
	}
}
