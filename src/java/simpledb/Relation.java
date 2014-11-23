package simpledb;

public class Relation {
	
	private DbIterator itr;

	public Relation(DbIterator itr) {
		this.itr = itr;
	}
	
	public DbIterator iterator() {
		return this.itr;
	}
}
