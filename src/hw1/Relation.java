package hw1;

import java.util.ArrayList;
import java.util.Arrays;



//Authored by 
//
//Melena Braggs and Courtney Fenderson


/**
 * This class provides methods to perform relational algebra operations. It will be used
 * to implement SQL queries.
 * @author Doug Shook
 *
 */
public class Relation {

	private ArrayList<Tuple> tuples;
	private TupleDesc td;

	public Relation(ArrayList<Tuple> l, TupleDesc td) {
		//your code here
		this.tuples = l;
		this.td = td;
	}

	/**
	 * This method performs a select operation on a relation
	 * @param field number (refer to TupleDesc) of the field to be compared, left side of comparison
	 * @param op the comparison operator
	 * @param operand a constant to be compared against the given column
	 * @return
	 */
	public Relation select(int field, RelationalOperator op, Field operand) {
		//your code here

		// if index can't be found in the array and there's a op and a 

		if ( (td.numFields() < field) || (op == null) || (operand == null) ) {
			return null; 
		}


		// the tuples that match the select we are looking for
		ArrayList<Tuple> theList = new ArrayList<>(); 

		for (Tuple t : tuples) {
			if (t.getField(field).compare(op, operand)) {
				theList.add(t); 
			}
		}
		this.tuples = theList; 

		return this; 
	}

	/**
	 * This method performs a rename operation on a relation
	 * @param fields the field numbers (refer to TupleDesc) of the fields to be renamed
	 * @param names a list of new names. The order of these names is the same as the order of field numbers in the field list
	 * @return
	 */
	public Relation rename(ArrayList<Integer> fields, ArrayList<String> names) {
		//your code here

		// for each field that needs to be renamed, rename it 
		
		
		String[] newNames = new String[td.numFields()];
		Type[] types = new Type[td.numFields()];
		
		TupleDesc newTD = new TupleDesc(types, newNames);
		
		for(int i = 0; i < td.numFields(); i++){
			types[i] = td.getType(i);
			newNames[i] = td.getFieldName(i);
			
			for(int j = 0; j < fields.size(); j++){
				
				if(fields.get(j) == i){
					newNames[i] = names.get(j);
					break;
				}
			}
		}
		
		return new Relation(this.tuples, newTD);

	
	}

	/**
	 * This method performs a project operation on a relation
	 * @param fields a list of field numbers (refer to TupleDesc) that should be in the result
	 * @return
	 */
	public Relation project(ArrayList<Integer> fields) {
		//your code here

		String[] newField = new String[fields.size()];
		Type[] newType = new Type[fields.size()];
		
		//update the names n types
		for(int i = 0; i < fields.size(); i++){
			newField[i] = td.getFieldName(fields.get(i));
			newType[i] = td.getType(fields.get(i));
		}
		
		
		TupleDesc newTD = new TupleDesc(newType, newField);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		// create tuples and add them in
		for(int i = 0; i < tuples.size(); i++){
			Tuple t = new Tuple(newTD);
			
			for(int j = 0; j < newTD.numFields(); j++){
				t.setField(j, tuples.get(i).getField(fields.get(j)));
			}
			newTuples.add(t);
		}
		
			
		return new Relation(newTuples, newTD);
	}




	/**
	 * This method performs a join between this relation and a second relation.
	 * The resulting relation will contain all of the columns from both of the given relations,
	 * joined using the equality operator (=)
	 * @param other the relation to be joined
	 * @param field1 the field number (refer to TupleDesc) from this relation to be used in the join condition
	 * @param field2 the field number (refer to TupleDesc) from other to be used in the join condition
	 * @return
	 */
	public Relation join(Relation other, int field1, int field2) {
		
		
		String[] f1 = td.copyFields();
		String[] f2 = other.td.copyFields();
		
		Type[] t1 = td.copyTypes();
		Type[] t2 = other.td.copyTypes();
	
		String[] fields = Arrays.copyOf(f1, f1.length + f2.length); 	//using java2 help for import of Arrays
		System.arraycopy(f2, 0, fields, f1.length, f2.length); 
		Type[]types = Arrays.copyOf(t1, t1.length + t2.length);
		System.arraycopy(t2, 0, types, t1.length, t2.length);
		
		TupleDesc newTd = new TupleDesc(types, fields);
		ArrayList<Tuple> newTuples = new ArrayList<>();
		for (Tuple tup1: tuples) {
			for (Tuple tup2: other.tuples) {
				if (tup1.getField(field1).compare(RelationalOperator.EQ, tup2.getField(field2))) {
					Tuple newTuple = new Tuple(newTd);
					for (int i = 0; i < f1.length; i++) newTuple.setField(i, tup1.getField(i));
					for (int i = 0; i < f2.length; i++) newTuple.setField(i, tup2.getField(i));
					newTuples.add(newTuple);
				}
			}
		}
		return new Relation(newTuples, newTd);
	}
	

	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		
		Aggregator agg = new Aggregator(op, groupBy, td); 
		
		for(int i = 0; i < tuples.size(); i++){
			agg.merge(tuples.get(i));
			//System.out.println(tuples.get(i).toString());
		}
		ArrayList<Tuple> newtuples = agg.getResults();
		
		return new Relation(newtuples, td);
	
	}

	public TupleDesc getDesc() {
		//your code here
		return this.td;
	}

	public ArrayList<Tuple> getTuples() {
		//your code here
		return this.tuples;
	}

	/**
	 * Returns a string representation of this relation. The string representation should
	 * first contain the TupleDesc, followed by each of the tuples in this relation
	 */
	public String toString() {
		//your code here
		
		String theString = "";
		for(int i = 0; i < tuples.size(); i++){
			theString += tuples.get(i).toString() + "\n";
		}
		return theString;
	}
}
