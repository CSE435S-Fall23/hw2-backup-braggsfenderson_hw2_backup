package hw1;

import java.util.ArrayList;

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





		for (int i= 0; i< td.numFields(); i++) {
			// if we are a field that needs to be renamed 
			if (fields.contains(i)) {
				String newName = names.get(i);


			}
		}


		return null;
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
		//your code here
		
		// bring things together from each relation
		
		String[] joinNames = new String[td.numFields()+ other.td.numFields()]; 
		Type[] joinTypes = new Type[td.numFields() + other.td.numFields()]; 
		
		TupleDesc joinTD = new TupleDesc(joinTypes,joinNames); 
		
		
		
		for (int i = 0; i < td.numFields() + other.td.numFields(); i++) {
			
			if (i < td.numFields()) {
			joinNames[i] = td.getFieldName(i);
			
			joinTypes[i] = td.getType(i);
		}
			else {
				joinNames[i] = td.getFieldName(i-td.numFields());
				
				joinTypes[i] = td.getType(i-td.numFields());
			}
			
		}
			
		ArrayList<Tuple> newTuples = new ArrayList<>();
		
		for (Tuple t1: tuples) {
			for (Tuple t2: other.tuples) {
				if (t1.getField(field1).compare(RelationalOperator.EQ, t2.getField(field2))) {
					
					Tuple newTuple = new Tuple(joinTD);
					
					for (int i = 0; i < td.numFields() + other.td.numFields(); i++) {
						if (i < td.numFields()) {
						newTuple.setField(i, t1.getField(i));
						}
						
						else {
							newTuple.setField(i-td.numFields(), t1.getField(i-td.numFields())); 
						}
	
					} 
					
					newTuples.add(newTuple);
				}
			}
		}
			
		
		return new Relation(newTuples, joinTD);
	}

	/**
	 * Performs an aggregation operation on a relation. See the lab write up for details.
	 * @param op the aggregation operation to be performed
	 * @param groupBy whether or not a grouping should be performed
	 * @return
	 */
	public Relation aggregate(AggregateOperator op, boolean groupBy) {
		//your code here
		return null;
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
		return null;
	}
}
