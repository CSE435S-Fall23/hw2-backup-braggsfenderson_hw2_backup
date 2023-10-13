package hw1;

import java.sql.Types;
import java.util.HashMap;

/**
 * This class represents a tuple that will contain a single row's worth of information
 * from a table. It also includes information about where it is stored
 * @author Sam Madden modified by Doug Shook
 *
 */
public class Tuple {
	private TupleDesc TD;  // how to do the mapping
	private int pageID; 
	private int tupID; 
	private Field[] fields; 
	
	/**
	 * Creates a new tuple with the given description
	 * @param t the schema for this tuple
	 */
	public Tuple(TupleDesc t) {
		 this.TD = t; 
		 this.fields = new Field[t.numFields()] ;
	}
	
	public TupleDesc getDesc() {
		//your code here
		return this.TD;
	}
	
	/**
	 * retrieves the page id where this tuple is stored
	 * @return the page id of this tuple
	 */
	public int getPid() {
		//your code here
		return this.pageID;
	}

	public void setPid(int pid) {
		//your code here
		this.pageID = pid; 
	}
	

	/**
	 * retrieves the tuple (slot) id of this tuple
	 * @return the slot where this tuple is stored
	 */
	public int getId() {
		//your code here
	 return this.tupID;
	}

	public void setId(int id) {
		//your code here
		 this.tupID = id; 
	}
	
	public void setDesc(TupleDesc td) {
		//your code here;
		
		this.TD = td; 
	}
	
	/**
	 * Stores the given data at the i-th field
	 * @param i the field number to store the data
	 * @param v the data
	 */
	public void setField(int i, Field v) {
		//your code here
		this.fields[i] = v; 
	}
	
	public Field getField(int i) {
		//your code here
		return fields[i];
	}
	
	/**
	 * Creates a string representation of this tuple that displays its contents.
	 * You should convert the binary data into a readable format (i.e. display the ints in base-10 and convert
	 * the String columns to readable text).
	 */
	public String toString() {
		String theString = ""; 
		for (int i = 0; i < fields.length; i++) {
			// type.int is how we ge to that enum
			if (this.TD.getType(i) == Type.INT) {
				theString += (IntField)fields[i]; 
			}
			
			else {
				theString += (StringField)fields[i]; 
			}
		}
		return theString;
	}
	
}
	