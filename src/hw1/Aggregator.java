package hw1;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;


//Authored by 
//
//Melena Braggs and Courtney Fenderson


/**
 * A class to perform various aggregations, by accepting one tuple at a time
 * @author Doug Shook
 *
 */
public class Aggregator {

	private AggregateOperator agop; 
	private boolean groupb; 
	private TupleDesc tD; 

	ArrayList<Tuple> tuple = new ArrayList<Tuple>();
	ArrayList<Tuple> help = new ArrayList<Tuple>();


	public Aggregator(AggregateOperator o, boolean groupBy, TupleDesc td) {
		//your code here

		this.agop = o; 
		this.groupb = groupBy; 
		this.tD = td; 

	}

	/**
	 * Merges the given tuple into the current aggregation
	 * @param t the tuple to be aggregated
	 */
	public void merge(Tuple t) {


		Type theType = t.getDesc().getType(0);
		Field theField = null;
		tuple.add(t);
		if(groupb == true){
			
			if(theType == Type.INT){
				if(help.contains(mergeHelper(t, theField, theType))){

				}
				else{
					help.add(mergeHelper(t, theField, theType));
				}
				help.add(mergeHelper(t, theField, theType));			
			}

			else{
				help.add(mergeHelper(t, theField, theType));
			}
		}
	}

	private Tuple mergeHelper(Tuple t, Field theField, Type theType) {
		byte[] info = t.getField(0).toByteArray();

		// creates a new field
		if(theType == Type.STRING) {
			theField = new StringField(info);
		}
		else {
			theField = new IntField(info);
		}
		Tuple newTuple = new Tuple(tD);
		
		// puts the field in the tuple
		newTuple.setField(0, theField);
		return newTuple;
	}

	/**
	 * Returns the result of the aggregation
	 * @return a list containing the tuples after aggregation
	 */
	public ArrayList<Tuple> getResults() {


		ArrayList<Tuple> result = new ArrayList<Tuple>();
		
		//what to do when it summed 
		if(agop == AggregateOperator.SUM) {
			int sum = 0;
			if(groupb == true) {

				ArrayList<Tuple> list = new ArrayList<>();
				Type[] types = new Type[] { tD.getType(0), Type.INT };
				String[] fields = new String[] { tD.getFieldName(0), "SUM" };
				TupleDesc newTD = new TupleDesc(types, fields);

				
				//both integers
				if (tD.getType(0) == Type.INT && tD.getType(1) == Type.INT) {
					Map<Integer, Integer> name = new HashMap<>(); 
					for(int i=0; i < tuple.size(); i++) {
						
						// 2 diff helpers for if it has the key or not 
						if (name.containsKey(tuple.get(i).getField(0).hashCode())) {
							sumHelperInt(tuple.get(i), name, list);
						}
						else {
							sumHelperIntb(i, list, name, newTD);
						}
					}
				} 
				
				//one string and one int
				else if (tD.getType(0) == Type.STRING && tD.getType(1) == Type.INT) {
					Map<String, Integer> name = new HashMap<>(); 
					for (int i=0; i<tuple.size(); i++) {
						
						if (name.containsKey(tuple.get(i).getField(0).toString())) {
							sumHelperString(tuple.get(i), name, list);
						} 
						else {
							sumHelperStringb(i, list, name, newTD);
						}
					}
				}
				return list;
			}

			else {		
				for(int i=0; i<tuple.size(); i++) {
					Type x = tuple.get(i).getDesc().getType(0);

					if(!x.equals(Type.STRING)) {
						int value = ((IntField) tuple.get(i).getField(0)).getValue();
						sum = sum + value;
					}
				}
				result.add(countHelperb(sum));
			}
		}

		if(agop == AggregateOperator.COUNT){

			if(groupb == false){
				int theResult = 0;
				int len = tuple.size();
				for(int i=0; i<len; i++){
					theResult++;
				}
				result.add(countHelperb(theResult));
			}
			else {
				// if we are grouping 
				for(int i=0; i<help.size(); i++) {
					int theResult = 0;
					
					for(int j=0; j<tuple.size(); j++) {
						if(tuple.get(j).equals(help.get(i))) {
							theResult++;
						}
						result.add(countHelper(i, theResult));
					}
				}
			}
		}



		if(agop == AggregateOperator.AVG) {
			
			if (groupb == true) {
				for(int i=0; i < help.size(); i++) {
					int sum = 0;
					
					Type x = help.get(i).getDesc().getType(0);

					if(!x.equals(Type.STRING)) {
						//if it's not a string it's an int 
						IntField intValueA = (IntField) help.get(i).getField(0);
						int actValueA = intValueA.getValue();
						
						for(int j=0; j < tuple.size(); j++) {
							
							Type y = tuple.get(j).getDesc().getType(0);
							if(y == Type.INT) {
								IntField intValueB = (IntField) tuple.get(j).getField(0);
								int actValueB = intValueB.getValue();
								if(actValueA == actValueB) {
									IntField comValue = (IntField) tuple.get(j).getField(1);
									int valb = comValue.getValue();
									sum = sum + valb;
									
								}
							}
						}
					}
					int ave = sum/(tuple.size());
					result.add(countHelper(i, ave));
				}
			} else {
				int sum = 0;
			
				Tuple ans = new Tuple(tD);
				for(int i=0; i< tuple.size(); i++) {
					
					int temp = tuple.get(i).getField(0).hashCode();
					sum = sum + temp;
				}

				int ave = sum / tuple.size();
				IntField intfield = new IntField(ave);
				ans.setField(0, intfield);
				
				// add the tuple with an int field that is the average to the result tuple list 
				result.add(ans);
			}
		}


		if(agop == AggregateOperator.MIN) {
			if(groupb == false) {
				int min = Integer.MIN_VALUE;

				String theString = "";
				Type x = tuple.get(0).getField(0).getType();
				if(!x.equals(Type.INT)) {

					theString = ((StringField) tuple.get(0).getField(0)).toString();
				}

				
				if(x.equals(Type.INT)) {
					result.add(maxMinInt(min));
				}else {
					result.add(maxMinString(theString));
				}
			}
			else {
				int min = Integer.MIN_VALUE;

				String theString = "";
				Type x = tuple.get(0).getField(0).getType();
				if(!x.equals(Type.INT)) {

					theString = ((StringField) tuple.get(0).getField(0)).toString();
				}

				for(int i=0; i<help.size(); i++) {
		
					if(x.equals(Type.INT)) {
						result.add(maxMinIntHelper(i, min));
					}
					else {
						result.add(maxMinStringHelper(i, theString));
					}
				}
			}
		}

		if(agop == AggregateOperator.MAX) {

			if(groupb == false) {
				int max = Integer.MAX_VALUE;
				String theString = "";
				Type x = tuple.get(0).getField(0).getType();
				
				if(!x.equals(Type.INT)) {
					theString = ((StringField) tuple.get(0).getField(0)).toString();
				}

				if(x.equals(Type.INT)) {
					result.add(maxMinInt(max));
				}
				
				else {
					result.add(maxMinString(theString));
				}
			}
			else {
				int max = Integer.MAX_VALUE;

				String theString = "";
				Type x = tuple.get(0).getField(0).getType();
				if(!x.equals(Type.INT)) {

					theString = ((StringField) tuple.get(0).getField(0)).toString();
				}

				for(int i=0; i<help.size(); i++) {
					
					if(x.equals(Type.INT)) {
						result.add(maxMinIntHelper(i, max));
					}else {
						result.add(maxMinStringHelper(i, theString));
					}
				}
			}
		}


		return result;
	}

	private Tuple maxMinIntHelper(int i, int maxMin){

		Tuple maxMinTup = new Tuple(tD);
		maxMinTup.setField(0, help.get(i).getField(0));
		IntField maxMinInt = new IntField(maxMin);
		maxMinTup.setField(1, maxMinInt);
		return maxMinTup;
	}


	private void valueHelper(int i, String theString) {
		StringField value = (StringField) tuple.get(i).getField(0);
		String theValue = value.toString();
		if(theValue.equals(theString)) {
			theString = theValue;
		}
	}

	

	private Tuple maxMinStringHelper(int i, String theString) {
		Tuple maxMinTuple = new Tuple(tD);
		maxMinTuple.setField(0, help.get(i).getField(0));
		StringField maxMinString = new StringField(theString);
		maxMinTuple.setField(1, maxMinString);
		return maxMinTuple;
	}	


	private Tuple maxMinString(String theString) {
		Tuple maxMinTuple = new Tuple(tD);
		StringField stringMaxMin = new StringField(theString);
		maxMinTuple.setField(0, stringMaxMin);
		return maxMinTuple;
	}

	private Tuple maxMinInt(int maxMin) {
		Tuple maxMinTuple = new Tuple(tD);
		IntField intMaxMin = new IntField(maxMin);
		maxMinTuple.setField(0, intMaxMin);
		return maxMinTuple;
	}


	private Tuple countHelper(int i, int given) {
		
		IntField x = new IntField(given);
		
		Tuple newTuple = new Tuple(tD);
		newTuple.setField(0, help.get(i).getField(0));
		newTuple.setField(1, x);
		return newTuple;
	}

	private Tuple countHelperb(int given) {
		IntField a = new IntField(given);
		
		Tuple newTuple = new Tuple(tD);
		newTuple.setField(0, a);
		return newTuple;
	}

	
	
	private void sumHelperString(Tuple t, Map<String, Integer> nameSpace, ArrayList<Tuple> list) {
		int index = nameSpace.get(t.getField(0).hashCode());
		Tuple temp = list.get(index);
		IntField theIF = new IntField(t.getField(1).hashCode() + temp.getField(1).hashCode());
		temp.setField(1, theIF);
	}

	
	// some of these helper could be if statement above but making them helpers makes the above code a bit less clunky
	private void sumHelperStringb(int i, ArrayList<Tuple> theList, Map<String, Integer> name, TupleDesc newTD) {
		Tuple newTuple = new Tuple(newTD);
		newTuple.setField(0, tuple.get(i).getField(0));
		newTuple.setField(1, tuple.get(i).getField(1));
		theList.add(newTuple);
		name.put(newTuple.getField(0).toString(), theList.size() - 1);
	}
	
	private void sumHelperInt(Tuple t, Map<Integer, Integer> name, ArrayList<Tuple> list) {
		
		int index = name.get(t.getField(0).hashCode());
		Tuple temp = list.get(index);
		IntField theIF = new IntField(t.getField(1).hashCode() + temp.getField(1).hashCode());

		temp.setField(1, theIF);
	}

	private void sumHelperIntb(int i, ArrayList<Tuple> list, Map<Integer, Integer> name, TupleDesc newTD) {
		Tuple newTuple = new Tuple(newTD);
		newTuple.setField(0, tuple.get(i).getField(0));
		newTuple.setField(1, tuple.get(i).getField(1));
		list.add(newTuple);
		name.put(newTuple.getField(0).hashCode(), list.size() - 1);
	}
	
	


}
