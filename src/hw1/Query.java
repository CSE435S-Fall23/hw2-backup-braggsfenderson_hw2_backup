package hw1;

import java.util.ArrayList;

import java.util.List;

import net.sf.jsqlparser.JSQLParserException;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.parser.*;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.*;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.util.TablesNamesFinder;

public class Query {

	private String q;
	
	public Query(String q) {
		this.q = q;
	}
	
	public Relation execute()  {
		Statement statement = null;
		try {
			statement = CCJSqlParserUtil.parse(q);
		} catch (JSQLParserException e) {
			System.out.println("Unable to parse query");
			e.printStackTrace();
		}
		Select selectStatement = (Select) statement;
		PlainSelect sb = (PlainSelect)selectStatement.getSelectBody();
		
		
		//your code here
		
		Catalog cat = Database.getCatalog();
		ColumnVisitor colVisitor = new ColumnVisitor();
		TablesNamesFinder tablesNamesFinder = new TablesNamesFinder();
		List<String> tableLi = tablesNamesFinder.getTableList(statement);
		int tbId = cat.getTableId(tableLi.get(0));
		ArrayList<Tuple> tupleList = cat.getDbFile(tbId).getAllTuples();
		TupleDesc originTd = cat.getTupleDesc(tbId);
		Relation r = new Relation(tupleList, originTd); //current rel
		
		// bring together
		Relation joinRel = r;
		List<Join> joinLi = sb.getJoins();

		if (joinLi != null) {
			for (Join join : joinLi) {
				//in order to get current rel
				String nameTable = join.getRightItem().toString();
				TupleDesc joinTupleDesc = cat.getTupleDesc(cat.getTableId(nameTable));
				ArrayList<Tuple> joinTupleLi = cat.getDbFile(cat.getTableId(nameTable)).getAllTuples();
				Relation newJoinRel = new Relation(joinTupleLi, joinTupleDesc);
				
				// deal w the input w expression
				String[] deal = join.getOnExpression().toString().split("=");
				String[] t1F = deal[0].trim().split("\\."); //field
				String[] t2F = deal[1].trim().split("\\.");

				String nameTable2 = t2F[0], fieldName1 = t1F[1], fieldName2 = t2F[1];
				
				// swap if not = name
				if (!nameTable.toLowerCase().equals(nameTable2.toLowerCase())) {
					String temp = fieldName1;
					fieldName1 = fieldName2;
					fieldName2 = temp;
				}
				
				int fieldIndex1 = joinRel.getDesc().nameToId(fieldName1);
				int fieldIndex2 = newJoinRel.getDesc().nameToId(fieldName2);
				joinRel = joinRel.join(newJoinRel, fieldIndex1, fieldIndex2);
			}
		}

		// for where
		Relation whereRel = joinRel;
		WhereExpressionVisitor whereVisitor = new WhereExpressionVisitor();
		if (sb.getWhere() != null) {
			sb.getWhere().accept(whereVisitor);
			whereRel = joinRel.select(joinRel.getDesc().nameToId(
					whereVisitor.getLeft()),
					whereVisitor.getOp(),
					whereVisitor.getRight());
		}

		// selecting 
		Relation selectRel = whereRel;
		List<SelectItem> selectList = sb.getSelectItems();

		ArrayList<Integer> projectFields = new ArrayList<Integer>();

		for (SelectItem item : selectList) {
			item.accept(colVisitor);
			
			String selectCol = colVisitor.isAggregate() ?  colVisitor.getColumn() : item.toString(); 

			if (selectCol.equals("*")){
				for (int i = 0; i < whereRel.getDesc().numFields(); i++) {
					projectFields.add(i);
				}
				break;
			} 
			int field = selectCol.equals("*") && colVisitor.isAggregate() ?
						0 : whereRel.getDesc().nameToId(selectCol);
			if (!projectFields.contains(field)) projectFields.add(field);
		}
		selectRel = whereRel.project(projectFields);

		// aggregate
		boolean groupFlag = sb.getGroupByColumnReferences() != null;
		
		Relation agg = colVisitor.isAggregate() ? 
				selectRel.aggregate(colVisitor.getOp(), groupFlag) : selectRel;

		return agg;
		
		
		
	}
}
