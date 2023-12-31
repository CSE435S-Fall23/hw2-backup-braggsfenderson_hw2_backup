package test;

import static org.junit.Assert.*;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;

import org.junit.Before;
import org.junit.Test;

import hw1.AggregateOperator;
import hw1.Catalog;
import hw1.Database;
import hw1.HeapFile;
import hw1.IntField;
import hw1.Relation;
import hw1.RelationalOperator;
import hw1.TupleDesc;

public class YourUnitTest {

	private HeapFile testhf;
	private TupleDesc testtd;
	private HeapFile ahf;
	private TupleDesc atd;
	private Catalog c;

	@Before
	public void setup() {
		
		try {
			Files.copy(new File("testfiles/test.dat.bak").toPath(), new File("testfiles/test.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
			Files.copy(new File("testfiles/A.dat.bak").toPath(), new File("testfiles/A.dat").toPath(), StandardCopyOption.REPLACE_EXISTING);
		} catch (IOException e) {
			System.out.println("unable to copy files");
			e.printStackTrace();
		}
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/test.txt");
		
		int tableId = c.getTableId("test");
		testtd = c.getTupleDesc(tableId);
		testhf = c.getDbFile(tableId);
		
		c = Database.getCatalog();
		c.loadSchema("testfiles/A.txt");
		
		tableId = c.getTableId("A");
		atd = c.getTupleDesc(tableId);
		ahf = c.getDbFile(tableId);
	}
	

	
//	@Test
//	public void testMax() {
//		Relation ar = new Relation(ahf.getAllTuples(), atd);
//		ArrayList<Integer> c = new ArrayList<Integer>();
//		c.add(1);
//		ar = ar.project(c);
//		ar = ar.aggregate(AggregateOperator.MAX, false);
//		
//		assertTrue(ar.getTuples().size() == 1);
//		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
//		assertTrue(agg.getValue() == 36);
//	}
//	
//	
//	@Test
//	public void testCount() {
//		Relation ar = new Relation(ahf.getAllTuples(), atd);
//		ArrayList<Integer> c = new ArrayList<Integer>();
//		c.add(1);
//		ar = ar.project(c);
//		ar = ar.aggregate(AggregateOperator.COUNT, false);
//		
//		assertTrue(ar.getTuples().size() == 1);
//		IntField agg = (IntField)(ar.getTuples().get(0).getField(0));
//		assertTrue(agg.getValue() == 1);
//	}
	
	


}
