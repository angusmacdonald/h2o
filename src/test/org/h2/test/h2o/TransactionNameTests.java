package org.h2.test.h2o;

import static org.junit.Assert.assertNotNull;

import org.h2.h2o.util.TransactionNameGenerator;
import org.junit.Test;

public class TransactionNameTests {
	/**
	 * Test that transaction names are correctly generated even when the number of transactions
	 * exceeds the maximum allowed long value.
	 */
	@Test
	public void testGeneration(){
		long lastNumber = Long.MAX_VALUE - 1000;
		
		
		TransactionNameGenerator instance = new TransactionNameGenerator(null, lastNumber);
		
		
		
		for (long i = 0; i < 2000; i++){
			instance.generateName();
		}
	}
	
	/**
	 * Check that a null Database Instance parameter is handled without error.
	 */
	@Test
	public void nullCheck(){
		TransactionNameGenerator instance = new TransactionNameGenerator(null);
		
		assertNotNull(instance.generateName());
	}
	
	/**
	 * Check that a null string parameter is handled without error.
	 */
	@Test
	public void nullCheck2(){
		TransactionNameGenerator instance = new TransactionNameGenerator(null);
		
		assertNotNull(TransactionNameGenerator.generateName((String)null));
	}
}
