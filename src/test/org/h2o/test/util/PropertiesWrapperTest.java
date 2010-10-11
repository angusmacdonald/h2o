package org.h2o.test.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;

import org.h2o.util.H2OPropertiesWrapper;
import org.junit.Test;


/**
 * Tests of the PropertiesWrapper class.
 * 
 * @author Angus
 */
public class PropertiesWrapperTest {
	
	/**
	 * Tests the behaviour of the wrapper when trying to get a property with a 'null' paramater.
	 */
	@Test
	public void getPropertyNullParameter(){
		H2OPropertiesWrapper wrapper = new H2OPropertiesWrapper("testFile");
		try {
			wrapper.createNewFile();
		} catch (IOException e) {
			fail("Should be able to create file here.");
		}
		
		assertEquals(null, wrapper.getProperty(null));
	}
}
