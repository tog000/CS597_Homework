package edu.boisestate.cs597;

import java.io.IOException;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

import org.apache.pig.data.DefaultTuple;
import org.apache.pig.data.Tuple;

/**
 * Unit test for simple App.
 */
public class AppTest 
    extends TestCase
{
    /**
     * Create the test case
     *
     * @param testName name of the test case
     */
    public AppTest( String testName )
    {
        super( testName );
    }

    /**
     * @return the suite of tests being tested
     */
    public static Test suite()
    {
        return new TestSuite( AppTest.class );
    }

    public void testApp() throws IOException
    {
    	
    	WeekdaysBetweenDates wbd = new WeekdaysBetweenDates();
		Tuple input = new DefaultTuple();
		
		input.append("09/30/13");
		input.append("12/27/13");
    	
		Integer output = wbd.exec(input);
		
        assertEquals(64,output.intValue());
        
    }
}
