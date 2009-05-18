package org.h2.h2o;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

import org.h2.api.Trigger;

/**
 * 
 *
 * @author Angus Macdonald (angus@cs.st-andrews.ac.uk)
 */
public class SchemaUpdateTrigger implements Trigger {

	/* (non-Javadoc)
	 * @see org.h2.api.Trigger#fire(java.sql.Connection, java.lang.Object[], java.lang.Object[])
	 */
	@Override
	public void fire(Connection conn, Object[] oldRow, Object[] newRow)
			throws SQLException {
        
		System.out.println("Hey, it worked!");
//		BigDecimal diff = null;
//        if (newRow != null) {
//            diff = (BigDecimal) newRow[1];
//        }
//        if (oldRow != null) {
//            BigDecimal m = (BigDecimal) oldRow[1];
//            diff = diff == null ? m.negate() : diff.subtract(m);
//        }
//        PreparedStatement prep = conn.prepareStatement(
//                "UPDATE INVOICE_SUM SET AMOUNT=AMOUNT+?");
//        prep.setBigDecimal(1, diff);
//        prep.execute();

	}

	/* (non-Javadoc)
	 * @see org.h2.api.Trigger#init(java.sql.Connection, java.lang.String, java.lang.String, java.lang.String, boolean, int)
	 */
	@Override
	public void init(Connection conn, String schemaName, String triggerName,
			String tableName, boolean before, int type) throws SQLException {
		
	}

}
