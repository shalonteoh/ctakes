package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Timestamp;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class TimeField extends AbstractJdbcField<Timestamp> {

   public TimeField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final Timestamp value ) throws SQLException {
      statement.setTimestamp( getFieldName(), value );
   }

   public void insertInStatement( final CallableStatement statement, final Timestamp value ) throws SQLException {
      statement.setTimestamp( getFieldIndex(), value );
   }

}
