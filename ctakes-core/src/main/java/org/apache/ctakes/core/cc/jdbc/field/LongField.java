package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class LongField extends AbstractJdbcField<Long> {

   public LongField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final Long value ) throws SQLException {
      statement.setLong( getFieldName(), value );
   }

   public void insertInStatement( final CallableStatement statement, final Long value ) throws SQLException {
      statement.setLong( getFieldIndex(), value );
   }

}
