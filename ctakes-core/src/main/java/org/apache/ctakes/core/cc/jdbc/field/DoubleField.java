package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class DoubleField extends AbstractJdbcField<Double> {

   public DoubleField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final Double value ) throws SQLException {
      statement.setDouble( getFieldName(), value );
   }

   public void insertInStatement( final CallableStatement statement, final Double value ) throws SQLException {
      statement.setDouble( getFieldIndex(), value );
   }

}
