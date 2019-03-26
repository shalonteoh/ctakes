package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class FloatField extends AbstractJdbcField<Float> {

   public FloatField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final Float value ) throws SQLException {
      statement.setFloat( getFieldName(), value );
   }

   public void insertInStatement( final CallableStatement statement, final Float value ) throws SQLException {
      statement.setFloat( getFieldIndex(), value );
   }

}
