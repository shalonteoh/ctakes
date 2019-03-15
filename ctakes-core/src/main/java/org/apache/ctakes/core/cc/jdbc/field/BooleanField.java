package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class BooleanField extends AbstractJdbcField<Boolean> {

   public BooleanField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final Boolean value ) throws SQLException {
      statement.setBoolean( getFieldName(), value );
   }

}
