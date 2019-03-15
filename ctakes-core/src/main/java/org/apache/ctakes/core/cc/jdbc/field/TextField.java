package org.apache.ctakes.core.cc.jdbc.field;


import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class TextField extends AbstractJdbcField<String> {

   public TextField( final String name ) {
      super( name, NO_INDEX );
   }

   public void addToStatement( final CallableStatement statement, final String value ) throws SQLException {
      statement.setString( getFieldName(), value );
   }

}
