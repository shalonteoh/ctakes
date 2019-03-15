package org.apache.ctakes.core.cc.jdbc.table;


import org.apache.ctakes.core.cc.jdbc.row.JdbcRow;

import java.sql.CallableStatement;
import java.sql.SQLDataException;
import java.sql.SQLException;
import java.util.Collection;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
public interface JdbcTable<T> {

   String getTableName();

   Class<T> getDataType();

   JdbcRow<?, ?, ?, ?, ?> getJdbcRow();

   CallableStatement getCallableStatement();

   default Collection<String> getFieldNames() {
      return getJdbcRow().getFieldNames();
   }

   void writeValue( final T value ) throws SQLException;

   /**
    * @return -
    * @throws SQLDataException -
    */
   default String createRowInsertSql() throws SQLDataException {
      if ( getFieldNames().isEmpty() ) {
         throw new SQLDataException( "Must set at least one Field to create an sql insert Statement" );
      }
      final StringBuilder statement = new StringBuilder( "insert into" );
      final StringBuilder queries = new StringBuilder();
      statement.append( " " ).append( getTableName() );
      statement.append( " (" );
      for ( String fieldName : getFieldNames() ) {
         statement.append( fieldName ).append( "," );
         queries.append( "?," );
      }
      // remove the last comma
      statement.setLength( statement.length() - 1 );
      queries.setLength( queries.length() - 1 );
      statement.append( ") values (" ).append( queries ).append( ")" );
      return statement.toString();
   }

   default void close() throws SQLException {
      getCallableStatement().close();
   }

}
