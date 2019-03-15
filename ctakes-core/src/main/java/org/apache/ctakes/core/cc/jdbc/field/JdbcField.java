package org.apache.ctakes.core.cc.jdbc.field;

import java.sql.CallableStatement;
import java.sql.SQLException;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
public interface JdbcField<T> {

   int NO_INDEX = -1;

   String getFieldName();

   default int getFieldIndex() {
      return NO_INDEX;
   }

   void addToStatement( final CallableStatement statement, final T value ) throws SQLException;

}
