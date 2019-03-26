package org.apache.ctakes.core.cc.jdbc.row;

import org.apache.ctakes.core.cc.jdbc.field.JdbcField;

import java.sql.*;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/14/2019
 */
public interface JdbcRow<C, P, D, E, T> {

   Collection<JdbcField<?>> getFields();

   default Collection<String> getFieldNames() {
      return getFields().stream()
                        .sorted( Comparator.comparingInt( JdbcField::getFieldIndex ) )
                        .map( JdbcField::getFieldName )
                        .collect( Collectors.toList() );
   }

   default void initializeFieldIndices( final Connection connection, final String tableName ) throws SQLException {
      final Collection<JdbcField<?>> fields = getFields();
      if ( fields == null ) {
         throw new SQLException( "No Fields defined for table " + tableName );
      }
      final Map<String, JdbcField<?>> fieldMap
            = fields.stream().collect( Collectors.toMap( JdbcField::getFieldName, Function.identity() ) );
      final Collection<String> assigned = new ArrayList<>( fieldMap.size() );
      final DatabaseMetaData metadata = connection.getMetaData();
      final ResultSet resultSet = metadata.getColumns( null, null, tableName, null );
      int index = 0;
      while ( resultSet.next() ) {
         index++;
         final String name = resultSet.getString( "COLUMN_NAME" );
         final JdbcField<?> field = fieldMap.get( name );
         if ( field != null ) {
            field.setFieldIndex( index );
            assigned.add( name );
         }
      }
      fieldMap.keySet().removeAll( assigned );
      if ( !fieldMap.isEmpty() ) {
         throw new SQLException( "No field indices for " + String.join( " , ", fieldMap.keySet() ) );
      }
   }

   default void initializeCorpus( final C corpusValue ) {
   }

   default void initializePatient( final P patientValue ) {
   }

   default void initializeDocument( final D documentValue ) {
   }

   default void initializeEntity( final E entityValue ) {
   }

   void addToStatement( final CallableStatement statement, final T value ) throws SQLException;

}
