package org.apache.ctakes.core.cc.jdbc.field;


import org.apache.log4j.Logger;


/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
final public class FieldFactory {

   static private final Logger LOGGER = Logger.getLogger( "FieldFactory" );

   private FieldFactory() {
   }

   static public JdbcField createField( final String type, final String name ) {
      switch ( type.toLowerCase().trim() ) {
         case "string":
            return new TextField( name );
         case "text":
            return new TextField( name );
         case "int":
            return new IntField( name );
         case "integer":
            return new IntField( name );
         case "long":
            return new LongField( name );
         case "number":
            return new LongField( name );
         case "float":
            return new FloatField( name );
         case "double":
            return new DoubleField( name );
         case "decimal":
            return new DoubleField( name );
         case "timestamp":
            return new TimeField( name );
         case "time":
            return new TimeField( name );
         case "date":
            return new TimeField( name );
      }
      throw new IllegalArgumentException( "Unknown Field Type : " + type );
   }

}
