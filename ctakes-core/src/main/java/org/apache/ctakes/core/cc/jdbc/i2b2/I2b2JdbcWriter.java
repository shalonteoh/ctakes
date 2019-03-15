package org.apache.ctakes.core.cc.jdbc.i2b2;

import org.apache.ctakes.core.cc.jdbc.AbstractJCasJdbcWriter;
import org.apache.ctakes.core.cc.jdbc.db.JdbcDb;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.resource.ResourceInitializationException;

import java.sql.SQLException;


/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/12/2019
 */
public class I2b2JdbcWriter extends AbstractJCasJdbcWriter {


   static public final String PARAM_TABLE_NAME = "FactOutputTable";
   @ConfigurationParameter(
         name = PARAM_TABLE_NAME,
         description = "Name of the Observation_Fact table for writing output."
   )
   private String _tableName;


   final protected JdbcDb createJdbcDb( final String driver,
                                        final String url,
                                        final String user,
                                        final String pass,
                                        final String keepAlive ) throws ResourceInitializationException {
      final I2b2Db db = new I2b2Db( driver, url, user, pass, keepAlive );
      try {
         db.addObservationFact( _tableName );
      } catch ( SQLException sqlE ) {
         throw new ResourceInitializationException( sqlE );
      }
      return db;
   }

}
