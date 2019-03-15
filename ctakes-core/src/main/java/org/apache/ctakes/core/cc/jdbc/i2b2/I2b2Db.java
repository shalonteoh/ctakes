package org.apache.ctakes.core.cc.jdbc.i2b2;

import org.apache.ctakes.core.cc.jdbc.db.AbstractJdbcDb;
import org.apache.uima.resource.ResourceInitializationException;

import java.sql.SQLException;

import static org.apache.ctakes.core.cc.jdbc.i2b2.ObservationFactTable.CorpusSettings;


/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/15/2019
 */
public class I2b2Db extends AbstractJdbcDb {


   public I2b2Db( final String driver,
                  final String url,
                  final String user,
                  final String pass,
                  final String keepAlive ) throws ResourceInitializationException {
      super( driver, url, user, pass, keepAlive );
   }

   /**
    * @param tableName name of the output observation fact table.
    * @return observation fact table with only negation marked by a negative (-) sign before concept codes.
    * @throws SQLException -
    */
   protected ObservationFactTable addObservationFact( final String tableName ) throws SQLException {
      final CorpusSettings settings = new CorpusSettings( CorpusSettings.Marker.MARK_NEGATED );
      return addObservationFact( tableName, settings );
   }

   /**
    * @param tableName      name of the output observation fact table.
    * @param corpusSettings settings for marking negation, uncertainty and generics.
    * @return observation fact table.
    * @throws SQLException -
    */
   protected ObservationFactTable addObservationFact( final String tableName,
                                                      final CorpusSettings corpusSettings ) throws SQLException {
      final ObservationFactTable table = new ObservationFactTable( getConnection(), tableName, corpusSettings );
      addTable( table );
      return table;
   }

}
