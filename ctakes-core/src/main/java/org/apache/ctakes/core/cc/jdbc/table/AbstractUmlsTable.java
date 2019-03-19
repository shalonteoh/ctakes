package org.apache.ctakes.core.cc.jdbc.table;


import org.apache.ctakes.core.cc.jdbc.row.JdbcRow;
import org.apache.ctakes.core.util.OntologyConceptUtil;
import org.apache.ctakes.typesystem.type.refsem.UmlsConcept;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.log4j.Logger;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;

/**
 * Table that has one row per UmlsConcept in the JCas.
 *
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/14/2019
 */
abstract public class AbstractUmlsTable<C> extends AbstractJdbcTable<JCas> {

   private JdbcRow<C, JCas, JCas, IdentifiedAnnotation, UmlsConcept> _jdbcRow;

   public AbstractUmlsTable( final Connection connection, final String tableName ) throws SQLException {
      super( connection, tableName );
   }

   /**
    * @param jCas ye olde ...
    * @return some class that can be used to initialize the table.
    */
   abstract protected C getCorpusInitializer( final JCas jCas );

   /**
    * @return some row that uses JCas, Identified annotation, and a UmlsConcept per row.
    */
   abstract protected JdbcRow<C, JCas, JCas, IdentifiedAnnotation, UmlsConcept> createJdbcRow();

   /**
    * {@inheritDoc}
    */
   @Override
   final public Class<JCas> getDataType() {
      return JCas.class;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   final public JdbcRow<C, JCas, JCas, IdentifiedAnnotation, UmlsConcept> getJdbcRow() {
      if ( _jdbcRow == null ) {
         _jdbcRow = createJdbcRow();
      }
      return _jdbcRow;
   }

   /**
    * {@inheritDoc}
    */
   @Override
   final public void writeValue( final JCas value ) throws SQLException {
      final CallableStatement statement = getCallableStatement();
      final C corpusInitializer = getCorpusInitializer( value );
      final JdbcRow<C, JCas, JCas, IdentifiedAnnotation, UmlsConcept> row = getJdbcRow();

      row.initializeCorpus( corpusInitializer );
      row.initializePatient( value );
      row.initializeDocument( value );
      boolean batchWritten = false;
      final Collection<IdentifiedAnnotation> annotations = JCasUtil.select( value, IdentifiedAnnotation.class );
      Logger.getLogger( "AbstractUmlsTable" ).info( annotations.size() + " annotations to be written" );
      for ( IdentifiedAnnotation annotation : annotations ) {
         row.initializeEntity( annotation );
         final Collection<UmlsConcept> umlsConcepts = OntologyConceptUtil.getUmlsConcepts( annotation );
         Logger.getLogger( "AbstractUmlsTable" )
               .info( "   " + annotation.getCoveredText() + " " + umlsConcepts.size() + " concepts" );
         for ( UmlsConcept concept : umlsConcepts ) {
            row.addToStatement( statement, concept );
            batchWritten = incrementBatchIndex();
         }
      }
      if ( !batchWritten ) {
         // The current batch has not been written to db.  Do so now.
         final int[] updateToLog = getCallableStatement().executeBatch();
         final Collection<String> u2l = new ArrayList<>();
         Arrays.stream( updateToLog ).forEach( i -> u2l.add( i + "" ) );
         Logger.getLogger( "AbstractUmlsTable" ).info( String.join( ",", u2l ) );
      }
   }


}
