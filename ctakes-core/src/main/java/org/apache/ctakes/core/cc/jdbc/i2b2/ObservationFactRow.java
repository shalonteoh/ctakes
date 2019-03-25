package org.apache.ctakes.core.cc.jdbc.i2b2;

import org.apache.ctakes.core.cc.jdbc.field.IntField;
import org.apache.ctakes.core.cc.jdbc.field.LongField;
import org.apache.ctakes.core.cc.jdbc.field.TextField;
import org.apache.ctakes.core.cc.jdbc.field.TimeField;
import org.apache.ctakes.core.cc.jdbc.row.JdbcRow;
import org.apache.ctakes.core.util.SourceMetadataUtil;
import org.apache.ctakes.typesystem.type.constants.CONST;
import org.apache.ctakes.typesystem.type.refsem.UmlsConcept;
import org.apache.ctakes.typesystem.type.structured.SourceData;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.log4j.Logger;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceProcessException;

import java.sql.CallableStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.Arrays;
import java.util.Collection;

import static org.apache.ctakes.core.cc.jdbc.i2b2.ObservationFactTable.CorpusSettings;

/**
 * For the format of the I2B2 Observation_Fact table,
 * see https://www.i2b2.org/software/projects/datarepo/CRC_Design_Doc_13.pdf
 *
 * @author SPF , chip-nlp
 * @version %I%
 * @since 3/14/2019
 */
final public class ObservationFactRow
      implements JdbcRow<CorpusSettings, JCas, JCas, IdentifiedAnnotation, UmlsConcept> {

   static private final Logger LOGGER = Logger.getLogger( "ObservationFactRow" );

   static private final String PATIENT_NUM = "patient_num";
   static private final String PROVIDER_ID = "provider_id";
   static private final String ENCOUNTER_NUM = "encounter_num";
   static private final String START_DATE = "start_date";
   static private final String INSTANCE_NUM = "instance_num";
   static private final String CONCEPT_CD = "concept_cd";
   static private final String MODIFIER_CD = "modifier_cd";
   static private final String VALTYPE_CD = "valtype_cd";
   static private final String TVAL_CHAR = "tval_char";
   static private final String OBSERVATION_BLOB = "observation_blob";

   static private final String DEFAULT_MODIFIER_CD = "@";
   static private final String DEFAULT_VALTYPE_CD = "T";

   static private final String NEGATED_MARK = "-";
   static private final String UNCERTAIN_MARK = "~";
   static private final String GENERIC_MARK = "*";
   static private final String NO_MARK = "";


   // Per Patient
   private final LongField _patientNum = new LongField( PATIENT_NUM );

   // Per Document
   private final TextField _providerId = new TextField( PROVIDER_ID );
   private final IntField _encounterNum = new IntField( ENCOUNTER_NUM );
   private final TimeField _startDate = new TimeField( START_DATE );

   // Per Row
   private final LongField _instanceNum = new LongField( INSTANCE_NUM );
   private final TextField _conceptCd = new TextField( CONCEPT_CD );
   private final TextField _modifierCd = new TextField( MODIFIER_CD );
   private final TextField _valtypeCd = new TextField( VALTYPE_CD );
   private final TextField _tvalChar = new TextField( TVAL_CHAR );
   private final TextField _observationBlob = new TextField( OBSERVATION_BLOB );

   // Per Patient
   private long _patient;

   // Per Document
   private String _provider;
   private int _encounter;
   private Timestamp _start;
   private long _instance;

   // Per Row
   private String _negatedMark;
   private String _uncertainMark;
   private String _genericMark;

   private String _conceptPrefix;
   private IdentifiedAnnotation _annotation;


   public Collection<String> getFieldNames() {
      return Arrays.asList( PATIENT_NUM,
            // Per Document
            PROVIDER_ID, ENCOUNTER_NUM, START_DATE,
            // Per Row
            INSTANCE_NUM, CONCEPT_CD, MODIFIER_CD, VALTYPE_CD, TVAL_CHAR, OBSERVATION_BLOB );
   }

   public void initializeCorpus( final CorpusSettings corpusValue ) {
      _negatedMark = corpusValue.markNegated() ? NEGATED_MARK : NO_MARK;
      _uncertainMark = corpusValue.markUncertain() ? UNCERTAIN_MARK : NO_MARK;
      _genericMark = corpusValue.markGeneric() ? GENERIC_MARK : NO_MARK;
   }

   /**
    * {@inheritDoc}
    * <p>
    * Sets the patient_num.
    */
   @Override
   public void initializePatient( final JCas patientValue ) {
      _patient = SourceMetadataUtil.getPatientNum( patientValue );
   }

   /**
    * {@inheritDoc}
    * <p>
    * Set the encounter_num, provider_id, start_date.
    */
   @Override
   public void initializeDocument( final JCas documentValue ) {
      _instance = 1;
      final SourceData sourceData = SourceMetadataUtil.getSourceData( documentValue );
      if ( sourceData == null ) {
         LOGGER.warn( "No document source data." );
         setEmptyDocInfo();
         return;
      }
      try {
         _encounter = SourceMetadataUtil.getEncounterNum( sourceData );
         _provider = SourceMetadataUtil.getProviderId( sourceData );
         _start = SourceMetadataUtil.getStartDate( sourceData );
      } catch ( ResourceProcessException rpE ) {
         LOGGER.warn( "Error setting document source data: " + rpE.getMessage() );
         setEmptyDocInfo();
      }
   }

   /**
    * {@inheritDoc}
    * <p>
    * Set the encounter_num, provider_id, start_date.
    */
   public void initializeEntity( final IdentifiedAnnotation entityValue ) {
      _annotation = entityValue;
      _conceptPrefix = ""
                       + (_annotation.getPolarity() == CONST.NE_POLARITY_NEGATION_PRESENT ? _negatedMark : NO_MARK)
                       + (_annotation.getUncertainty() == CONST.NE_UNCERTAINTY_PRESENT ? _uncertainMark : NO_MARK)
                       + (_annotation.getGeneric() ? _genericMark : NO_MARK);
   }

   /**
    * {@inheritDoc}
    * <p>
    * Set the concept_cd, modifier_cd, instance_num, valtype_cd, tval_char, observation_blob.
    */
   @Override
   public void addToStatement( final CallableStatement statement,
                               final UmlsConcept value ) throws SQLException {
      // Per Patient
      _patientNum.addToStatement( statement, _patient );

      // Per Document
      _providerId.addToStatement( statement, _provider );
      _encounterNum.addToStatement( statement, _encounter );
      _startDate.addToStatement( statement, _start );

      // Per Row
      _instanceNum.addToStatement( statement, _instance );
      _conceptCd.addToStatement( statement, getConceptCode( value ) );
      _modifierCd.addToStatement( statement, getModifierCd( value ) );
      _valtypeCd.addToStatement( statement, getValtypeCd( value ) );
      _tvalChar.addToStatement( statement, getTvalChar( value ) );
      _observationBlob.addToStatement( statement, getObservationBlob( value ) );
      _instance++;
   }

   /**
    * Set document information as empty.
    */
   private void setEmptyDocInfo() {
      _encounter = -1;
      _provider = "";
      _start = Timestamp.valueOf( LocalDateTime.now() );
   }

   private String getConceptCode( final UmlsConcept concept ) {
      return _conceptPrefix + concept.getCui();
   }

   private String getModifierCd( final UmlsConcept concept ) {
      return DEFAULT_MODIFIER_CD;
   }

   private String getValtypeCd( final UmlsConcept concept ) {
      return DEFAULT_VALTYPE_CD;
   }

   private String getTvalChar( final UmlsConcept concept ) {
      return concept.getPreferredText();
   }

   private String getObservationBlob( final UmlsConcept concept ) {
      return "";
   }


}
