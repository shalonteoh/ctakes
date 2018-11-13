package org.apache.ctakes.fhir.cc;

import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.IParser;
import org.apache.ctakes.core.cc.AbstractJCasFileWriter;
import org.apache.ctakes.core.config.ConfigParameterConstants;
import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.fhir.resource.PractitionerCtakes;
import org.apache.log4j.Logger;
import org.apache.uima.analysis_engine.AnalysisEngine;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.ResourceInitializationException;
import org.hl7.fhir.dstu3.model.Bundle;

import java.io.*;


/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 11/9/2018
 */
@PipeBitInfo(
      name = "FHIR JSON File Writer (Dir Tree)",
      description = "Writes Json files with full representation of input text and all extracted information.",
      role = PipeBitInfo.Role.WRITER,
      dependencies = { PipeBitInfo.TypeProduct.DOCUMENT_ID },
      usables = { PipeBitInfo.TypeProduct.DOCUMENT_ID_PREFIX }
)
public class FhirJsonFileWriter extends AbstractJCasFileWriter {

   @ConfigurationParameter(
         name = "WriteNlpFhir",
         description = "Write all nlp information (paragraph, sentence, base annotations) to FHIR.",
         mandatory = false,
         defaultValue = "false"
   )
   private boolean _writeNlpFhir;

   static private final Logger LOGGER = Logger.getLogger( "FhirJsonFileWriter" );

   /**
    * {@inheritDoc}
    */
   @Override
   public void writeFile( final JCas jCas, final String outputDir,
                          final String documentId, final String fileName ) throws IOException {
      final Bundle bundle = FhirDocComposer.composeDocFhir( jCas, PractitionerCtakes.getInstance(), _writeNlpFhir );

      final FhirContext fhirContext = FhirContext.forDstu3();
      final IParser jsonParser = fhirContext.newJsonParser();
      jsonParser.setPrettyPrint( true );
      final String json = jsonParser.encodeResourceToString( bundle );

      final File file = new File( outputDir, fileName + ".json" );
      try ( Writer writer = new BufferedWriter( new FileWriter( file ) ) ) {
         writer.write( json );
      }
   }

   public static AnalysisEngine createEngine( final String outputDirectory ) throws ResourceInitializationException {
      return AnalysisEngineFactory
            .createEngine( FhirJsonFileWriter.class, ConfigParameterConstants.PARAM_OUTPUTDIR, outputDirectory );
   }

   public static AnalysisEngine createEngine( final String outputDirectory,
                                              final String subDirectory ) throws ResourceInitializationException {
      return AnalysisEngineFactory
            .createEngine( FhirJsonFileWriter.class, ConfigParameterConstants.PARAM_OUTPUTDIR, outputDirectory,
                  ConfigParameterConstants.PARAM_SUBDIR, subDirectory );
   }


}
