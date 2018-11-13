package org.apache.ctakes.fhir.cr;

import ca.uhn.fhir.context.ConfigurationException;
import ca.uhn.fhir.context.FhirContext;
import ca.uhn.fhir.parser.DataFormatException;
import ca.uhn.fhir.parser.IParser;
import org.apache.ctakes.core.cr.AbstractFileTreeReader;
import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.core.util.RelationArgumentUtil;
import org.apache.ctakes.fhir.element.FhirElementParser;
import org.apache.ctakes.fhir.resource.*;
import org.apache.ctakes.typesystem.type.relation.BinaryTextRelation;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.log4j.Logger;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.hl7.fhir.dstu3.model.*;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.utilities.xhtml.XhtmlNode;

import java.io.*;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.ctakes.fhir.element.FhirElementFactory.CTAKES_FHIR_URL;
import static org.apache.ctakes.fhir.element.FhirElementFactory.RELATION_EXT_PREFIX;
import static org.apache.ctakes.fhir.resource.AnnotationCreator.ID_NAME_ANNOTATION;
import static org.apache.ctakes.fhir.resource.BaseTokenCreator.ID_NAME_BASE_TOKEN;
import static org.apache.ctakes.fhir.resource.IdentifiedAnnotationCreator.ID_NAME_IDENTIFIED_ANNOTATION;
import static org.apache.ctakes.fhir.resource.ParagraphCreator.ID_NAME_PARAGRAPH;
import static org.apache.ctakes.fhir.resource.SectionCreator.ID_NAME_SECTION;
import static org.apache.ctakes.fhir.resource.SentenceCreator.ID_NAME_SENTENCE;
import static org.hl7.fhir.dstu3.model.Bundle.BundleEntryComponent;


/**
 * Unfinished collection reader to create ctakes annotations from fhir files.
 *
 * @author SPF , chip-nlp
 * @version %I%
 * @since 1/22/2018
 */
@PipeBitInfo(
      name = "FhirJsonFileReader",
      description = "Reads fhir information from json.", role = PipeBitInfo.Role.READER
)
public class FhirJsonFileReader extends AbstractFileTreeReader {

   static private final Logger LOGGER = Logger.getLogger( "FhirJsonFileReader" );

   /**
    * {@inheritDoc}
    */
   @Override
   protected void readFile( final JCas jCas, final File file ) throws IOException {
      jCas.reset();

      final Bundle bundle = readBundle( file );

      final CompositionParser compositionParser = new CompositionParser();
      final SectionParser sectionParser = new SectionParser();
      final ParagraphParser paragraphParser = new ParagraphParser();
      final SentenceParser sentenceParser = new SentenceParser();
      final BaseTokenParser baseTokenParser = new BaseTokenParser();
      final AnnotationParser annotationParser = new AnnotationParser();
      final IdentifiedAnnotationParser iaParser = new IdentifiedAnnotationParser();
      // Build map of resources to annotations, sections, etc.
      final List<BundleEntryComponent> entries = bundle.getEntry();
      final Map<IBaseResource, Annotation> resourceAnnotations = new HashMap<>( entries.size() );
      for ( BundleEntryComponent entry : entries ) {
         final IBaseResource resource = entry.getResource();
         final Annotation annotation = parseResource( jCas, resource,
               compositionParser, sectionParser, paragraphParser, sentenceParser, baseTokenParser,
               annotationParser, iaParser );
         if ( annotation != null ) {
            resourceAnnotations.put( resource, annotation );
         }
      }

      // Go through the (Basic) entries in the map and build relations
      for ( Map.Entry<IBaseResource, Annotation> resourceAnnotation : resourceAnnotations.entrySet() ) {
         if ( !Basic.class.isInstance( resourceAnnotation.getKey() ) ) {
            continue;
         }
         final Basic basic = (Basic)resourceAnnotation.getKey();
         final List<Extension> extensions = basic.getExtension();
         for ( Extension extension : extensions ) {
            final String url = extension.getUrl();
            if ( url.startsWith( CTAKES_FHIR_URL + RELATION_EXT_PREFIX ) ) {
               final Type type = extension.getValue();
               if ( type instanceof Reference ) {
                  final IBaseResource resource = ((Reference)type).getResource();
                  final Annotation target = resourceAnnotations.get( resource );
                  if ( target != null ) {
                     createRelation( jCas, url, resourceAnnotation.getValue(), target );
                  }
               }
            }
         }
      }

      // TODO build Map<Integer,Collection<Annotation>> with coref chain index to annotations that belong from the Basic Extensions
   }


   static private void createRelation( final JCas jCas, final String url,
                                       final Annotation source, final Annotation target ) {
      if ( source instanceof IdentifiedAnnotation && target instanceof IdentifiedAnnotation ) {
         final String category = url.substring( (CTAKES_FHIR_URL + RELATION_EXT_PREFIX).length() );
         final BinaryTextRelation relation
               = RelationArgumentUtil.createRelation( jCas, (IdentifiedAnnotation)source, (IdentifiedAnnotation)target,
               category );
         relation.addToIndexes();
      }
   }


   // TODO
   static private void createCoreference( final JCas jCas, Collection<Annotation> marked ) {

   }


   static private Annotation parseResource( final JCas jCas,
                                            final IBaseResource resource,
                                            final CompositionParser compositionParser,
                                            final SectionParser sectionParser,
                                            final ParagraphParser paragraphParser,
                                            final SentenceParser sentenceParser,
                                            final BaseTokenParser baseTokenParser,
                                            final AnnotationParser annotationParser,
                                            final IdentifiedAnnotationParser iaParser ) {
      if ( resource instanceof Composition ) {
         final Narrative narrative = ((Composition)resource).getText();
         final XhtmlNode html = narrative.getDiv();
         final String docText = html.allText();
         jCas.setDocumentText( docText );
         return null;
      }
      Annotation annotation = null;
      if ( resource instanceof Basic ) {
         final Basic basic = (Basic)resource;
         final String idName = FhirElementParser.getIdName( basic.getId() );
         switch ( idName ) {
            case ID_NAME_SECTION:
               annotation = sectionParser.parseResource( jCas, basic );
               break;
            case ID_NAME_PARAGRAPH:
               annotation = paragraphParser.parseResource( jCas, basic );
               break;
            case ID_NAME_SENTENCE:
               annotation = sentenceParser.parseResource( jCas, basic );
               break;
            case ID_NAME_BASE_TOKEN:
               annotation = baseTokenParser.parseResource( jCas, basic );
               break;
            case ID_NAME_ANNOTATION:
               annotation = annotationParser.parseResource( jCas, basic );
               break;
            case ID_NAME_IDENTIFIED_ANNOTATION:
               annotation = iaParser.parseResource( jCas, basic );
               break;
         }
         if ( annotation != null ) {
            annotation.addToIndexes();
            return annotation;
         }
      }
      return null;
   }


   static private Bundle readBundle( final File file ) throws IOException {
      IBaseResource baseResource;
      final FhirContext fhirContext = FhirContext.forDstu3();
      final IParser jsonParser = fhirContext.newJsonParser();
      try ( Reader reader = new BufferedReader( new FileReader( file ) ) ) {
         baseResource = jsonParser.parseResource( reader );

      } catch ( IOException | ConfigurationException | DataFormatException multE ) {
         LOGGER.error( "Could not read fhir from " + file.getAbsolutePath(), multE );
         throw new IOException( multE );
      }
      if ( baseResource == null ) {
         throw new IOException( "Null Bundle for file " + file.getAbsolutePath() );
      }
      if ( !Bundle.class.isInstance( baseResource ) ) {
         throw new IOException( "Resource is not a Bundle for file " + file.getAbsolutePath() );
      }
      return (Bundle)baseResource;
   }


}
