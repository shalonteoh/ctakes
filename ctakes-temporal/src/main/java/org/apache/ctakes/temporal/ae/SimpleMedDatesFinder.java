package org.apache.ctakes.temporal.ae;

import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.core.resource.FileLocator;
import org.apache.ctakes.core.util.OntologyConceptUtil;
import org.apache.ctakes.core.util.Pair;
import org.apache.ctakes.temporal.utils.CalendarUtil;
import org.apache.ctakes.typesystem.type.refsem.Date;
import org.apache.ctakes.typesystem.type.syntax.Chunk;
import org.apache.ctakes.typesystem.type.textsem.*;
import org.apache.ctakes.typesystem.type.textspan.Segment;
import org.apache.log4j.Logger;
import org.apache.uima.UimaContext;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.analysis_engine.AnalysisEngineProcessException;
import org.apache.uima.fit.component.JCasAnnotator_ImplBase;
import org.apache.uima.fit.descriptor.ConfigurationParameter;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.regex.Pattern;

import static org.apache.ctakes.temporal.utils.CalendarUtil.NULL_CALENDAR;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 1/7/2019
 */
@PipeBitInfo(
      name = "SimpleMedDatesFinder",
      description = "Finds start and stop dates for medication events.",
      role = PipeBitInfo.Role.ANNOTATOR,
      dependencies = PipeBitInfo.TypeProduct.IDENTIFIED_ANNOTATION,
      products = PipeBitInfo.TypeProduct.TEMPORAL_RELATION
)
final public class SimpleMedDatesFinder extends JCasAnnotator_ImplBase {

   static private final Logger LOGGER = Logger.getLogger( "SimpleMedDatesFinder" );

   /**
    * specifies the type of window to use for lookup
    */
   static public final String PARAM_LOOKUP_WINDOW_ANNOTATION = "LookupWindow";
   static public final String PARAM_SECTION_LIST_PATH = "SectionList";
   static public final String PARAM_CUI_LIST_PATH = "CuiList";


   static private final String DEFAULT_LOOKUP_WINDOW = "org.apache.ctakes.typesystem.type.textspan.Paragraph";
   @ConfigurationParameter( name = PARAM_LOOKUP_WINDOW_ANNOTATION,
         description = "Type of Lookup window to use.  Default is Paragraph.",
         mandatory = false,
         defaultValue = DEFAULT_LOOKUP_WINDOW
   )
   private String _windowClassName;


   static public final String SECTION_LIST_DESC
         = "Path to a file containing a list of sections of interest.  If none is specified then all sections are viable.";
   @ConfigurationParameter(
         name = PARAM_SECTION_LIST_PATH,
         description = SECTION_LIST_DESC,
         mandatory = false
   )
   private String _sectionListPath;


   static public final String CUI_LIST_DESC
         = "path to a file containing a list of cuis of interest.  If none is specified then all cuis are viable.";
   @ConfigurationParameter(
         name = PARAM_CUI_LIST_PATH,
         description = CUI_LIST_DESC,
         mandatory = false
   )
   private String _cuiListPath;


   static private final Pattern PARTIAL_DATE = Pattern.compile( "[0-9]{0,2}/[0-9]{0,2}/[0-9]{4}" );

   private Class<? extends Annotation> _lookupClass;
   private final Collection<String> _sectionList = new ArrayList<>();
   private final Collection<String> _cuiList = new ArrayList<>();


   /**
    * {@inheritDoc}
    */
   @Override
   public void initialize( final UimaContext context ) throws ResourceInitializationException {
      // Always call the super first
      super.initialize( context );

      loadSections();
      loadCuis();

      try {
         final Class<?> windowClass = Class.forName( _windowClassName );
         if ( !Annotation.class.isAssignableFrom( windowClass ) ) {
            LOGGER.error( "Lookup Window Class " + _windowClassName + " not found" );
            throw new ResourceInitializationException( new ClassNotFoundException() );
         }
         _lookupClass = (Class<? extends Annotation>)windowClass;
      } catch ( ClassNotFoundException cnfE ) {
         LOGGER.error( "Lookup Window Class " + _windowClassName + " not found" );
         throw new ResourceInitializationException( cnfE );
      }
      LOGGER.info( "Using Simple Event Date lookup window type: " + _windowClassName );

   }

   /**
    * {@inheritDoc}
    */
   @Override
   public void process( final JCas jCas ) throws AnalysisEngineProcessException {
      LOGGER.info( "Finding Medication Dates ..." );

      final Map<Annotation, Collection<Annotation>> windowAnnotationMap = new HashMap<>();
      if ( _sectionList.isEmpty() ) {
         windowAnnotationMap.putAll( JCasUtil.indexCovered( jCas, _lookupClass, Annotation.class ) );
      } else {
         if ( _lookupClass.equals( Segment.class ) ) {
            final Map<Segment, Collection<Annotation>> sectionAnnotationMap
                  = JCasUtil.indexCovered( jCas, Segment.class, Annotation.class );
            for ( Map.Entry<Segment, Collection<Annotation>> sectionAnnotations : sectionAnnotationMap.entrySet() ) {
               final Segment section = sectionAnnotations.getKey();
               if ( _sectionList.contains( section.getPreferredText() )
                    || _sectionList.contains( section.getId() ) ) {
                  windowAnnotationMap.put( section, sectionAnnotations.getValue() );
               }
            }
         } else {
            final Map<Segment, Collection<Annotation>> sectionWindowMap
                  = JCasUtil.indexCovered( jCas, Segment.class, _lookupClass );
            for ( Map.Entry<Segment, Collection<Annotation>> sectionWindows : sectionWindowMap.entrySet() ) {
               final Segment section = sectionWindows.getKey();
               if ( _sectionList.contains( section.getPreferredText() )
                    || _sectionList.contains( section.getId() ) ) {
                  final Collection<Annotation> annotations
                        = JCasUtil.selectCovered( jCas, Annotation.class, section );
                  windowAnnotationMap.putAll( splitCovered( annotations, sectionWindows.getValue() ) );
               }
            }

         }
      }

      for ( Map.Entry<Annotation, Collection<Annotation>> windowAnnotations : windowAnnotationMap.entrySet() ) {
         processWindow( jCas, windowAnnotations.getKey(), windowAnnotations.getValue() );
      }
      LOGGER.info( "Finished." );
   }


   /**
    * For instance "Aspirin, 10mg.  Started 1/1/2000.  Stopped 12/31/2000."
    *
    * @param jCas        ye olde ...
    * @param window      contains relatable annotations.
    * @param annotations -
    */
   private void processWindow( final JCas jCas,
                               final Annotation window,
                               final Collection<Annotation> annotations ) {
      final int offset = window.getBegin();
      final String text = window.getCoveredText().toLowerCase();
      final List<Integer> startSpans = getTextIndices( text, "started" );
      final List<Integer> stopSpans = getTextIndices( text, "stopped" );
      if ( startSpans.isEmpty() && stopSpans.isEmpty() ) {
         return;
      }

      final List<Pair<Integer>> spans = new ArrayList<>();
      final Map<Pair<Integer>, EventMention> medMap = new HashMap<>();
      final Map<Pair<Integer>, Calendar> calendarMap = new HashMap<>();

      for ( Annotation annotation : annotations ) {
         if ( (annotation instanceof MedicationEventMention || annotation instanceof MedicationMention)
              && !spans.contains( createTextSpan( annotation, offset ) )
              && (_cuiList.isEmpty()
                  || OntologyConceptUtil.getCuis( (IdentifiedAnnotation)annotation )
                                        .stream()
                                        .anyMatch( _cuiList::contains )) ) {
            final Pair<Integer> span = createTextSpan( annotation, offset );
            spans.add( span );
            medMap.put( span, (EventMention)annotation );
         } else if ( annotation instanceof TimeMention ) {
            final Calendar calendar = CalendarUtil.getCalendar( (TimeMention)annotation );
            if ( !NULL_CALENDAR.equals( calendar ) ) {
               final Pair<Integer> span = createTextSpan( annotation, offset );
               spans.add( span );
               calendarMap.put( span, calendar );
            }
         } else if ( annotation instanceof DateAnnotation
                     && !spans.contains( createTextSpan( annotation, offset ) ) ) {
            final Calendar calendar = CalendarUtil.getCalendar( (DateAnnotation)annotation );
            if ( !NULL_CALENDAR.equals( calendar ) ) {
               final Pair<Integer> span = createTextSpan( annotation, offset );
               spans.add( span );
               calendarMap.put( span, calendar );
            }
         } else if ( annotation instanceof Chunk
                     && annotation.getCoveredText().length() >= 6
                     && annotation.getCoveredText().length() <= 10
                     && PARTIAL_DATE.matcher( annotation.getCoveredText() ).matches()
                     && !spans.contains( createTextSpan( annotation, offset ) ) ) {
            final Calendar calendar = CalendarUtil.getSlashCalendar( annotation.getCoveredText() );
            if ( !NULL_CALENDAR.equals( calendar ) ) {
               final Pair<Integer> span = createTextSpan( annotation, offset );
               spans.add( span );
               calendarMap.put( span, calendar );
            }
         }
      }
      processSpans( jCas, spans, medMap, calendarMap, startSpans, stopSpans );
   }


   static private void processSpans( final JCas jCas,
                                     final List<Pair<Integer>> medDateSpans,
                                     final Map<Pair<Integer>, EventMention> medMap,
                                     final Map<Pair<Integer>, Calendar> calendarMap,
                                     final List<Integer> startSpans,
                                     final List<Integer> stopSpans ) {
      int startIndex = startSpans.size() - 1;
      int stopIndex = stopSpans.size() - 1;

      Date startDate = null;
      Date stopDate = null;
      TimeMention startMention = null;
      TimeMention stopMention = null;

      for ( int z = medDateSpans.size() - 1; z >= 0; z-- ) {
         final Pair<Integer> span = medDateSpans.get( z );
         int startSpanBegin = startSpans.get( startIndex );
         int stopSpanBegin = stopSpans.get( stopIndex );
         if ( span.getValue1() > startSpanBegin && span.getValue1() < startSpanBegin + 15 ) {
            final Calendar start = calendarMap.get( span );
            if ( start != null ) {
               startDate = CalendarUtil.createTypeDate( jCas, start );
               startMention = new TimeMention( jCas, span.getValue1(), span.getValue2() );
               startMention.setDate( startDate );
               startIndex = Math.max( startIndex - 1, 0 );
            }
         } else if ( span.getValue1() > stopSpanBegin && span.getValue1() < stopSpanBegin + 15 ) {
            final Calendar stop = calendarMap.get( span );
            if ( stop != null ) {
               stopDate = CalendarUtil.createTypeDate( jCas, stop );
               stopMention = new TimeMention( jCas, span.getValue1(), span.getValue2() );
               stopMention.setDate( stopDate );
               stopIndex = Math.max( stopIndex - 1, 0 );
               startDate = null;
               startMention = null;
            }
         } else {
            final EventMention med = medMap.get( span );
            if ( med == null ) {
               // possibly some interrupting date?  Reset the dates.
               startDate = null;
               startMention = null;
               stopDate = null;
               stopMention = null;
               continue;
            }
            if ( med instanceof MedicationEventMention ) {
               if ( startDate != null ) {
                  ((MedicationEventMention)med).setStartDate( startDate );
               }
               if ( stopDate != null ) {
                  ((MedicationEventMention)med).setEndDate( stopDate );
               }
            } else if ( med instanceof MedicationMention ) {
               if ( startMention != null ) {
                  ((MedicationMention)med).setStartDate( startMention );
               }
               if ( stopMention != null ) {
                  ((MedicationMention)med).setEndDate( stopMention );
               }
            }
         }
      }
   }

   static private Map<Annotation, Collection<Annotation>> splitCovered(
         final Collection<Annotation> annotations,
         final Collection<Annotation> covering ) {
      final Map<Annotation, Collection<Annotation>> covered = new HashMap<>();
      for ( Annotation annotation : annotations ) {
         final int begin = annotation.getBegin();
         for ( Annotation cover : covering ) {
            if ( begin >= cover.getBegin() && begin < cover.getEnd() ) {
               covered.computeIfAbsent( cover, c -> new ArrayList<>() ).add( annotation );
            }
         }
      }
      return covered;
   }

   static private List<Integer> getTextIndices( final String windowText, final String searchText ) {
      final String text = windowText.toLowerCase();
      final int maxIndex = text.length() - 1;
      final List<Integer> indices = new ArrayList<>();
      int index = text.indexOf( searchText );
      while ( index >= 0 ) {
         indices.add( index );
         if ( index == maxIndex ) {
            break;
         }
         index = text.indexOf( searchText, index + 1 );
      }
      return indices;
   }

   static private Pair<Integer> createTextSpan( final Annotation annotation, final int offset ) {
      return new Pair<>( annotation.getBegin() - offset, annotation.getEnd() - offset );
   }




   synchronized private void loadSections() throws ResourceInitializationException {
      if ( _sectionListPath == null ) {
         return;
      }
      loadList( _sectionListPath, _sectionList );
   }


   synchronized private void loadCuis() throws ResourceInitializationException {
      if ( _cuiListPath == null ) {
         return;
      }
      loadList( _cuiListPath, _cuiList );
   }


   synchronized private void loadList( final String filePath, final Collection<String> list )
         throws ResourceInitializationException {
      if ( filePath == null ) {
         return;
      }
      LOGGER.info( "Parsing " + filePath );
      try ( BufferedReader reader = new BufferedReader( new InputStreamReader( FileLocator
            .getAsStream( filePath ) ) ) ) {
         String line = reader.readLine();
         while ( line != null ) {
            final String value = readBsvLine( line );
            if ( !value.isEmpty() ) {
               list.add( value );
            }
            line = reader.readLine();
         }
      } catch ( IOException ioE ) {
         throw new ResourceInitializationException( ioE );
      }
      LOGGER.info( "Finished Parsing" );
   }

   /**
    * @param line double-bar separated text
    */
   private String readBsvLine( final String line ) {
      if ( line.isEmpty() || line.startsWith( "#" ) || line.startsWith( "//" ) ) {
         // comment
         return "";
      }
      final String[] splits = line.split( "\\|" );
      if ( splits.length >= 1 ) {
         // We are only interested in the first entry
         return splits[ 0 ].trim();
      }
      return "";
   }


   static public AnalysisEngineDescription createEngineDescription( final String cuiListPath )
         throws ResourceInitializationException {
      return AnalysisEngineFactory.createEngineDescription( SimpleMedDatesFinder.class,
            PARAM_CUI_LIST_PATH, cuiListPath );
   }

}
