package org.apache.ctakes.temporal.ae;

import org.apache.ctakes.core.pipeline.PipeBitInfo;
import org.apache.ctakes.core.resource.FileLocator;
import org.apache.ctakes.core.util.OntologyConceptUtil;
import org.apache.ctakes.core.util.Pair;
import org.apache.ctakes.typesystem.type.refsem.Date;
import org.apache.ctakes.typesystem.type.textsem.DateAnnotation;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.ctakes.typesystem.type.textsem.MedicationEventMention;
import org.apache.ctakes.typesystem.type.textsem.TimeMention;
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

import static java.util.Calendar.DAY_OF_MONTH;

/**
 * @author SPF , chip-nlp
 * @version %I%
 * @since 1/7/2019
 */
@PipeBitInfo(
      name = "SimpleMedDatesFinder",
      description = "Finds start and stop dates for events.",
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


   static private final Calendar NULL_CALENDAR = new Calendar.Builder().setDate( 1, 1, 1 ).build();
//   static private final Options PAST_OPTIONS = new Options( Pointer.PointerType.PAST );

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
      LOGGER.info( "Finding Simple Event Dates ..." );

      final Map<Annotation, Collection<IdentifiedAnnotation>> windowAnnotationMap = new HashMap<>();
      if ( _sectionList.isEmpty() ) {
         windowAnnotationMap.putAll( JCasUtil.indexCovered( jCas, _lookupClass, IdentifiedAnnotation.class ) );
      } else {
         if ( _lookupClass.equals( Segment.class ) ) {
            final Map<Segment, Collection<IdentifiedAnnotation>> sectionAnnotationMap
                  = JCasUtil.indexCovered( jCas, Segment.class, IdentifiedAnnotation.class );
            for ( Map.Entry<Segment, Collection<IdentifiedAnnotation>> sectionAnnotations : sectionAnnotationMap.entrySet() ) {
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
                  final Collection<IdentifiedAnnotation> annotations
                        = JCasUtil.selectCovered( jCas, IdentifiedAnnotation.class, section );
                  windowAnnotationMap.putAll( splitCovered( annotations, sectionWindows.getValue() ) );
               }
            }

         }
      }

      for ( Map.Entry<Annotation, Collection<IdentifiedAnnotation>> windowAnnotations : windowAnnotationMap.entrySet() ) {
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
                               final Collection<IdentifiedAnnotation> annotations ) {
      final int offset = window.getBegin();
      final String text = window.getCoveredText().toLowerCase();
      final List<Integer> startSpans = getTextIndices( text, "started" );
      final List<Integer> stopSpans = getTextIndices( text, "stopped" );
      if ( startSpans.isEmpty() && stopSpans.isEmpty() ) {
         return;
      }

      final List<Pair<Integer>> spans = new ArrayList<>();
      final Map<Pair<Integer>, MedicationEventMention> medMap = new HashMap<>();
      final Map<Pair<Integer>, Calendar> calendarMap = new HashMap<>();

      for ( IdentifiedAnnotation annotation : annotations ) {
         if ( annotation instanceof MedicationEventMention
              && (_cuiList.isEmpty()
                  || OntologyConceptUtil.getCuis( annotation ).stream().anyMatch( _cuiList::contains )) ) {
            final Pair<Integer> span = createTextSpan( annotation, offset );
            spans.add( span );
            medMap.put( span, (MedicationEventMention)annotation );
         } else if ( annotation instanceof TimeMention ) {
            final Calendar calendar = getCalendar( (TimeMention)annotation );
            if ( !NULL_CALENDAR.equals( calendar ) ) {
               final Pair<Integer> span = createTextSpan( annotation, offset );
               spans.add( span );
               calendarMap.put( span, calendar );
            }
         } else if ( annotation instanceof DateAnnotation ) {
            final Calendar calendar = getCalendar( (DateAnnotation)annotation );
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
                                     final Map<Pair<Integer>, MedicationEventMention> medMap,
                                     final Map<Pair<Integer>, Calendar> calendarMap,
                                     final List<Integer> startSpans,
                                     final List<Integer> stopSpans ) {
      int startIndex = startSpans.size() - 1;
      int stopIndex = stopSpans.size() - 1;

      Date startDate = null;
      Date stopDate = null;

      for ( int z = medDateSpans.size() - 1; z >= 0; z-- ) {
         final Pair<Integer> span = medDateSpans.get( z );
         if ( span.getValue1() > startIndex && span.getValue1() < startIndex + 5 ) {
            final Calendar start = calendarMap.get( span );
            if ( start != null ) {
               startDate = new Date( jCas );
               startDate.setDay( "" + start.get( DAY_OF_MONTH ) );
               startDate.setMonth( "" + (start.get( Calendar.MONTH ) + 1) );
               startDate.setYear( "" + start.get( Calendar.YEAR ) );
            }
         } else if ( span.getValue1() > stopIndex && span.getValue1() < stopIndex + 5 ) {
            final Calendar stop = calendarMap.get( span );
            if ( stop != null ) {
               stopDate = new Date( jCas );
               stopDate.setDay( "" + stop.get( DAY_OF_MONTH ) );
               stopDate.setMonth( "" + (stop.get( Calendar.MONTH ) + 1) );
               stopDate.setYear( "" + stop.get( Calendar.YEAR ) );
            }
         } else {
            final MedicationEventMention med = medMap.get( span );
            if ( med == null ) {
               // possibly some interrupting date?  Reset the dates.
               startDate = null;
               stopDate = null;
               continue;
            }
            if ( startDate != null ) {
               med.setStartDate( startDate );
            }
            if ( stopDate != null ) {
               med.setEndDate( stopDate );
            }
         }
      }
   }

   static private Map<Annotation, Collection<IdentifiedAnnotation>> splitCovered(
         final Collection<IdentifiedAnnotation> annotations,
         final Collection<Annotation> covering ) {
      final Map<Annotation, Collection<IdentifiedAnnotation>> covered = new HashMap<>();
      for ( IdentifiedAnnotation annotation : annotations ) {
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
      final List<Integer> indices = new ArrayList<>();
      int index = text.indexOf( searchText );
      while ( index >= 0 ) {
         indices.add( index );
         index = text.indexOf( searchText, index );
      }
      return indices;
   }

   static private Pair<Integer> createTextSpan( final Annotation annotation, final int offset ) {
      return new Pair<>( annotation.getBegin() - offset, annotation.getEnd() - offset );
   }


   static private Calendar getCalendar( final TimeMention timeMention ) {
      final org.apache.ctakes.typesystem.type.refsem.Date typeDate = timeMention.getDate();
      if ( typeDate != null ) {
         final int year = parseInt( typeDate.getYear() );
         final int month = parseInt( typeDate.getMonth() );
         final int day = parseInt( typeDate.getDay() );
         if ( year == Integer.MIN_VALUE || month == Integer.MIN_VALUE || day == Integer.MIN_VALUE ) {
            return NULL_CALENDAR;
         }
         LOGGER.debug( "TimeMention Date " + year + "" + month + "" + day );
         return new Calendar.Builder().setDate( year, month - 1, day ).build();
      }
      return getCalendar( timeMention.getCoveredText() );
   }

   static private Calendar getCalendar( final DateAnnotation dateAnnotation ) {
      return getCalendar( dateAnnotation.getCoveredText() );
   }

   static private Calendar getCalendar( final String text ) {
      if ( isLousyDateText( text ) ) {
         return NULL_CALENDAR;
      }
//      final Span span = Chronic.parse( text, PAST_OPTIONS );
//      if ( span == null ) {
//         return NULL_CALENDAR;
//      }
//      return span.getEndCalendar();

      return NULL_CALENDAR;
   }


   static private boolean isLousyDateText( final String text ) {
      if ( text.length() < 7 ) {
         return true;
      }
      for ( char c : text.toCharArray() ) {
         if ( Character.isDigit( c ) ) {
            return false;
         }
      }
      return true;
   }

   static private int parseInt( final String text ) {
      if ( text == null || text.isEmpty() ) {
         return Integer.MIN_VALUE;
      }
      for ( char c : text.toCharArray() ) {
         if ( !Character.isDigit( c ) ) {
            return Integer.MIN_VALUE;
         }
      }
      try {
         return Integer.parseInt( text );
      } catch ( NumberFormatException nfE ) {
         return Integer.MIN_VALUE;
      }
   }


   synchronized protected void loadSections() throws ResourceInitializationException {
      if ( _sectionListPath == null ) {
         return;
      }
      loadList( _sectionListPath, _sectionList );
   }


   synchronized protected void loadCuis() throws ResourceInitializationException {
      if ( _cuiListPath == null ) {
         return;
      }
      loadList( _cuiListPath, _cuiList );
   }


   synchronized protected void loadList( final String filePath, final Collection<String> list )
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
