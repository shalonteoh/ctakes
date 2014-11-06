/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.ctakes.temporal.ae;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.apache.ctakes.relationextractor.ae.RelationExtractorAnnotator;
import org.apache.ctakes.relationextractor.ae.features.PartOfSpeechFeaturesExtractor;
import org.apache.ctakes.relationextractor.ae.features.RelationFeaturesExtractor;
import org.apache.ctakes.relationextractor.ae.features.TokenFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.CheckSpecialWordRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.ConjunctionRelationFeaturesExtractor;
//import org.apache.ctakes.temporal.ae.feature.DependencyParseUtils;
import org.apache.ctakes.temporal.ae.feature.DependencyPathFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.CoordinateFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.DependingVerbsFeatureExtractor;
//import org.apache.ctakes.temporal.ae.feature.EventInBetweenPropertyExtractor;
//import org.apache.ctakes.temporal.ae.feature.EventOutsidePropertyExtractor;
import org.apache.ctakes.temporal.ae.feature.SpecialAnnotationRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.TemporalPETFlatExtractor;
import org.apache.ctakes.temporal.ae.feature.TokenPropertyFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.DeterminerRelationFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.EventArgumentPropertyExtractor;
import org.apache.ctakes.temporal.ae.feature.EventTimeRelationFeatureExtractor;
import org.apache.ctakes.temporal.ae.feature.EventPositionRelationFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.NumberOfEventsInTheSameSentenceExtractor;
import org.apache.ctakes.temporal.ae.feature.NearbyVerbTenseRelationExtractor;
import org.apache.ctakes.temporal.ae.feature.NumberOfEventTimeBetweenCandidatesExtractor;
import org.apache.ctakes.temporal.ae.feature.OverlappedHeadFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.SRLRelationFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.TimeXRelationFeaturesExtractor;
import org.apache.ctakes.temporal.ae.feature.SectionHeaderRelationExtractor;
//import org.apache.ctakes.temporal.ae.feature.TemporalAttributeFeatureExtractor;
import org.apache.ctakes.temporal.ae.feature.UmlsFeatureExtractor;
//import org.apache.ctakes.temporal.ae.feature.UnexpandedTokenFeaturesExtractor;
//import org.apache.ctakes.temporal.ae.feature.treekernel.TemporalPETExtractor;
import org.apache.ctakes.typesystem.type.relation.BinaryTextRelation;
import org.apache.ctakes.typesystem.type.relation.RelationArgument;
import org.apache.ctakes.typesystem.type.relation.TemporalTextRelation;
//import org.apache.ctakes.typesystem.type.syntax.ConllDependencyNode;
import org.apache.ctakes.typesystem.type.textsem.EventMention;
import org.apache.ctakes.typesystem.type.textsem.IdentifiedAnnotation;
import org.apache.ctakes.typesystem.type.textspan.Sentence;
import org.apache.uima.analysis_engine.AnalysisEngineDescription;
import org.apache.uima.jcas.JCas;
import org.apache.uima.jcas.tcas.Annotation;
import org.apache.uima.resource.ResourceInitializationException;
import org.cleartk.ml.CleartkAnnotator;
import org.cleartk.ml.DataWriter;
import org.cleartk.ml.jar.DefaultDataWriterFactory;
import org.cleartk.ml.jar.DirectoryDataWriterFactory;
import org.cleartk.ml.jar.GenericJarClassifierFactory;
import org.apache.uima.fit.factory.AnalysisEngineFactory;
import org.apache.uima.fit.util.JCasUtil;

import com.google.common.collect.Lists;

public class EventEventRelationAnnotator extends RelationExtractorAnnotator {

	public static AnalysisEngineDescription createDataWriterDescription(
			Class<? extends DataWriter<String>> dataWriterClass,
					File outputDirectory,
					double probabilityOfKeepingANegativeExample) throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(
				EventEventRelationAnnotator.class,
				CleartkAnnotator.PARAM_IS_TRAINING,
				true,
				DefaultDataWriterFactory.PARAM_DATA_WRITER_CLASS_NAME,
				dataWriterClass,
				DirectoryDataWriterFactory.PARAM_OUTPUT_DIRECTORY,
				outputDirectory,
				RelationExtractorAnnotator.PARAM_PROBABILITY_OF_KEEPING_A_NEGATIVE_EXAMPLE,
				// not sure why this has to be cast; something funny going on in uimaFIT maybe?
				(float) probabilityOfKeepingANegativeExample);
	}

	public static AnalysisEngineDescription createAnnotatorDescription(String modelPath)
			throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(
				EventEventRelationAnnotator.class,
				CleartkAnnotator.PARAM_IS_TRAINING,
				false,
				GenericJarClassifierFactory.PARAM_CLASSIFIER_JAR_PATH,
				modelPath);
	}

	/**
	 * @deprecated use String path instead of File.
	 * ClearTK will automatically Resolve the String to an InputStream.
	 * This will allow resources to be read within from a jar as well as File.  
	 */	  
	@SuppressWarnings("dep-ann")
	public static AnalysisEngineDescription createAnnotatorDescription(File modelDirectory)
			throws ResourceInitializationException {
		return AnalysisEngineFactory.createEngineDescription(
				EventEventRelationAnnotator.class,
				CleartkAnnotator.PARAM_IS_TRAINING,
				false,
				GenericJarClassifierFactory.PARAM_CLASSIFIER_JAR_PATH,
				new File(modelDirectory, "model.jar"));
	}

	@Override
	protected List<RelationFeaturesExtractor> getFeatureExtractors() {
		return Lists.newArrayList(
				new TokenFeaturesExtractor()
				//				new UnexpandedTokenFeaturesExtractor() //use unexpanded version for i2b2 data
				, new PartOfSpeechFeaturesExtractor()
				//	    		, new TemporalPETExtractor()
				, new EventArgumentPropertyExtractor()
				, new NumberOfEventTimeBetweenCandidatesExtractor()
				, new SectionHeaderRelationExtractor()
				, new NearbyVerbTenseRelationExtractor()
				, new CheckSpecialWordRelationExtractor()
				, new UmlsFeatureExtractor()
				, new DependencyPathFeaturesExtractor()
				, new CoordinateFeaturesExtractor()
				, new OverlappedHeadFeaturesExtractor()
				, new SRLRelationFeaturesExtractor()
				, new NumberOfEventsInTheSameSentenceExtractor()
				, new EventPositionRelationFeaturesExtractor() //not helpful
				, new TimeXRelationFeaturesExtractor() //not helpful
				, new ConjunctionRelationFeaturesExtractor()
				, new DeterminerRelationFeaturesExtractor()
				, new EventTimeRelationFeatureExtractor()
				, new TokenPropertyFeaturesExtractor()
				, new DependingVerbsFeatureExtractor()
				, new SpecialAnnotationRelationExtractor() //not helpful
				, new TemporalPETFlatExtractor()
				//				, new EventInBetweenPropertyExtractor()
				//				, new EventOutsidePropertyExtractor()
				);
	}

	@Override
	protected Class<? extends Annotation> getCoveringClass() {
		return Sentence.class;
	}

	@Override
	protected List<IdentifiedAnnotationPair> getCandidateRelationArgumentPairs(
			JCas jCas, Annotation sentence) {
		
		Map<EventMention, Collection<EventMention>> coveringMap =
				JCasUtil.indexCovering(jCas, EventMention.class, EventMention.class);
		
		List<IdentifiedAnnotationPair> pairs = Lists.newArrayList();
		List<EventMention> events = new ArrayList<>(JCasUtil.selectCovered(jCas, EventMention.class, sentence));
		//filter events:
		List<EventMention> realEvents = Lists.newArrayList();
		for( EventMention event : events){
			if(event.getClass().equals(EventMention.class)){
				realEvents.add(event);
			}
		}
		events = realEvents;

		int eventNum = events.size();

		for (int i = 0; i < eventNum-1; i++){
			for(int j = i+1; j < eventNum; j++){
				EventMention eventA = events.get(j);
				EventMention eventB = events.get(i);
				boolean eventAValid = false;
				boolean eventBValid = false;
				for (EventMention event : JCasUtil.selectCovered(jCas, EventMention.class, eventA)){
					if(!event.getClass().equals(EventMention.class)){
						eventAValid = true;
						break;
					}
				}
				for (EventMention event : JCasUtil.selectCovered(jCas, EventMention.class, eventB)){
					if(!event.getClass().equals(EventMention.class)){
						eventBValid = true;
						break;
					}
				}
				if(eventAValid && eventBValid){
					if(this.isTraining()){
						for (EventMention event1 : coveringMap.get(eventA)){
							for(EventMention event2 : coveringMap.get(eventB)){
								pairs.add(new IdentifiedAnnotationPair(event1, event2));
							}
						}
						for(EventMention event1 : JCasUtil.selectCovered(jCas, EventMention.class, eventA)){
							for(EventMention event2 : JCasUtil.selectCovered(jCas, EventMention.class, eventB)){
								pairs.add(new IdentifiedAnnotationPair(event1, event2));
							}
						}
					}
					pairs.add(new IdentifiedAnnotationPair(eventA, eventB));
				}
			}
		}


		//		if(eventNum >= 2){
		//			for ( int i = 0; i< eventNum -1 ; i ++){
		//				EventMention evI = events.get(i);
		//				for(int j = i+1; j< eventNum; j++){
		//					EventMention evJ = events.get(j);
		//					if(j-i==1 || j-i==eventNum-1){//if two events are consecutive, or major events
		//						pairs.add(new IdentifiedAnnotationPair(evJ, evI));
		//					}else if(ifDependent(jCas, evI, evJ)){//if the event pairs are dependent// eventNum < 7 && 
		//						pairs.add(new IdentifiedAnnotationPair(evJ, evI));
		//					}else{// if the 
		//						continue;
		//					}
		//				}
		//			}
		//		}

		return pairs;
	}

	@Override
	protected void createRelation(JCas jCas, IdentifiedAnnotation arg1,
			IdentifiedAnnotation arg2, String predictedCategory) {
		RelationArgument relArg1 = new RelationArgument(jCas);
		relArg1.setArgument(arg1);
		relArg1.setRole("Arg1");
		relArg1.addToIndexes();
		RelationArgument relArg2 = new RelationArgument(jCas);
		relArg2.setArgument(arg2);
		relArg2.setRole("Arg2");
		relArg2.addToIndexes();
		TemporalTextRelation relation = new TemporalTextRelation(jCas);
		relation.setArg1(relArg1);
		relation.setArg2(relArg2);
		relation.setCategory(predictedCategory);
		relation.addToIndexes();
	}

	@Override
	protected String getRelationCategory(
			Map<List<Annotation>, BinaryTextRelation> relationLookup,
			IdentifiedAnnotation arg1,
			IdentifiedAnnotation arg2) {
		BinaryTextRelation relation = relationLookup.get(Arrays.asList(arg1, arg2));
		String category = null;
		if (relation != null && relation instanceof TemporalTextRelation) {
			category = relation.getCategory();
		} else {
			relation = relationLookup.get(Arrays.asList(arg2, arg1));
			if (relation != null && relation instanceof TemporalTextRelation) {
				if(relation.getCategory().equals("OVERLAP")){
					category = relation.getCategory();
					//				}else if (relation.getCategory().equals("BEFORE")){
					//					category = "AFTER";
					//				}else if (relation.getCategory().equals("AFTER")){
					//					category = "BEFORE";
					//				}
				}else{
					category = relation.getCategory() + "-1";
				}
			}
		}
		if (category == null && coin.nextDouble() <= this.probabilityOfKeepingANegativeExample) {
			category = NO_RELATION_CATEGORY;
		}
		return category;
	}
}
