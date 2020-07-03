import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.scify.jedai.blockbuilding.AbstractBlockBuilding;
import org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking;
import org.scify.jedai.blockprocessing.AbstractBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractComparisonCleaning;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.*;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.entityclustering.AbstractEntityClustering;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.entitymatching.AbstractEntityMatching;
import org.scify.jedai.entitymatching.GroupLinkage;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.Set;

public class Detailed {

    private static final Logger log = LogManager.getLogger("default");

    /**
     * run experiment with parameter values set to obtain best available result for this workflow
     * @param args
     */
    public static void main(String[] args) {
        BasicConfigurator.configure();

        EntitySerializationReader amazon = new EntitySerializationReader(Common.AMAZON_PROFILE_FILEPATH);
        EntitySerializationReader google = new EntitySerializationReader(Common.GOOGLE_PROFILE_FILEPATH);
        GtSerializationReader truth = new GtSerializationReader(Common.GROUND_TRUTH_FILEPATH);

        List<EntityProfile> amazonProfiles = amazon.getEntityProfiles();
        List<EntityProfile> googleProfiles = google.getEntityProfiles();

        Set<IdDuplicates> duplicates = truth.getDuplicatePairs(amazonProfiles, googleProfiles);

        final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(duplicates);

        /*
          parameters
         */
        int windowSize = 2;
        float filteringThreshold = 0.3f;
        float glThreshold = 0.05f;
        float umcThreshold = 0.01f;
        RepresentationModel representationModel = RepresentationModel.TOKEN_TRIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;

        /*
          workflow elements
         */
        AbstractBlockBuilding blockBuilding = new ExtendedSortedNeighborhoodBlocking(windowSize);
        AbstractBlockProcessing blockProcessing = new BlockFiltering(filteringThreshold);
        AbstractComparisonCleaning comparisonCleaning = new ComparisonPropagation();
        AbstractEntityMatching entityMatching = new GroupLinkage(glThreshold, amazonProfiles, googleProfiles, representationModel, similarityMetric);
        AbstractEntityClustering entityClustering = new UniqueMappingClustering(umcThreshold);

        /*
          process
         */
        Instant s0 = Instant.now();
        List<AbstractBlock> blocks0 = blockBuilding.getBlocks(amazonProfiles, googleProfiles);
        Instant s1 = Instant.now();
        List<AbstractBlock> blocks1 = blockProcessing.refineBlocks(blocks0);
        Instant s2 = Instant.now();
        List<AbstractBlock> blocks2 = comparisonCleaning.refineBlocks(blocks1);
        Instant s3 = Instant.now();
        SimilarityPairs similarityPairs = entityMatching.executeComparisons(blocks2);
        Instant s4 = Instant.now();
        EquivalenceCluster[] clusters = entityClustering.getDuplicates(similarityPairs);
        Instant s5 = Instant.now();

        /*
          performance
         */
        BlocksPerformance blocksPerformance0 = new BlocksPerformance(blocks0, duplicatePropagation);
        BlocksPerformance blocksPerformance1 = new BlocksPerformance(blocks1, duplicatePropagation);
        BlocksPerformance blocksPerformance2 = new BlocksPerformance(blocks2, duplicatePropagation);

        blocksPerformance0.setStatistics();
        blocksPerformance1.setStatistics();
        blocksPerformance2.setStatistics();

        blocksPerformance0.printStatistics(Duration.between(s0, s1).toMillis(), blockBuilding.getMethodConfiguration(), blockBuilding.getMethodName());
        blocksPerformance1.printStatistics(Duration.between(s1, s2).toMillis(), blockProcessing.getMethodConfiguration(), blockProcessing.getMethodName());
        blocksPerformance2.printStatistics(Duration.between(s2, s3).toMillis(), comparisonCleaning.getMethodConfiguration(), blockProcessing.getMethodName());

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(s3, s5).toMillis(),
                entityMatching.getMethodName() + " -> " + entityClustering.getMethodName(),
                "\n" + entityMatching.getMethodName() + ":\t" + entityMatching.getMethodConfiguration() + "\n" +
                        entityClustering.getMethodName() + ":\t" + entityClustering.getMethodConfiguration() );


        log.info("F1:");
        Optional<AbstractBlock> opt0 = blocks0.stream().filter(block -> block.getComparisons().size() > 5).findFirst();
        if(opt0.isPresent()) {
            log.info("Block:\t" + opt0.get().getBlockIndex());
            for(int i=0; i<5; i++) {
                log.info(opt0.get().getComparisons().get(i));
            }
        }

        log.info("F2:");
        Optional<AbstractBlock> opt1 = blocks1.stream().filter(block -> block.getComparisons().size() > 5).findFirst();
        if(opt1.isPresent()) {
            log.info("Block:\t" + opt1.get().getBlockIndex());
            for(int i=0; i<5; i++) {
                log.info(opt1.get().getComparisons().get(i));
            }
        }
        log.info("F3:");
        Optional<AbstractBlock> opt2 = blocks2.stream().filter(block -> block.getComparisons().size() > 5).findFirst();
        if(opt2.isPresent()) {
            log.info("Block:\t" + opt2.get().getBlockIndex());
            for(int i=0; i<5; i++) {
                log.info(opt2.get().getComparisons().get(i));
            }
        }
        log.info("F4:");
        PairIterator pairIterator = similarityPairs.getPairIterator();
        for(int i=0; i<5; i++) {
            if(pairIterator.hasNext()) {
                log.info(pairIterator.next());
            }
        }
        log.info("F5:");
        for(int i=0; i<5; i++) {
            log.info("Cluster:\t" + i);
            log.info("Ids1:\t" + Arrays.toString(clusters[i].getEntityIdsD1().toArray()));
            log.info("Ids2:\t" + Arrays.toString(clusters[i].getEntityIdsD2().toArray()));
        }



    }
}
