import org.scify.jedai.blockbuilding.AbstractBlockBuilding;
import org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking;
import org.scify.jedai.blockprocessing.AbstractBlockProcessing;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractComparisonCleaning;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.*;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.entityclustering.AbstractEntityClustering;
import org.scify.jedai.entityclustering.RicochetSRClustering;
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
import java.util.List;
import java.util.Set;

public class Main {

    private final static String AMAZON_PROFILE_FILEPATH = "data/amazonProfiles";
    private final static String GOOGLE_PROFILE_FILEPATH = "data/gpProfiles";
    private final static String GROUND_TRUTH_FILEPATH = "data/amazonGpIdDuplicates";

    private static List<AbstractBlock> blockBuilding(AbstractBlockBuilding blockBuilding,
                                                     List<EntityProfile> profiles1,
                                                     List<EntityProfile> profiles2,
                                                     AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        List<AbstractBlock> blocks = blockBuilding.getBlocks(profiles1, profiles2);
        Instant end = Instant.now();

        BlocksPerformance blockBuildingStats = new BlocksPerformance(blocks, duplicatePropagation);
        blockBuildingStats.setStatistics();
        blockBuildingStats.printStatistics(Duration.between(start,end).toNanos(),blockBuilding.getMethodConfiguration(), blockBuilding.getMethodName());
        return blocks;
    }

    private static List<AbstractBlock> blockProcessing(IBlockProcessing blockProcessing,
                                                       List<AbstractBlock> blocks,
                                                       AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        List<AbstractBlock> newBlocks = blockProcessing.refineBlocks(blocks);
        Instant end = Instant.now();

        BlocksPerformance blockProcessingStats = new BlocksPerformance(newBlocks, duplicatePropagation);
        blockProcessingStats.setStatistics();
        blockProcessingStats.printStatistics(Duration.between(start,end).toNanos(), blockProcessing.getMethodConfiguration(), blockProcessing.getMethodName());
        return newBlocks;
    }

    private static SimilarityPairs entityMatching(AbstractEntityMatching entityMatching,
                                                  List<AbstractBlock> blocks) {
        Instant start = Instant.now();
        SimilarityPairs similarityPairs = entityMatching.executeComparisons(blocks);
        Instant end = Instant.now();

        System.out.println(entityMatching.getMethodName());
        System.out.println(entityMatching.getMethodInfo());
        System.out.println(entityMatching.getMethodConfiguration());
        System.out.println(entityMatching.getMethodParameters());
        System.out.println("Elapsed time: " + Duration.between(start, end).toMillis());
        return similarityPairs;
    }

    private static double entityClustering(AbstractEntityClustering clusteringMethod,
                                           SimilarityPairs similarityPairs,
                                           AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), clusteringMethod.getMethodConfiguration(), clusteringMethod.getMethodName());
        return clustersPerformance.getFMeasure();
    }

    public static void main(String[] args) {
        EntitySerializationReader amazon = new EntitySerializationReader(AMAZON_PROFILE_FILEPATH);
        EntitySerializationReader google = new EntitySerializationReader(GOOGLE_PROFILE_FILEPATH);
        GtSerializationReader truth = new GtSerializationReader(GROUND_TRUTH_FILEPATH);

        List<EntityProfile> amazonProfiles = amazon.getEntityProfiles();
        List<EntityProfile> googleProfiles = google.getEntityProfiles();

        Set<IdDuplicates> duplicates = truth.getDuplicatePairs(amazonProfiles, googleProfiles);

        BilateralDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(duplicates);
        List<AbstractBlock> blocks;

        /*
         * Extended Sorted Neighbourhood
         * window size param (constructor) [2, ...]
         */
        System.out.println("F1: Extended Sorted Neighbourhood");
        AbstractBlockBuilding blockBuilding = new ExtendedSortedNeighborhoodBlocking(2);

        blocks = blockBuilding(blockBuilding, amazonProfiles, googleProfiles, duplicatePropagation);

        /*
         * Block Filtering
         * r (threshold??) parameter (constructor) [0, 1]
         */
        System.out.println("\n\nF2: Block Filtering");
        AbstractBlockProcessing blockFiltering = new BlockFiltering(0.8);

        blocks = blockProcessing(blockFiltering, blocks, duplicatePropagation);

        /*
         * Comparison propagation
         * no parameters taken, nothing to optimize here
         */
        System.out.println("\n\nF3: Comparison propagation");
        AbstractComparisonCleaning comparisonPropagation = new ComparisonPropagation();

        blocks = blockProcessing(comparisonPropagation, blocks, duplicatePropagation);

        /*
         * Group linkage
         * Choose representation, similarity metric, threshold
         */
        System.out.println("\n\nF4: Group Linkage");
        RepresentationModel representationModel = RepresentationModel.TOKEN_UNIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.getModelDefaultSimMetric(representationModel);
        AbstractEntityMatching entityMatchingStrategy = new GroupLinkage(0.1, amazonProfiles, googleProfiles, representationModel, similarityMetric);

        SimilarityPairs similarityPairs = entityMatching(entityMatchingStrategy, blocks);

        /*
         * Ricochet SR Clustering
         */
        System.out.println("\n\nF5: Ricochet SR Clustering");
        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(0.1);

        double result = entityClustering(clusteringMethod, similarityPairs, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("Result [F1-score]: " + result);
        System.out.println("-----------------");
    }
}
