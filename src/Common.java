import org.scify.jedai.blockbuilding.AbstractBlockBuilding;
import org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking;
import org.scify.jedai.blockprocessing.AbstractBlockProcessing;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractComparisonCleaning;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.entityclustering.AbstractEntityClustering;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.entitymatching.AbstractEntityMatching;
import org.scify.jedai.entitymatching.GroupLinkage;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

import java.time.Duration;
import java.time.Instant;
import java.util.List;

public class Common {

    public final static String AMAZON_PROFILE_FILEPATH = "data/amazonProfiles";
    public final static String GOOGLE_PROFILE_FILEPATH = "data/gpProfiles";
    public final static String GROUND_TRUTH_FILEPATH = "data/amazonGpIdDuplicates";
    public final static String DIRTY_PROFILE_FILEPATH = "data/amazonGpProfilesDirty";
    public final static String DIRTY_GROUND_TRUTH_FILEPATH = "data/amazonGpIdDuplicatesDirty";

    public static boolean verbose = true;

    public static List<AbstractBlock> blockBuilding(AbstractBlockBuilding blockBuilding,
                                                     List<EntityProfile> profiles1,
                                                     List<EntityProfile> profiles2,
                                                     AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        List<AbstractBlock> blocks = blockBuilding.getBlocks(profiles1, profiles2);
        Instant end = Instant.now();

        if (verbose) {
            BlocksPerformance blockBuildingStats = new BlocksPerformance(blocks, duplicatePropagation);
            blockBuildingStats.setStatistics();
            blockBuildingStats.printStatistics(Duration.between(start,end).toMillis(),blockBuilding.getMethodConfiguration(), blockBuilding.getMethodName());
        }

        return blocks;
    }

    public static List<AbstractBlock> blockProcessing(IBlockProcessing blockProcessing,
                                                       List<AbstractBlock> blocks,
                                                       AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        List<AbstractBlock> newBlocks = blockProcessing.refineBlocks(blocks);
        Instant end = Instant.now();

        if (verbose) {
            BlocksPerformance blockProcessingStats = new BlocksPerformance(newBlocks, duplicatePropagation);
            blockProcessingStats.setStatistics();
            blockProcessingStats.printStatistics(Duration.between(start,end).toMillis(), blockProcessing.getMethodConfiguration(), blockProcessing.getMethodName());
        }

        return newBlocks;
    }

    public static SimilarityPairs entityMatching(AbstractEntityMatching entityMatching,
                                                  List<AbstractBlock> blocks) {
        Instant start = Instant.now();
        SimilarityPairs similarityPairs = entityMatching.executeComparisons(blocks);
        Instant end = Instant.now();

        if (verbose) {
            System.out.println(entityMatching.getMethodName());
            System.out.println(entityMatching.getMethodInfo());
            System.out.println(entityMatching.getMethodConfiguration());
            System.out.println(entityMatching.getMethodParameters());
            System.out.println("Elapsed time: " + Duration.between(start, end).toMillis());
        }

        return similarityPairs;
    }

    public static ResultTuple entityClustering(AbstractEntityClustering clusteringMethod,
                                               SimilarityPairs similarityPairs,
                                               AbstractDuplicatePropagation duplicatePropagation) {
        Instant start = Instant.now();
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        if (verbose) {
            clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), clusteringMethod.getMethodConfiguration(), clusteringMethod.getMethodName());
        }

        return new ResultTuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
    }

    // grid search pipeline
    public static ResultTuple run(List<EntityProfile> profiles1,
                                  List<EntityProfile> profiles2,
                                  AbstractDuplicatePropagation duplicatePropagation,
                                  int windowSize,
                                  double blockFilterThreshold,
                                  RepresentationModel representationModel,
                                  SimilarityMetric similarityMetric,
                                  double entityMatchingSimilarityThreshold,
                                  double entityClusteringSimilarityThreshold) {

        List<AbstractBlock> blocks;

        AbstractBlockBuilding blockBuilding = new ExtendedSortedNeighborhoodBlocking(windowSize);
        blocks = blockBuilding(blockBuilding, profiles1, profiles2, duplicatePropagation);

        AbstractBlockProcessing blockFiltering = new BlockFiltering(blockFilterThreshold);
        blocks = blockProcessing(blockFiltering, blocks, duplicatePropagation);

        AbstractComparisonCleaning comparisonPropagation = new ComparisonPropagation();
        blocks = blockProcessing(comparisonPropagation, blocks, duplicatePropagation);

        AbstractEntityMatching entityMatchingStrategy = new GroupLinkage(entityMatchingSimilarityThreshold, profiles1, profiles2, representationModel, similarityMetric);
        SimilarityPairs similarityPairs = entityMatching(entityMatchingStrategy, blocks);

//        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(entityClusteringSimilarityThreshold);
        AbstractEntityClustering clusteringMethod = new UniqueMappingClustering(entityClusteringSimilarityThreshold);

        return entityClustering(clusteringMethod, similarityPairs, duplicatePropagation);
    }

    public static List<EntityProfile> getAmazonClean() {
        EntitySerializationReader amazon = new EntitySerializationReader(Common.AMAZON_PROFILE_FILEPATH);
        return amazon.getEntityProfiles();
    }

    public static List<EntityProfile> getGoogleClean() {
        EntitySerializationReader google = new EntitySerializationReader(Common.GOOGLE_PROFILE_FILEPATH);
        return google.getEntityProfiles();
    }

    public static List<EntityProfile> getAllDirty() {
        IEntityReader eReader = new EntitySerializationReader(Common.DIRTY_PROFILE_FILEPATH);
        return eReader.getEntityProfiles();
    }
}
