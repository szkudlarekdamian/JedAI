import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.apache.jena.base.Sys;
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

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

public class Main {

    private final static String AMAZON_PROFILE_FILEPATH = "data/amazonProfiles";
    private final static String GOOGLE_PROFILE_FILEPATH = "data/gpProfiles";
    private final static String GROUND_TRUTH_FILEPATH = "data/amazonGpIdDuplicates";

    private static boolean verbose = true;

    private static List<AbstractBlock> blockBuilding(AbstractBlockBuilding blockBuilding,
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

    private static List<AbstractBlock> blockProcessing(IBlockProcessing blockProcessing,
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

    private static SimilarityPairs entityMatching(AbstractEntityMatching entityMatching,
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

    private static double entityClustering(AbstractEntityClustering clusteringMethod,
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

        return clustersPerformance.getFMeasure();
    }

    public static double run(List<EntityProfile> profiles1,
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

        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(entityClusteringSimilarityThreshold);
        return entityClustering(clusteringMethod, similarityPairs, duplicatePropagation);
    }

    public static void main(String[] args) {
        EntitySerializationReader amazon = new EntitySerializationReader(AMAZON_PROFILE_FILEPATH);
        EntitySerializationReader google = new EntitySerializationReader(GOOGLE_PROFILE_FILEPATH);
        GtSerializationReader truth = new GtSerializationReader(GROUND_TRUTH_FILEPATH);

        List<EntityProfile> amazonProfiles = amazon.getEntityProfiles();
        List<EntityProfile> googleProfiles = google.getEntityProfiles();

        Set<IdDuplicates> duplicates = truth.getDuplicatePairs(amazonProfiles, googleProfiles);

        AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(duplicates);

        verbose = false;

//        double result = run(amazonProfiles, googleProfiles, duplicatePropagation,
//                2, 0.5,
//                RepresentationModel.TOKEN_UNIGRAMS_TF_IDF,
//                SimilarityMetric.COSINE_SIMILARITY,
//                0.1, 0.1);

        int[] windowSizes = {2,3,5,7,9};//,11,13,15,17,19};
        double[] thresholds = {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9};
        RepresentationModel[] representationModels = {RepresentationModel.TOKEN_UNIGRAMS_TF_IDF};//, RepresentationModel.TOKEN_UNIGRAMS};
        SimilarityMetric[] similarityMetrics = {SimilarityMetric.COSINE_SIMILARITY};//, SimilarityMetric.JACCARD_SIMILARITY};

        List<String[]> recordsList = new ArrayList<>();

        // grid search
        for (int windowSize: windowSizes) {
            for(double bfT: thresholds) {
                for(RepresentationModel rm: representationModels) {
                    for(SimilarityMetric sm: similarityMetrics) {
                        for(double emT: thresholds) {
                            for(double ecT: thresholds) {
                                Instant start = Instant.now();
                                double result = run(amazonProfiles, googleProfiles, duplicatePropagation,
                                        windowSize, bfT, rm, sm, emT, ecT);
                                Instant end = Instant.now();
                                long elapsed = Duration.between(start, end).toMillis();
                                String[] record = new String[]{
                                        String.valueOf(elapsed),
                                        String.valueOf(windowSize),
                                        String.valueOf(bfT),
                                        rm.name(),
                                        sm.name(),
                                        String.valueOf(emT),
                                        String.valueOf(ecT),
                                        String.valueOf(result)
                                };
                                recordsList.add(record);
                                System.out.println(Arrays.toString(record));
                            }
                        }
                    }
                }
            }
        }

        try {
            FileWriter out = new FileWriter(Instant.now().toString() + "_results.csv");
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
            printer.printRecords(recordsList);
        } catch (IOException e) {
            e.printStackTrace();
        }


//        List<AbstractBlock> blocks;
//
//        /*
//         * Extended Sorted Neighbourhood
//         * window size param (constructor) [2, ...]
//         */
//        System.out.println("F1: Extended Sorted Neighbourhood");
//        AbstractBlockBuilding blockBuilding = new ExtendedSortedNeighborhoodBlocking(2);
//        System.out.println(blockBuilding.getMethodParameters());
//
//        blocks = blockBuilding(blockBuilding, amazonProfiles, googleProfiles, duplicatePropagation);
//
//        /*
//         * Block Filtering
//         * r (threshold??) parameter (constructor) [0, 1]
//         */
//        System.out.println("\n\nF2: Block Filtering");
//        AbstractBlockProcessing blockFiltering = new BlockFiltering(0.5);
//        System.out.println(blockFiltering.getMethodParameters());
//
//        blocks = blockProcessing(blockFiltering, blocks, duplicatePropagation);
//
//        /*
//         * Comparison propagation
//         * no parameters taken, nothing to optimize here
//         */
//        System.out.println("\n\nF3: Comparison propagation");
//        AbstractComparisonCleaning comparisonPropagation = new ComparisonPropagation();
//
//        blocks = blockProcessing(comparisonPropagation, blocks, duplicatePropagation);
//
//        /*
//         * Group linkage
//         * Choose representation, similarity metric, threshold
//         */
//        System.out.println("\n\nF4: Group Linkage");
//        RepresentationModel representationModel = RepresentationModel.TOKEN_UNIGRAMS_TF_IDF;
//        SimilarityMetric similarityMetric = SimilarityMetric.getModelDefaultSimMetric(representationModel);
//        AbstractEntityMatching entityMatchingStrategy = new GroupLinkage(0.1, amazonProfiles, googleProfiles, representationModel, similarityMetric);
//
//        SimilarityPairs similarityPairs = entityMatching(entityMatchingStrategy, blocks);
//
//        /*
//         * Ricochet SR Clustering
//         */
//        System.out.println("\n\nF5: Ricochet SR Clustering");
//        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(0.1);
//        System.out.println(clusteringMethod.getMethodParameters());
//
//        double result = entityClustering(clusteringMethod, similarityPairs, duplicatePropagation);

//        System.out.println("\n\n");
//        System.out.println("-----------------");
//        System.out.println("Result [F1-score]: " + result);
//        System.out.println("-----------------");
    }
}
