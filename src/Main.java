import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.scify.jedai.blockbuilding.AbstractBlockBuilding;
import org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking;
import org.scify.jedai.blockbuilding.StandardBlocking;
import org.scify.jedai.blockprocessing.AbstractBlockProcessing;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering;
import org.scify.jedai.blockprocessing.blockcleaning.ComparisonsBasedBlockPurging;
import org.scify.jedai.blockprocessing.comparisoncleaning.AbstractComparisonCleaning;
import org.scify.jedai.blockprocessing.comparisoncleaning.CardinalityNodePruning;
import org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation;
import org.scify.jedai.datamodel.*;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.entityclustering.AbstractEntityClustering;
import org.scify.jedai.entityclustering.RicochetSRClustering;
import org.scify.jedai.entityclustering.UniqueMappingClustering;
import org.scify.jedai.entitymatching.AbstractEntityMatching;
import org.scify.jedai.entitymatching.GroupLinkage;
import org.scify.jedai.entitymatching.ProfileMatcher;
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

    private static class Tuple {
        public double fMeasure;
        public double precision;
        public double recall;

        Tuple(double fMeasure, double precision, double recall) {
            this.fMeasure = fMeasure;
            this.precision = precision;
            this.recall = recall;
        }
    }

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

    private static Tuple entityClustering(AbstractEntityClustering clusteringMethod,
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

        return new Tuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
    }

    public static Tuple run(List<EntityProfile> profiles1,
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

    public static void gridSearch(List<EntityProfile> amazonProfiles,
                                  List<EntityProfile> googleProfiles,
                                  AbstractDuplicatePropagation duplicatePropagation) {
        int[] windowSizes = {2,5,9};
        double[] thresholds = {0.1, 0.3, 0.5, 0.7, 0.9};
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
                                Tuple result = run(amazonProfiles, googleProfiles, duplicatePropagation,
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
                                        String.valueOf(result.precision),
                                        String.valueOf(result.recall),
                                        String.valueOf(result.fMeasure)
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
    }

    public static List<AbstractBlock> pipeBlockingInit(List<EntityProfile> profiles1,
                                                       List<EntityProfile> profiles2,
                                                       AbstractDuplicatePropagation duplicatePropagation) {
        AbstractBlockBuilding blockBuilding = new ExtendedSortedNeighborhoodBlocking(2);
        AbstractBlockProcessing blockFiltering = new BlockFiltering(0.3);
        AbstractComparisonCleaning comparisonPropagation = new ComparisonPropagation();

        Instant start = Instant.now();
        List<AbstractBlock> blocks;
        blocks = blockBuilding.getBlocks(profiles1, profiles2);
        blocks = blockFiltering.refineBlocks(blocks);
        blocks = comparisonPropagation.refineBlocks(blocks);
        Instant end = Instant.now();

        BlocksPerformance blockBuildingStats = new BlocksPerformance(blocks, duplicatePropagation);
        blockBuildingStats.setStatistics();
        blockBuildingStats.printStatistics(Duration.between(start,end).toMillis(),"TODO config", "Initial Blocking Pipeline");

        return blocks;
    }

    public static List<AbstractBlock> pipeBlockingGeorge(List<EntityProfile> profiles1,
                                                         List<EntityProfile> profiles2,
                                                         AbstractDuplicatePropagation duplicatePropagation) {
        IBlockProcessing blockCleaningMethod1 = new ComparisonsBasedBlockPurging(1.00f);
        IBlockProcessing blockCleaningMethod2 = new BlockFiltering();
        CardinalityNodePruning comparisonCleaningMethod = new CardinalityNodePruning();

        Instant start = Instant.now();
        List<AbstractBlock> blocks;
        blocks = blockBuilding(new StandardBlocking(), profiles1, profiles2, duplicatePropagation);
        blocks = blockCleaningMethod1.refineBlocks(blocks);
        blocks = blockCleaningMethod2.refineBlocks(blocks);
        blocks = blockProcessing(comparisonCleaningMethod, blocks, duplicatePropagation);
        Instant end = Instant.now();

        BlocksPerformance blockBuildingStats = new BlocksPerformance(blocks, duplicatePropagation);
        blockBuildingStats.setStatistics();
        blockBuildingStats.printStatistics(Duration.between(start,end).toMillis(),"TODO config", "Best result blocking pipeline");

        return blocks;
    }
    public static Tuple pipeEntityResolutionInit(List<AbstractBlock> blocks,
                                                 List<EntityProfile> profiles1,
                                                 List<EntityProfile> profiles2,
                                                 AbstractDuplicatePropagation duplicatePropagation) {
        RepresentationModel representationModel = RepresentationModel.TOKEN_BIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;
        AbstractEntityMatching entityMatchingStrategy = new GroupLinkage(0.1, profiles1, profiles2, representationModel, similarityMetric);
        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(0.01);

        Instant start = Instant.now();
        SimilarityPairs similarityPairs = entityMatching(entityMatchingStrategy, blocks);
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), "Initial entity resolution pipeline", "TODO config");

        return new Tuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
    }

    public static Tuple pipeEntityResolutionGeorge(List<AbstractBlock> blocks,
                                                 List<EntityProfile> profiles1,
                                                 List<EntityProfile> profiles2,
                                                 AbstractDuplicatePropagation duplicatePropagation) {
        RepresentationModel representationModel = RepresentationModel.TOKEN_BIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;
        AbstractEntityMatching entityMatchingStrategy = new ProfileMatcher(profiles1, profiles2, representationModel, similarityMetric);
        AbstractEntityClustering clusteringMethod = new UniqueMappingClustering(0.05);

        Instant start = Instant.now();
        SimilarityPairs similarityPairs = entityMatching(entityMatchingStrategy, blocks);
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), "Best entity resolution pipeline","TODO config");

        return new Tuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
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

        List<AbstractBlock> blocks;


        /*
         * Pipeline 1 - given (initial) pipeline
         */
        blocks = pipeBlockingInit(amazonProfiles, googleProfiles, duplicatePropagation);
        Tuple r1 = pipeEntityResolutionInit(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P1\tResult [F1-score]: " + r1.fMeasure);
        System.out.println("-----------------");


        /*
         * Pipeline 2 - best pipeline
         *  https://github.com/scify/JedAIToolkit/blob/master/src/test/java/org/scify/jedai/version3/BestConfigurationBlockingBasedWorkflowCcer.java
         */

        blocks = pipeBlockingGeorge(amazonProfiles, googleProfiles, duplicatePropagation);
        Tuple r2 = pipeEntityResolutionGeorge(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P2\tResult [F1-score]: " + r2.fMeasure);
        System.out.println("-----------------");

        /*
         * Pipeline 3
         * Crossing - Init Blocking, George Entity Resolution
         */
        blocks = pipeBlockingInit(amazonProfiles, googleProfiles, duplicatePropagation);
        Tuple r3 = pipeEntityResolutionGeorge(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P3\tResult [F1-score]: " + r3.fMeasure);
        System.out.println("-----------------");

        /*
         * Pipeline 4
         * Crossing - George Blocking, Init Entity Resolution
         */
        blocks = pipeBlockingGeorge(amazonProfiles, googleProfiles, duplicatePropagation);
        Tuple r4 = pipeEntityResolutionInit(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P4\tResult [F1-score]: " + r4.fMeasure);
        System.out.println("-----------------");

        /*
         * Pipeline 5 & 6 - Resolve what is faulty - entity matching or entity clustering
         */

        blocks = pipeBlockingInit(amazonProfiles, googleProfiles, duplicatePropagation);

        RepresentationModel representationModel = RepresentationModel.TOKEN_BIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;

        AbstractEntityMatching groupLinkage = new GroupLinkage(0.1, amazonProfiles, googleProfiles, representationModel, similarityMetric);
        AbstractEntityMatching profileMatcher = new ProfileMatcher(amazonProfiles, googleProfiles, representationModel, similarityMetric);

        AbstractEntityClustering ricochet = new RicochetSRClustering(0.05);
        AbstractEntityClustering uniqueMappingClustering = new UniqueMappingClustering(0.05);

        Instant start;
        Instant end;
        // p5
        start = Instant.now();
        SimilarityPairs sp5 = groupLinkage.executeComparisons(blocks);
        EquivalenceCluster[] clusters5 = uniqueMappingClustering.getDuplicates(sp5);
        end = Instant.now();
        ClustersPerformance clustersPerformance5 = new ClustersPerformance(clusters5, duplicatePropagation);
        clustersPerformance5.setStatistics();
        clustersPerformance5.printStatistics(Duration.between(start, end).toMillis(), "Group Linkage + Unique Mapping Clustering","TODO config");

        // p6
        start = Instant.now();
        SimilarityPairs sp6 = profileMatcher.executeComparisons(blocks);
        EquivalenceCluster[] clusters6 = ricochet.getDuplicates(sp6);
        end = Instant.now();
        ClustersPerformance clustersPerformance6 = new ClustersPerformance(clusters6, duplicatePropagation);
        clustersPerformance6.setStatistics();
        clustersPerformance6.printStatistics(Duration.between(start, end).toMillis(), "Profile Matcher + Ricochet","TODO config");
    }
}
