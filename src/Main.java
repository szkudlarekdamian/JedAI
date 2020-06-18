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
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
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
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Set;

public class Main {

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
        blocks = Common.blockBuilding(new StandardBlocking(), profiles1, profiles2, duplicatePropagation);
        blocks = blockCleaningMethod1.refineBlocks(blocks);
        blocks = blockCleaningMethod2.refineBlocks(blocks);
        blocks = Common.blockProcessing(comparisonCleaningMethod, blocks, duplicatePropagation);
        Instant end = Instant.now();

        BlocksPerformance blockBuildingStats = new BlocksPerformance(blocks, duplicatePropagation);
        blockBuildingStats.setStatistics();
        blockBuildingStats.printStatistics(Duration.between(start,end).toMillis(),"TODO config", "Best result blocking pipeline");

        return blocks;
    }
    public static ResultTuple pipeEntityResolutionInit(List<AbstractBlock> blocks,
                                                       List<EntityProfile> profiles1,
                                                       List<EntityProfile> profiles2,
                                                       AbstractDuplicatePropagation duplicatePropagation) {
        RepresentationModel representationModel = RepresentationModel.TOKEN_BIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;
        AbstractEntityMatching entityMatchingStrategy = new GroupLinkage(0.1, profiles1, profiles2, representationModel, similarityMetric);
        AbstractEntityClustering clusteringMethod = new RicochetSRClustering(0.01);

        Instant start = Instant.now();
        SimilarityPairs similarityPairs = Common.entityMatching(entityMatchingStrategy, blocks);
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), "Initial entity resolution pipeline", "TODO config");

        return new ResultTuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
    }

    public static ResultTuple pipeEntityResolutionGeorge(List<AbstractBlock> blocks,
                                                         List<EntityProfile> profiles1,
                                                         List<EntityProfile> profiles2,
                                                         AbstractDuplicatePropagation duplicatePropagation) {
        RepresentationModel representationModel = RepresentationModel.TOKEN_BIGRAMS_TF_IDF;
        SimilarityMetric similarityMetric = SimilarityMetric.COSINE_SIMILARITY;
        AbstractEntityMatching entityMatchingStrategy = new ProfileMatcher(profiles1, profiles2, representationModel, similarityMetric);
        AbstractEntityClustering clusteringMethod = new UniqueMappingClustering(0.05);

        Instant start = Instant.now();
        SimilarityPairs similarityPairs = Common.entityMatching(entityMatchingStrategy, blocks);
        EquivalenceCluster[] clusters = clusteringMethod.getDuplicates(similarityPairs);
        Instant end = Instant.now();

        ClustersPerformance clustersPerformance = new ClustersPerformance(clusters, duplicatePropagation);
        clustersPerformance.setStatistics();
        clustersPerformance.printStatistics(Duration.between(start, end).toMillis(), "Best entity resolution pipeline","TODO config");

        return new ResultTuple(clustersPerformance.getFMeasure(),
                clustersPerformance.getPrecision(),
                clustersPerformance.getRecall());
    }

    public static void main(String[] args) {
//        BasicConfigurator.configure();

        EntitySerializationReader amazon = new EntitySerializationReader(Common.AMAZON_PROFILE_FILEPATH);
        EntitySerializationReader google = new EntitySerializationReader(Common.GOOGLE_PROFILE_FILEPATH);
        GtSerializationReader truth = new GtSerializationReader(Common.GROUND_TRUTH_FILEPATH);

        List<EntityProfile> amazonProfiles = amazon.getEntityProfiles();
        List<EntityProfile> googleProfiles = google.getEntityProfiles();

        Set<IdDuplicates> duplicates = truth.getDuplicatePairs(amazonProfiles, googleProfiles);

        final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(duplicates);

        // dirty
        IEntityReader eReader = new EntitySerializationReader(Common.DIRTY_PROFILE_FILEPATH);
        List<EntityProfile> profiles = eReader.getEntityProfiles();

        IGroundTruthReader gtReader = new GtSerializationReader(Common.DIRTY_GROUND_TRUTH_FILEPATH);
        final AbstractDuplicatePropagation duplicatePropagationDirty = new UnilateralDuplicatePropagation(gtReader.getDuplicatePairs(profiles));

        Common.verbose = false;

        List<AbstractBlock> blocks;


        /*
         * Pipeline 1 - given (initial) pipeline
         */
        blocks = pipeBlockingInit(amazonProfiles, googleProfiles, duplicatePropagation);
        ResultTuple r1 = pipeEntityResolutionInit(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P1\tResult [F1-score]: " + r1.fMeasure);
        System.out.println("-----------------");


        /*
         * Pipeline 2 - best pipeline
         *  https://github.com/scify/JedAIToolkit/blob/master/src/test/java/org/scify/jedai/version3/BestConfigurationBlockingBasedWorkflowCcer.java
         */

        blocks = pipeBlockingGeorge(amazonProfiles, googleProfiles, duplicatePropagation);
        ResultTuple r2 = pipeEntityResolutionGeorge(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P2\tResult [F1-score]: " + r2.fMeasure);
        System.out.println("-----------------");

        /*
         * Pipeline 3
         * Crossing - Init Blocking, George Entity Resolution
         */
        blocks = pipeBlockingInit(amazonProfiles, googleProfiles, duplicatePropagation);
        ResultTuple r3 = pipeEntityResolutionGeorge(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

        System.out.println("\n\n");
        System.out.println("-----------------");
        System.out.println("P3\tResult [F1-score]: " + r3.fMeasure);
        System.out.println("-----------------");

        /*
         * Pipeline 4
         * Crossing - George Blocking, Init Entity Resolution
         */
        blocks = pipeBlockingGeorge(amazonProfiles, googleProfiles, duplicatePropagation);
        ResultTuple r4 = pipeEntityResolutionInit(blocks, amazonProfiles, googleProfiles, duplicatePropagation);

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
