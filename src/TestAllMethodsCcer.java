/*
 * Copyright [2016-2020] [George Papadakis (gpapadis@yahoo.gr)]
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
 * https://github.com/scify/JedAIToolkit/blob/master/src/test/java/org/scify/jedai/entityclustering/TestAllMethodsCcer.java
 */

import java.io.File;
import java.io.FileNotFoundException;
import org.scify.jedai.blockbuilding.IBlockBuilding;
import org.scify.jedai.entityclustering.IEntityClustering;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.blockprocessing.IBlockProcessing;
import org.scify.jedai.datamodel.AbstractBlock;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datamodel.EquivalenceCluster;
import org.scify.jedai.datamodel.SimilarityPairs;
import org.scify.jedai.datareader.entityreader.IEntityReader;
import org.scify.jedai.datareader.entityreader.EntitySerializationReader;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.entitymatching.IEntityMatching;
import org.scify.jedai.utilities.BlocksPerformance;
import org.scify.jedai.utilities.ClustersPerformance;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.BlockBuildingMethod;
import org.scify.jedai.utilities.enumerations.EntityClusteringCcerMethod;
import org.scify.jedai.utilities.enumerations.EntityMatchingMethod;
import java.util.List;
import org.apache.log4j.BasicConfigurator;

/**
 *
 * @author G.A.P. II
 */
public class TestAllMethodsCcer {

    public static void main(String[] args) throws FileNotFoundException {
        BasicConfigurator.configure();

        String mainDirectory = "data" + File.separator;
        String[] entitiesFilePaths = { mainDirectory + "amazonProfiles", mainDirectory +  "gpProfiles" };
        String groundTruthFilePath = mainDirectory + "amazonGpIdDuplicates";

        IEntityReader eReader = new EntitySerializationReader(entitiesFilePaths[0]);
        List<EntityProfile> profilesD1 = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles D1\t:\t" + profilesD1.size());

        eReader = new EntitySerializationReader(entitiesFilePaths[1]);
        List<EntityProfile> profilesD2 = eReader.getEntityProfiles();
        System.out.println("Input Entity Profiles D2\t:\t" + profilesD2.size());

        IGroundTruthReader gtReader = new GtSerializationReader(groundTruthFilePath);
        final AbstractDuplicatePropagation duplicatePropagation = new BilateralDuplicatePropagation(gtReader.getDuplicatePairs(eReader.getEntityProfiles()));
        System.out.println("Existing Duplicates\t:\t" + duplicatePropagation.getDuplicates().size());
        BlockBuildingMethod blockingWorkflow = BlockBuildingMethod.STANDARD_BLOCKING;

        float time1 = System.currentTimeMillis();

        IBlockBuilding blockBuildingMethod = BlockBuildingMethod.getDefaultConfiguration(blockingWorkflow);
        List<AbstractBlock> blocks = blockBuildingMethod.getBlocks(profilesD1, profilesD2);
        System.out.println("Original blocks\t:\t" + blocks.size());

        StringBuilder blockingWorkflowConf = new StringBuilder();
        StringBuilder blockingWorkflowName = new StringBuilder();
        blockingWorkflowConf.append(blockBuildingMethod.getMethodConfiguration());
        blockingWorkflowName.append(blockBuildingMethod.getMethodName());

        IBlockProcessing blockCleaningMethod = BlockBuildingMethod.getDefaultBlockCleaning(blockingWorkflow);
        if (blockCleaningMethod != null) {
            blocks = blockCleaningMethod.refineBlocks(blocks);
            blockingWorkflowConf.append("\n").append(blockCleaningMethod.getMethodConfiguration());
            blockingWorkflowName.append("->").append(blockCleaningMethod.getMethodName());
        }

        IBlockProcessing comparisonCleaningMethod = BlockBuildingMethod.getDefaultComparisonCleaning(blockingWorkflow);
        if (comparisonCleaningMethod != null) {
            blocks = comparisonCleaningMethod.refineBlocks(blocks);
            blockingWorkflowConf.append("\n").append(comparisonCleaningMethod.getMethodConfiguration());
            blockingWorkflowName.append("->").append(comparisonCleaningMethod.getMethodName());
        }

        float time2 = System.currentTimeMillis();

        BlocksPerformance blp = new BlocksPerformance(blocks, duplicatePropagation);
//        blp.printFalseNegatives(profilesD1, profilesD2, "data" + File.separator + "falseNegatives.csv");
        blp.setStatistics();
        blp.printStatistics(time2 - time1, blockingWorkflowConf.toString(), blockingWorkflowName.toString());

        for (EntityMatchingMethod emMethod : EntityMatchingMethod.values()) {

            float time3 = System.currentTimeMillis();

            IEntityMatching em = EntityMatchingMethod.getDefaultConfiguration(profilesD1, profilesD2, emMethod);
            SimilarityPairs simPairs = em.executeComparisons(blocks);

            float time4 = System.currentTimeMillis();

            for (EntityClusteringCcerMethod ecMethod : EntityClusteringCcerMethod.values()) {

                //if (ecMethod.toString().contains("UNIQUE")) continue;
                //System.out.println("meth "+ecMethod.toString());

                float time5 = System.currentTimeMillis();

                IEntityClustering ec = EntityClusteringCcerMethod.getDefaultConfiguration(ecMethod);
                ec.setSimilarityThreshold(0.25f);
                EquivalenceCluster[] entityClusters = ec.getDuplicates(simPairs);

                float time6 = System.currentTimeMillis();

                StringBuilder matchingWorkflowConf = new StringBuilder();
                StringBuilder matchingWorkflowName = new StringBuilder();
                matchingWorkflowConf.append(em.getMethodConfiguration());
                matchingWorkflowName.append(em.getMethodName());
                matchingWorkflowConf.append("\n").append(ec.getMethodConfiguration());
                matchingWorkflowName.append("->").append(ec.getMethodName());


                ClustersPerformance clp = new ClustersPerformance(entityClusters, duplicatePropagation);
                clp.setStatistics();
                clp.printStatistics(time6 - time5 + time4 - time3, matchingWorkflowName.toString(), matchingWorkflowConf.toString());
                clp.printDetailedResults(profilesD1, profilesD2, "data" + File.separator + "test.csv");

                //System.exit(-1);
            }
        }
    }
}