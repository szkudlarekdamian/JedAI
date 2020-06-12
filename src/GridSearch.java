import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.scify.jedai.datamodel.EntityProfile;
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader;
import org.scify.jedai.datareader.groundtruthreader.IGroundTruthReader;
import org.scify.jedai.utilities.datastructures.AbstractDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation;
import org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation;
import org.scify.jedai.utilities.enumerations.RepresentationModel;
import org.scify.jedai.utilities.enumerations.SimilarityMetric;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class GridSearch {

    public static void gridSearch(List<EntityProfile> amazonProfiles,
                                  List<EntityProfile> googleProfiles,
                                  AbstractDuplicatePropagation duplicatePropagation) {
        int[] windowSizes = {2,3};
        double[] thresholds = {0.05, 0.1, 0.15, 0.2, 0.25, 0.3};
//        double[] thresholds = {0.3, 0.4, 0.5};
//        double[] thresholds = {0.1, 0.3, 0.5};
//        double[] entityThresholds = {0.01, 0.05, 0.1, 0.15};
        double[] entityThresholds = {0.01, 0.05, 0.1};
//        double[] entityThresholds = {0.1, 0.3, 0.5};
//        RepresentationModel[] representationModels = {RepresentationModel.TOKEN_BIGRAMS_TF_IDF};
//        RepresentationModel[] representationModels = {RepresentationModel.TOKEN_UNIGRAMS_TF_IDF};
        RepresentationModel[] representationModels = {RepresentationModel.TOKEN_TRIGRAMS_TF_IDF};
        SimilarityMetric[] similarityMetrics = {SimilarityMetric.COSINE_SIMILARITY};

        List<String[]> recordsList = new ArrayList<>();

        // grid search
        for (int windowSize: windowSizes) {
            for(double bfT: thresholds) {
                for(RepresentationModel rm: representationModels) {
                    for(SimilarityMetric sm: similarityMetrics) {
                        for(double emT: entityThresholds) {
                            for(double ecT: entityThresholds) {
                                Instant start = Instant.now();
                                Tuple result = Common.run(amazonProfiles, googleProfiles, duplicatePropagation,
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
            FileWriter out = new FileWriter("results/" + Instant.now().toString() + "_results.csv");
            CSVPrinter printer = new CSVPrinter(out, CSVFormat.DEFAULT);
            printer.printRecords(recordsList);
        } catch (IOException e) {
            e.printStackTrace();
            System.out.println("Failed to print record.");
        }
    }

    public static void main(String[] args) {
        //        BasicConfigurator.configure();

        // clean

        List<EntityProfile> amazonProfiles = Common.getAmazonClean();
        List<EntityProfile> googleProfiles = Common.getGoogleClean();

        GtSerializationReader cleanTruthReader = new GtSerializationReader(Common.GROUND_TRUTH_FILEPATH);
        final AbstractDuplicatePropagation duplicatePropagationClean = new BilateralDuplicatePropagation(cleanTruthReader.getDuplicatePairs(amazonProfiles, googleProfiles));

        // dirty

        List<EntityProfile> profiles = Common.getAllDirty();

        IGroundTruthReader dirtyTruthReader = new GtSerializationReader(Common.DIRTY_GROUND_TRUTH_FILEPATH);
        final AbstractDuplicatePropagation duplicatePropagationDirty = new UnilateralDuplicatePropagation(dirtyTruthReader.getDuplicatePairs(profiles));

        Common.verbose = false;

        // perform grid search
        // clean entity resolution
        gridSearch(amazonProfiles, googleProfiles, duplicatePropagationClean);
        // dirty entity resolution
//        gridSearch(profiles, null, duplicatePropagationDirty);

    }
}
