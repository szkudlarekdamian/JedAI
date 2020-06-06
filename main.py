import jnius_config
jnius_config.add_classpath('jedai-core/jedai-core-3.0-jar-with-dependencies.jar')

from jnius import autoclass
from timeit import default_timer as timer
# Pipeline
    # F1-Extended Sorted Neighborhood
    # F2-Block Filtering
    # F3-Comparison Propagation
    # F4-Group Linkage
    # F5-Ricochet SR Clustering

# AmazonProducts attributes             GoogleProducts attributes           PerfectMapping attributes
    # "id",                                 "id",                               "idAmazon",
    # "title",                              "name",                             "idGoogleBase"
    # "description",                        "description",
    # "manufacturer",                       "manufacturer",
    # "price"                               "price"
    # 1362 rows                             3226 rows                           1300 rows


def loadFile(filepath):
    """
        Load CSV file (separator ','; attribute names in first row)
        Return CSV Entity
    """
    dataReader = autoclass('org.scify.jedai.datareader.entityreader.EntityCSVReader')
    csvReader = dataReader(filepath)
    csvReader.setAttributeNamesInFirstRow(True)
    csvReader.setSeparator(",")
    csvReader.setIdIndex(0)
    return csvReader

def printProfile(profiles):
    """
        Print attributes of each record of a profile
    """
    profilesIterator = profiles.iterator()
    while profilesIterator.hasNext() :
        profile = profilesIterator.next()
        print("\n\n" + profile.getEntityUrl())
        attributesIterator = profile.getAttributes().iterator()
        while attributesIterator.hasNext() :
            print(attributesIterator.next().toString())

def F1(amazonProfiles, googleProfiles, duplicatePropagation):
    extendedSortedNeighborhoodBlocking = autoclass('org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking')()
    # Timing block building
    start = timer()
    blocks = extendedSortedNeighborhoodBlocking.getBlocks(amazonProfiles,googleProfiles)
    end = timer()
    # Print stats
    stats = autoclass('org.scify.jedai.utilities.BlocksPerformance')(blocks, duplicatePropagation)
    stats.setStatistics()
    stats.printStatistics(end-start, extendedSortedNeighborhoodBlocking.getMethodConfiguration(), extendedSortedNeighborhoodBlocking.getMethodName())
    return blocks

def F2(blocks):
    blockFiltering = autoclass('org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering')()
    # Timing block cleaning
    start = timer()
    filtered = blockFiltering.refineBlocks(blocks)
    end = timer()
    print("Overhead time\t:\t", end-start)
    return filtered

def F3(filteredBlocks):
    comparisonPropagation = autoclass('org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation')()
    # Timing comparison cleaning
    start = timer()
    compared = comparisonPropagation.refineBlocks(filteredBlocks)
    end = timer()
    print("Overhead time\t:\t", end-start)
    return compared

def F4(comparedBlocks):
    model = autoclass('org.scify.jedai.utilities.enumerations.RepresentationModel').TOKEN_UNIGRAMS_TF_IDF
    simMetric = autoclass('org.scify.jedai.utilities.enumerations.SimilarityMetric').getModelDefaultSimMetric(model)
    groupLinkage = autoclass('org.scify.jedai.entitymatching.GroupLinkage')(0.1, amazonProfiles, googleProfiles, model, simMetric)
    # groupLinkage = autoclass('org.scify.jedai.entitymatching.GroupLinkage')(amazonProfiles, googleProfiles)

    # Timing group linkage
    start = timer()
    similarityPairs = groupLinkage.executeComparisons(comparedBlocks)
    end = timer()
    print("Overhead time\t:\t", end-start)
    return similarityPairs

def F5(similarityPairs):
    ricochet = autoclass('org.scify.jedai.entityclustering.RicochetSRClustering')()
    # Timing entity clustering
    start = timer()
    clusters = ricochet.getDuplicates(similarityPairs)
    end = timer()
    # Print stats
    stats = autoclass('org.scify.jedai.utilities.ClustersPerformance')(clusters, duplicatePropagation)
    stats.setStatistics()
    stats.printStatistics(end-start, ricochet.getMethodConfiguration(), ricochet.getMethodName())
    return clusters

# CSV files
amazonProducts = loadFile('data/AmazonProducts.csv')
googleProducts = loadFile('data/GoogleProducts.csv')
groundTruth = autoclass('org.scify.jedai.datareader.groundtruthreader.GtCSVReader')('data/Amazon-Goodle-Products-PerfectMapping.csv')
groundTruth.setIgnoreFirstRow(True)
groundTruth.setSeparator(',')

# Get profiles of Amazon and Google
amazonProfiles = amazonProducts.getEntityProfiles()
googleProfiles = googleProducts.getEntityProfiles()

# Get duplicates
duplicates = groundTruth.getDuplicatePairs(amazonProfiles, googleProfiles)
duplicatePropagation = autoclass('org.scify.jedai.utilities.datastructures.UnilateralDuplicatePropagation')(groundTruth.getDuplicatePairs(None))

# Build F1
print("\n------------------------------------------------------------------------")
print("F1: Block Building...")
blocks = F1(amazonProfiles,googleProfiles,duplicatePropagation)

# Build F2
print("\n------------------------------------------------------------------------")
print("F2: Block Cleaning...")
filteredBlocks = F2(blocks)

# Build F3
print("\n------------------------------------------------------------------------")
print("F3: Comparison Cleaning...")
comparedBlocks = F3(filteredBlocks)

# Build F4
print("\n------------------------------------------------------------------------")
print("F4: Entity Grouping...")
similarityPairs = F4(comparedBlocks)

# Build F5
print("\n------------------------------------------------------------------------")
print("F5: Entity Clustering...")
clusters = F5(similarityPairs)

# autoclass('org.scify.jedai.utilities.PrintToFile').toCSV(amazonProfiles,googleProfiles,clusters, 'data/matches.csv')
# for sim in similarityPairs.getSimilarities():
#     print(sim)
# Similarity between records
# for i in range(0,similarityPairs.getNoOfComparisons()):
#     print(similarityPairs.getEntityIds1()[i], "\t\t", similarityPairs.getEntityIds2()[i], "\t\t", similarityPairs.getSimilarities()[i])