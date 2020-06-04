import jnius_config
jnius_config.add_classpath('jedai-core/jedai-core-3.0-jar-with-dependencies.jar')

from jnius import autoclass

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

def F1():
    return autoclass('org.scify.jedai.blockbuilding.ExtendedSortedNeighborhoodBlocking')

def F2():
    return autoclass('org.scify.jedai.blockprocessing.blockcleaning.BlockFiltering')

def F3():
    return autoclass('org.scify.jedai.blockprocessing.comparisoncleaning.ComparisonPropagation')

def F4():
    return autoclass('org.scify.jedai.entitymatching.GroupLinkage')

def F5():
    return autoclass('org.scify.jedai.entityclustering.RicochetSRClustering')


# CSV files
amazonProducts = loadFile('data/AmazonProducts.csv')
googleProducts = loadFile('data/GoogleProducts.csv')
groundTruth = autoclass('org.scify.jedai.datareader.groundtruthreader.GtCSVReader')('data/Amazon-Goodle-Products-PerfectMapping.csv')
groundTruth.setIgnoreFirstRow(True)
groundTruth.setSeparator(',')

# EntityProfile = autoclass('org.scify.jedai.datamodel.EntityProfile')
# Attribute = autoclass('org.scify.jedai.datamodel.Attribute')

# Get profiles of Amazon and Google
amazonProfiles = amazonProducts.getEntityProfiles()
googleProfiles = googleProducts.getEntityProfiles()

# Get duplicates
duplicates = groundTruth.getDuplicatePairs(amazonProfiles, googleProfiles)
# duplicatePropagation = autoclass('org.scify.jedai.utilities.datastructures.BilateralDuplicatePropagation')()

# Build F1
extendedSortedNeighborhoodBlocking = F1()()

for i in extendedSortedNeighborhoodBlocking.getBlocks(amazonProfiles,googleProfiles):
    print(i.toString())



