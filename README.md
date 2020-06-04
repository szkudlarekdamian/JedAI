## Pipeline
* F1-Extended Sorted Neighborhood
* F2-Block Filtering
* F3-Comparison Propagation
* F4-Group Linkage
* F5-Ricochet SR Clustering

## How to use JedAI with Python

You can combine JedAI with Python through PyJNIus (https://github.com/kivy/pyjnius).

Preparation Steps:
1. Install python3 and PyJNIus (https://github.com/kivy/pyjnius).
2. Install java 8 openjdk and openjfx for java 8 and configure it as the default java.
3. Create a directory or a jar file with jedai-core and its dependencies. One approach is to use the maven-assembly-plugin
(https://maven.apache.org/plugins/maven-assembly-plugin/usage.html), which will package everything to a single jar file:
jedai-core-3.0-jar-with-dependencies.jar

In the following code block a simple example is presented in python 3. The code reads the ACM.csv file found at (JedAIToolkit/data/cleanCleanErDatasets/DBLP-ACM) and prints the entities found:

~~~~
import jnius_config;
jnius_config.add_classpath('jedai-core-3.0-jar-with-dependencies.jar')

from jnius import autoclass

filePath = 'path_to/ACM.csv'
CsvReader = autoclass('org.scify.jedai.datareader.entityreader.EntityCSVReader')
List = autoclass('java.util.List')
EntityProfile = autoclass('org.scify.jedai.datamodel.EntityProfile')
Attribute = autoclass('org.scify.jedai.datamodel.Attribute')
csvReader = CsvReader(filePath)
csvReader.setAttributeNamesInFirstRow(True);
csvReader.setSeparator(",");
csvReader.setIdIndex(0);
profiles = csvReader.getEntityProfiles()
profilesIterator = profiles.iterator()
while profilesIterator.hasNext() :
    profile = profilesIterator.next()
    print("\n\n" + profile.getEntityUrl())
    attributesIterator = profile.getAttributes().iterator()
    while attributesIterator.hasNext() :
        print(attributesIterator.next().toString())