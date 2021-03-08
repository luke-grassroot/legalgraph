# LegalGraph

Repository for assembling and exploring a graph representation of the data assembled by the [Development Data Lab](http://www.devdatalab.org/) in their "big data for justice" [article](https://devdatalab.medium.com/big-data-for-justice-f53e0e14c9c9), [repo](https://github.com/devdatalab/paper-justice) and [dataset](http://www.devdatalab.org/judicial-bias-data).

Given the size of the datasets, they are not included here, but are referenced fairly straightforwardly in the notebooks.

## Graph structure

### Nodes

* **Judges** (index on judge ID), current count ~100k
* **Cases** (index on case ID), current count ~14m (2018 loaded so far only)
* **Acts** (index on act ID), current count ~30k
* **States** (index on state ID), current count 32
* **Districts** (index on district ID), current count 632

### Relationships

* *JUDGED*: (Judge) -> (Case), current count ~3 million (**NB**: Appears may not have loaded all the judge-case relationships)
* *USES_ACT*: (Case) -> (Act), current count ~17 million
* *IN_STATE*: (Case) -> (State), current count ~14 million
* *CONTAINS*: (State) -> (District) current count ~600
* *IN_DISTRICT*: (District) -> (Case) current count ~~14 million

## Notebooks and source

The notebooks provides various routines to load the data into Neo4J. The notebook uses the Neo4J driver for doing this, rather than Cypher scripts (e.g., using Neo4J's CSV import). That allows for easier local replication by using Pandas to easily chunk and reproduce the code. It also partially mitigates underlying lock-in to Cypher/Neo4J by encapsulating the key functions. Basic structure is:

* src/lg_utils: Various methods for doing batched loads of very large CSVs as nodes and relationships
* src/IndiaLegal-LoadDataAndProps: notebook doing main lift of putting the CSVs into the graph (supercedes older IndiaLegal-LoadData)
* src/IndiaLegalData-CleanData: notebook that consolidates quite a few acts that are the same but have variant spellings (still quite a few left)
* src/IndiaLegal-DS: notebook that stores queries for running PageRank and Louvain community detection, and then exploring the results

## Next steps

* See how PageRank scores and Louvain communities change if restrict cases to female defendants
* Add in judge relationships on decisions, and again see how these change
* Understand what act L scores are telling us about relationship between acts and states
* Add sections to graph and rerun above, for final analyses
