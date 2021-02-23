# LegalGraph

Repository for assembling and exploring a graph representation of the data assembled by the [Development Data Lab](http://www.devdatalab.org/) in their "big data for justice" [article](https://devdatalab.medium.com/big-data-for-justice-f53e0e14c9c9), [repo](https://github.com/devdatalab/paper-justice) and [dataset](http://www.devdatalab.org/judicial-bias-data).

Given the size of the datasets, they are not included here, but are referenced fairly straightforwardly in the notebooks.

## Graph structure

### Nodes

* **Judges** (index on judge ID)
* **Cases** (index on case ID)
* **Acts** (index on act ID)

### Relationships

* *JUDGED*: (Judge) -> (Case)
* *USES_ACT*: (Case) -> Act

## Notebook

The notebook provides various routines to load the data into Neo4J. The notebook uses the Neo4J driver for doing this, rather than Cypher scripts (e.g., using Neo4J's CSV import). That allows for easier local replication by using Pandas to easily chunk and reproduce the code. It also partially mitigates underlying lock-in to Cypher/Neo4J by encapsulating the key functions.

## Next steps

1. PageRank (+ eyeballs on Bloom) show pretty quickly that the "Code of Criminal Procedure" is replicated several times. The full list is now included in the notebook, in the Acts section. These should clearly be merged. Instead of doing so as a heavy list throughout the original CSVs, leverage the graph and combine the nodes once all is loaded.

2. The next acts down in PageRank, so far, relate to bail and other procedural matters, or prohibition. There are some interesting quirks (e.g., bail is PageRank central, but has low "total count")
