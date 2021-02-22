# LegalGraph

Repository for assembling and exploring a graph representation of the data assembled by the [Development Data Lab](http://www.devdatalab.org/) in their "big data for justice" [article](https://devdatalab.medium.com/big-data-for-justice-f53e0e14c9c9), [repo](https://github.com/devdatalab/paper-justice) and [dataset](http://www.devdatalab.org/judicial-bias-data).

Given the size of the datasets, they are not included here, but are referenced fairly straightforwardly in the notebooks.

## Graph structure

### Nodes

* **Judges** 
* **Cases**
* **Acts**

### Relationships

* *JUDGED*: (Judge) -> (Case)
* *USES_ACT*: (Case) -> Act

## Notebook

The notebook provides various routines to load the data into Neo4J. The notebook uses the Neo4J driver for doing this, rather than Cypher scripts (e.g., using Neo4J's CSV import). That allows for easier local replication by using Pandas to easily chunk and reproduce the code. It also partially mitigates underlying lock-in to Cypher/Neo4J by encapsulating the key functions.

## Next steps

Utilize the Neo4J graph data science routines to start generating insights on centrality, diversity, and the like.
