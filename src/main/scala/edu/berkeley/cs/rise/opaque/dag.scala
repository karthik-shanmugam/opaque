
import scala.collection.mutable
import scala.util.Random

import org.apache.spark.NarrowDependency
import org.apache.spark.Dependency
import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

import DAGSchedulerSimulator._

// abstraction of a node in our DAG
class DAGNode(task_id: (Int, Int)) {
    val rddId = task_id._1
    val partitionId = task_id._2
    val token = Random.nextInt
    val children = new mutable.ListBuffer[DAGNode]()
    override def hashCode(): Int = 1000000 * rddId + partitionId
    override def toString: String = s"($rddId, $partitionId, $token)"
}

object DAGUtils {

    type SparkDAG = Seq[DAGNode]

    // takes an rdd and creates a dag of (rdd, partition) tuples of every step leading up to that rdd
    def rddToDAG(rdd: RDD[_]): Seq[DAGNode] = {

        // datastructure that holds every node in our dag
        val nodes = new mutable.HashMap[Int, mutable.HashMap[Int, DAGNode]]()

        // collection of nodes with no incoming edges
        val roots = new mutable.HashSet[DAGNode]()

        // abstraction for default creation of nodes
        def getNode(rddId: Int, partitionId: Int): DAGNode = {
            if (!nodes.contains(rddId)) {
                nodes += (rddId -> new mutable.HashMap[Int, DAGNode]())
            }
            if (!nodes(rddId).contains(partitionId)) {
                nodes(rddId) += (partitionId -> new DAGNode((rddId, partitionId)))
            }
            nodes(rddId)(partitionId)
        }

        // keeps track of RDD's we've seen so we can do a dfs
        val visited = new mutable.HashSet[RDD[_]]

        // performs a DFS over the RDD dependencies, and adds edges to our DAG
        // note that the DAG edges go in the opposite direction of dependencies
        def traverseRdd(rdd: RDD[_]): Unit = {
            visited += rdd
            val currentNodes = rdd.partitions.map { partition =>
                getNode(rdd.id, partition.index)
            }
            rdd.dependencies.map{ dependency =>
                val parentId = dependency.rdd.id
                dependency match {
                    // get every (parent partition, child partition) pair and add edges to our dag accordingly
                    case narrowDependency: NarrowDependency[_] => {
                        rdd.partitions.foreach { childPartition =>
                            narrowDependency.getParents(childPartition.index).foreach { parentPartitionIndex =>
                                getNode(parentId, parentPartitionIndex).children += getNode(rdd.id, childPartition.index)
                            }
                        }
                    }
                    // TODO(karthik-shanmugam): is this ok? should we only allow narrow dependencies?
                    // what if some of these n^2 input edges we add aren't real?
                    case broadDependency: Dependency[_] => {
                        broadDependency.rdd.partitions.foreach { partition =>
                            getNode(parentId, partition.index).children ++= currentNodes
                        }
                    }
                }
                if (!visited.contains(dependency.rdd)) {
                    traverseRdd(dependency.rdd)
                }
            }
            // if this rdd does not have a parent, then all its partitions are root nodes of our dag
            if (rdd.dependencies.isEmpty) {
                roots ++= currentNodes
            }
        }
        traverseRdd(rdd)
        return roots.toList
    }

    // takes an rdd and creates a dag of (rdd, partition) tuples of every step leading up to that rdd
    def rddToDAG2(rdd: RDD[_]): Seq[DAGNode] = {

        val finalStage = new DAGScheduler().createResultStage(rdd)

        // datastructure that holds every node in our dag
        val nodes = new mutable.HashMap[Int, mutable.HashMap[Int, DAGNode]]()

        // collection of nodes with no incoming edges
        val roots = new mutable.HashSet[DAGNode]()

        // abstraction for default creation of nodes
        def getNode(rddId: Int, partitionId: Int): DAGNode = {
            if (!nodes.contains(rddId)) {
                nodes += (rddId -> new mutable.HashMap[Int, DAGNode]())
            }
            if (!nodes(rddId).contains(partitionId)) {
                nodes(rddId) += (partitionId -> new DAGNode((rddId, partitionId)))
            }
            nodes(rddId)(partitionId)
        }

        // keeps track of RDD's we've seen so we can do a dfs
        val visited = new mutable.HashSet[RDD[_]]

        // performs a DFS over the RDD dependencies, and adds edges to our DAG
        // note that the DAG edges go in the opposite direction of dependencies
        def traverseStage(stage: Stage): Unit = {
            visited += rdd
            val currentNodes = stage.rdd.partitions.map { partition =>
                getNode(stage.rdd.id, partition.index)
            }
            stage.parents.map{ parentStage =>
                val parentId = parentStage.rdd.id
                parentStage match {
                    // // get every (parent partition, child partition) pair and add edges to our dag accordingly
                    // case narrowDependency: NarrowDependency[_] => {
                    //     rdd.partitions.foreach { childPartition =>
                    //         narrowDependency.getParents(childPartition.index).foreach { parentPartitionIndex =>
                    //             getNode(parentId, parentPartitionIndex).children += getNode(rdd.id, childPartition.index)
                    //         }
                    //     }
                    // }
                    // TODO(karthik-shanmugam): is this ok? should we only allow narrow dependencies?
                    // what if some of these n^2 input edges we add aren't real?
                    case shuffleMapStage: ShuffleMapStage => {
                        shuffleMapStage.rdd.partitions.foreach { partition =>
                            getNode(parentId, partition.index).children ++= currentNodes
                        }
                    }
                    case otherStage: Stage => {
                    }
                }
                if (!visited.contains(dependency.rdd)) {
                    traverseRdd(dependency.rdd)
                }
            }
            // if this rdd does not have a parent, then all its partitions are root nodes of our dag
            if (stage.parents.isEmpty) {
                roots ++= currentNodes
            }
        }
        traverseStage(finalStage)
        return roots.toList
    }

    def DAGtoString(dag: SparkDAG): String = {
        val res = new mutable.StringBuilder("")
        val traversed = new mutable.HashSet[DAGNode]()
        def traverseDAGNode(node: DAGNode, depth: Int): Unit = {
            res ++= "    " * depth + s"${node.toString}\n"
            if (!traversed.contains(node)) {
                traversed += node
                node.children.foreach { childNode => traverseDAGNode(childNode, depth+1) }
            } else {
                res ++= "    " * (depth+1) + "redundant path\n"
            }
        }
        dag.foreach{rootNode => traverseDAGNode(rootNode, 0)}
        return res.toString
    }

    def testCase(sc: SparkContext): (RDD[_], SparkDAG, String) = {
        val data = sc.parallelize(0 to 100, 2)
        val rdd = (
            data
            .map(i=>i+1)
            .map(i=>(i, i))
            .reduceByKey((a, b)=>a+b)
            .map{case (a, b) => (a, b+1)}
            .reduceByKey((a, b)=>a+b)
            )
        val dag = rddToDAG(rdd)
        val str = DAGtoString(dag)
        val dag2 = rddToDAG2(rdd)
        val str2 = DAGtoString(dag2)
        return (rdd, dag, str, dag2, str2)
    }
}