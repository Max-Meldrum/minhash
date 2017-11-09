// Max Meldrum

import scala.io.Source


object MinhashLSH extends App {
  type Document = Set[String]
  type Documents = Set[Document]
  type HashedShingles = Set[Int]

  val shingleSize = 3
  val minHashFunctions = 100
  val threshold = 0.8

  val files = Seq("data/data.txt", "data/data1.txt")
  val documents: Seq[Document] = files.map(file => getShinglesFromFile(file, shingleSize))
  val hashedDocuments: Seq[(HashedShingles, Int)] = documents.map(doc => hashShingles(doc)).toIndexedSeq.zipWithIndex

  println(documents)
  println(hashedDocuments)


  /** Get K-shingles from a file
    *
    * @param name file to open
    * @param shingleSize amount of k-shingles
    * @return Document
    */
  def getShinglesFromFile(name: String, shingleSize: Int): Document = {
    Source.fromFile(name) match {
      case source =>  source.toList.sliding(shingleSize).map(_.mkString).toSet
      case _ => Set.empty[String]
    }
  }

  /** For each Document, convert k-shingles to integer
    *
    * Hash idea taken from: https://github.com/taivop/eth-ir-project/blob/master/p2/src/main/scala/ch/ethz/dal/tinyir/shingling/MinHash.scala
    * @param doc
    */
  def hashShingles(doc: Document): HashedShingles =
    doc.map(shingle => Math.abs(shingle.hashCode)% minHashFunctions)


  /** Calculate the Jaccard Similarity between two sets
    *
    * @param docOne Set of k-shingles
    * @param docTwo Set of k-shingles
    * @return intersection/union as BigDeccimal
    */
  def jaccardSimilarity(docOne: Document, docTwo: Document): BigDecimal = {
    val intersection = docOne.intersect(docTwo).size
    val union = docOne.union(docTwo).size
    return BigDecimal(intersection.toDouble/union.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP)
  }

  /**
    *
    * @param hashedShingles Shingles converted to integers
    */
  def minHash(hashedShingles: HashedShingles): Unit = {

  }





}
