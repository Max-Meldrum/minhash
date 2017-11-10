// Max Meldrum

import scala.io.Source
import scala.util.Random


object MinhashLSH extends App {
  type Document = Set[String]
  type HashedShingles = Set[Int]
  type Signature = Set[Int]

  val shingleSize = 3
  val minHashFunctions = 100
  val threshold = 0.8
  val maxShingle = 2147483647
  // Next prime num after maxShingle
  val bigPrime: Long = 2147483869L


  val files = Seq("data/data.txt", "data/data1.txt", "data/data2.txt")
  val documents: Seq[Document] = files.map(file => getShinglesFromFile(file, shingleSize))
  val hashedDocuments: Seq[(HashedShingles, Int)] = documents.map(doc => hashShingles(doc)).toIndexedSeq.zipWithIndex
  val coeffs = randomHashCoefficients(minHashFunctions)

  val signatures: Seq[(Int, Set[Int])] = hashedDocuments.map(doc => {
    (doc._2, minHash(doc._1, coeffs))
  })



  /** Get K-shingles from a file
    *
    * @param name file to open
    * @param shingleSize amount of k-shingles
    * @return Document
    */
  private def getShinglesFromFile(name: String, shingleSize: Int): Document = {
    Source.fromFile(name) match {
      case source =>  source.toList.sliding(shingleSize).map(_.mkString).toSet
      case _ => Set.empty[String]
    }
  }

  /** For each Document, convert k-shingles to integer
    *
    * @param doc
    */
  private def hashShingles(doc: Document): HashedShingles =
    doc.map(shingle => Math.abs(shingle.hashCode)% maxShingle)


  /** Calculate the Jaccard Similarity between two sets
    *
    * @param sigOne Set of k-shingles
    * @param sigTwo Set of k-shingles
    * @return intersection/union as BigDecimal
    */
  private def jaccardSimilarity(sigOne: Signature, sigTwo: Signature): BigDecimal = {
    val intersection = sigOne.intersect(sigTwo).size
    val union = sigOne.union(sigTwo).size
    return BigDecimal(intersection.toDouble/union.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP)
  }

  /**
    *
    * @param hashedShingles Shingles converted to integers
    */
  private def minHash(hashedShingles: HashedShingles, coeffs: Seq[(Int, Int)]): Set[Int] = {
    coeffs.map(co => {
      hashedShingles.map(shingle => generateHash(shingle, co._1, co._2)).min
    }).toSet
  }


  /** Generate Tuple of random coefficients
    *
    * @param max number of tuples to generate
    * @return  Seq of tuples
    */
  private def randomHashCoefficients(max: Int): Seq[(Int, Int)] =
    (1 to max).map(_ => (Math.abs(Random.nextInt(maxShingle)), Math.abs(Random.nextInt(maxShingle))))

  /**  h(x) = (ax + b) mod c
    *
    * @param x value to be hashed
    * @param a random value
    * @param b random value
    * @return hashed value
    */
  private def generateHash(x: Int, a: Int, b: Int): Int =
    ((a*x + b) % bigPrime).toInt
}
