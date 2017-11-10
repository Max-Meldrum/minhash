import scala.io.Source
import scala.util.Random

object MinhashLSH extends App {
  type Document = Set[String]
  type HashedShingles = Set[Int]
  type Signature = Seq[Int]

  val shingleSize = 5
  val minHashFunctions = 100
  val threshold = 0.5
  val maxShingle = 2147483647
  // Next prime num after maxShingle
  val bigPrime: Long = 2147483869L


  val files = Seq("data/data.txt", "data/data1.txt", "data/data2.txt", "data/data3.txt", "data/data4.txt")
  val documents: Seq[Document] = files.map(file => getShinglesFromFile(file, shingleSize))
  val hashedDocuments: Seq[(HashedShingles, Int)] = documents.map(doc => hashShingles(doc))
    .toIndexedSeq
    .zipWithIndex

  val coeffs = randomHashCoefficients(minHashFunctions)
  val signatures: Seq[(Int, Seq[Int])] = hashedDocuments.map(doc => {
    (doc._2, minHash(doc._1, coeffs))
  })

  println("Using a threshold size of " + threshold)
  println("Using shingle size of " + shingleSize)

  val sim = (s1: Signature, s2: Signature) => similarity(s1, s2)
  val jacc = (s1: Signature, s2: Signature) => jaccardSimilarity(s1, s2)

  // Measure time of running minhash similarities and jaccard similarity
  time(compare(sim))
  time(compare(jacc))

  private def compare(f: (Signature, Signature) => BigDecimal): Unit = {
    val size = signatures.size
    for (i <- 0 to size-1) {
      val signature = signatures(i)._2
      for (j <- (i +1) to (size-1)) {
        if (f(signature, signatures(j)._2) >= threshold) {
          println("Document " + files(i) + " matches with " + files(j))
        }
      }
    }
  }

  // Cred to https://gist.github.com/mariussoutier/3293709
  def time[T](block: => T): T = {
    val start = System.currentTimeMillis
    val res = block
    val totalTime = System.currentTimeMillis - start
    println("Elapsed time: %1d ms".format(totalTime))
    res
  }

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

  /** For each Document, convert k-shingles to integers
    *
    * @param doc Document containing the shingles to be hashed
    * @return HashedShingles set of ints
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


  /** Used to check similarity of two minhashed signatures
    *
    * @param sigOne Seq of Signatures
    * @param sigTwo Seq of Signatures
    * @return Minhash similarity
    */
  private def similarity(sigOne: Signature, sigTwo: Signature): BigDecimal = {
    var count = 0
    for (i <- 0 to sigOne.size-1) {
      if (sigOne(i) == sigTwo(i))
        count += 1
    }
    return BigDecimal(count.toDouble/sigOne.size.toDouble).setScale(2, BigDecimal.RoundingMode.HALF_UP)
  }

  /**
    *
    * @param hashedShingles Shingles converted to integers
    */
  private def minHash(hashedShingles: HashedShingles, coeffs: Seq[(Int, Int)]): Seq[Int] = {
    coeffs.map(co => {
      val max = Int.MaxValue
      val result =  hashedShingles.map(shingle => generateHash(shingle, co._1, co._2)).min
      if (result < max) result else max
    })
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
