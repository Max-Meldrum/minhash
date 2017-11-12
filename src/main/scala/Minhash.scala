import scala.io.Source
import scala.util.Random

object Minhash extends App {
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
  val biggerFiles = Seq("data/2007_audi_a6", "data/2007_audi_a8", "data/2007_audi_q7",
    "data/2007_audi_rs4", "data/2007_audi_s8", "data/2007_bmw_3_series", "data/2007_bmw_5_series",
    "data/2007_bmw_6_series", "data/2007_bmw_x3", "data/2007_bmw_x5")

  checkFiles(files)
  checkFiles(biggerFiles)

  /** Just a helper to allow us to test different Sequences of files
    *
    * @param files Seq of files to be evaluated
    */
  private def checkFiles(files: Seq[String]): Unit = {
    println("Files being checked:")
    files.foreach(file => print(file + ", "))
    print("\n")

    val documents: Seq[Document] = files.map(file => getShinglesFromFile(file, shingleSize))
    val hashedDocuments: Seq[(HashedShingles, Int)] = documents.map(doc => hashShingles(doc))
      .toIndexedSeq
      .zipWithIndex

    val coeffs = randomHashCoefficients(minHashFunctions)
    val signatures: Seq[(Int, Seq[Int])] = hashedDocuments.map(doc => {
      (doc._2, minHash(doc._1, coeffs))
    })

    val sigItems = signatures.map(_._2)
    val hashedItems = hashedDocuments.map(_._1)

    println("Using a threshold size of " + threshold)
    println("Using shingle size of " + shingleSize)
    println("Going through " + files.size + " files\n")

    val sim = (s1: Signature, s2: Signature) => similarity(s1, s2)
    val jacc = (h1: HashedShingles, h2: HashedShingles) => jaccardSimilarity(h1, h2)

    // Measure time of running minhash similarities and jaccard similarity
    println("Running check with Minhash similarity")
    time(compare(sim, sigItems, files))
    println("Running check with Jaccard similarity")
    time(compare(jacc, hashedItems, files))
    print("\n")
  }

  /** Compare helper that allows us to check matching files
    *
    * @param f function that does the similarity check between two Signatures or HashedShingles
    * @param items Seq of collected Signatures/HashedShingles that we are checking
    * @tparam T Type -> Signature || HashedShingles
    */
  private def compare[T](f: (T, T) => BigDecimal, items: Seq[T], files: Seq[String]): Unit = {
    val size = items.size-1
    for (i <- 0 to size) {
      val signature = items(i)
      for (j <- (i +1) to size) {
        if (f(signature, items(j)) >= threshold)
          println("Document " + files(i) + " matches with " + files(j))
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
    * @param h1 Set of k-shingles
    * @param h2 Set of k-shingles
    * @return intersection/union as BigDecimal
    */
  private def jaccardSimilarity(h1: HashedShingles, h2: HashedShingles): BigDecimal = {
    val intersection = h1.intersect(h2).size
    val union = h1.union(h2).size
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

  /** Create signatures for a document
    *
    * @param hashedShingles Shingles converted to integers
    * @return Seq of signatures
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
