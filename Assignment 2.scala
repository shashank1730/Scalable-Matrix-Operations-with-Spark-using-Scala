// Databricks notebook source
// MAGIC %md
// MAGIC ## Part 1
// MAGIC ### Matrix Multiplication of Coordinate Matrices

// COMMAND ----------

// Define two small matrices M and N
val M = Array(Array(1.0, 2.0), Array(3.0, 4.0))
val N = Array(Array(5.0, 6.0), Array(7.0, 8.0))

// COMMAND ----------

//// Convert the small matrices to RDDs
val M_RDD_Small = sc.parallelize(M.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })
val N_RDD_Small = sc.parallelize(N.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })



M_RDD_Small.collect


// COMMAND ----------

// MAGIC %md
// MAGIC #### [TODO] Function to perform matrix multiplication of coordinate matrices (40 Points)

// COMMAND ----------

def COOMatrixMultiply ( M: RDD[((Int,Int),Double)], N: RDD[((Int,Int),Double)] ): RDD[((Int,Int),Double)] = {
  val matrixproduct = M
    .map { case ((i, j), v) => (j, (i, v)) }
    .join(N.map { case ((j, k), w) => (j, (k, w)) })
    .map { case (_, ((i, v), (k, w))) => ((i, k), v * w) }
    .reduceByKey(_ + _)
  matrixproduct
}

// COMMAND ----------

val R_RDD_Coo = COOMatrixMultiply(M_RDD_Small, N_RDD_Small)
R_RDD_Coo.collect.foreach(println)

// COMMAND ----------

// MAGIC %md
// MAGIC ## Part 2
// MAGIC ### Matrix Multiplication of Block Matrices

// COMMAND ----------

// MAGIC %md
// MAGIC #### [TODO] Convert cordinate matrix to block matrix (20 Points)

// COMMAND ----------

def convertToBlockMatrix(
    rdd: RDD[((Int, Int), Double)], // Input RDD with (row, col) as key and value as the matrix element
    blockSize: Int = 2              // Block size, default is 2x2
): RDD[((Int, Int), Array[Double])] = {
  
  rdd
    .map { case ((row, col), value) =>
      val blockRow = row / blockSize       // Calculate which block the element belongs to
      val blockCol = col / blockSize      // Same for column
      val innerRow = row % blockSize      // Inside the block (row position inside block)
      val innerCol = col % blockSize     // Inside the block (col position inside block)
      ((blockRow, blockCol), (innerRow, innerCol, value)) // Return as a tuple (block position, (inner row, inner col, value))
    }
    .groupByKey()  // Group the values based on block position
    .mapValues { values =>
      val blockArray = Array.fill(blockSize * blockSize)(0.0)  // Create a zero-filled block array
      // Place the value in its correct position inside the block
      for ((r, c, v) <- values) {
        blockArray(r * blockSize + c) = v
      }
      blockArray  // Return the block array
    }
}


// COMMAND ----------

// MAGIC %md
// MAGIC #### [TODO] Function to perform matrix multiplication of block matrices (40 Points)

// COMMAND ----------

import org.apache.spark.rdd.RDD

// Block Matrix Multiplication Function
def blockMatrixMultiply(
    M: RDD[((Int, Int), Array[Double])],  // Matrix M in block format
    N: RDD[((Int, Int), Array[Double])],  // Matrix N in block format
    blockSize: Int = 2                    // Block size (2x2 by default)
): RDD[((Int, Int), Array[Double])] = {
  
  // Perform the block multiplication
  val result = M.cartesian(N)  // Cartesian product of M and N
    .filter { case (((blockRowM, blockColM), blockM), ((blockRowN, blockColN), blockN)) =>
      blockColM == blockRowN  // Ensure compatibility (M's columns match N's rows)
    }
    .map { case (((blockRowM, blockColM), blockM), ((blockRowN, blockColN), blockN)) =>
      // Multiply blocks and accumulate the result in a new block C
      val multipliedBlock = multiplyBlocks(blockM, blockN, blockSize)
      ((blockRowM, blockColN), multipliedBlock)
    }
    .reduceByKey { (blockA, blockB) =>
      // Sum the blocks (if multiple results for same block position)
      addBlocks(blockA, blockB, blockSize)
    }
  
  result
}

// Function to multiply two blocks
def multiplyBlocks(blockM: Array[Double], blockN: Array[Double], blockSize: Int): Array[Double] = {
  val resultBlock = Array.fill(blockSize * blockSize)(0.0)
  
  for (i <- 0 until blockSize) {
    for (j <- 0 until blockSize) {
      for (k <- 0 until blockSize) {
        resultBlock(i * blockSize + j) += blockM(i * blockSize + k) * blockN(k * blockSize + j)
      }
    }
  }
  resultBlock
}

// Function to add two blocks
def addBlocks(blockA: Array[Double], blockB: Array[Double], blockSize: Int): Array[Double] = {
  val resultBlock = Array.fill(blockSize * blockSize)(0.0)
  
  for (i <- 0 until blockSize) {
    for (j <- 0 until blockSize) {
      resultBlock(i * blockSize + j) = blockA(i * blockSize + j) + blockB(i * blockSize + j)
    }
  }
  resultBlock
}


// COMMAND ----------

// Example matrices M and N (4x4 matrices)
val M = Array(
  Array(1.0, 2.0, 3.0, 4.0),
  Array(5.0, 6.0, 7.0, 8.0),
  Array(9.0, 10.0, 11.0, 12.0),
  Array(13.0, 14.0, 15.0, 16.0)
)

val N = Array(
  Array(1.0, 2.0, 3.0, 4.0),
  Array(5.0, 6.0, 7.0, 8.0),
  Array(9.0, 10.0, 11.0, 12.0),
  Array(13.0, 14.0, 15.0, 16.0)
)

// Convert M and N to RDDs
val M_RDD_Small = sc.parallelize(M.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })
val N_RDD_Small = sc.parallelize(N.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })

// Convert to block format
val M_RDD_Block = convertToBlockMatrix(M_RDD_Small)
val N_RDD_Block = convertToBlockMatrix(N_RDD_Small)

// Perform block matrix multiplication
val resultRDD = blockMatrixMultiply(M_RDD_Block, N_RDD_Block)

// Print the result
resultRDD.collect().foreach {
  case ((blockRow, blockCol), blockArray) =>
    println(s"Block ($blockRow, $blockCol): ${blockArray.mkString(", ")}")
}


// COMMAND ----------

// MAGIC %md
// MAGIC
// MAGIC ## FINAL MATRIX AFTER BLOCK MATRIX MULTIPLICATION 

// COMMAND ----------

// Function to convert blocks back into the final matrix
def reconstructMatrix(resultRDD: RDD[((Int, Int), Array[Double])], matrixSize: Int, blockSize: Int): Array[Array[Double]] = {
  // Initialize the result matrix with zeros
  val resultMatrix = Array.fill(matrixSize)(Array.fill(matrixSize)(0.0))
  
  // Loop over the RDD result and place the values into the correct positions
  resultRDD.collect().foreach {
    case ((blockRow, blockCol), blockArray) =>
      for (i <- 0 until blockSize) {
        for (j <- 0 until blockSize) {
          // Place the block value into the corresponding position in the final matrix
          resultMatrix(blockRow * blockSize + i)(blockCol * blockSize + j) = blockArray(i * blockSize + j)
        }
      }
  }
  resultMatrix
}

// Reconstruct the final matrix from the blocks
val resultMatrix = reconstructMatrix(resultRDD, matrixSize = 4, blockSize = 2)

// Print the final result matrix
resultMatrix.foreach(row => println(row.mkString(", ")))


// COMMAND ----------

// MAGIC %md
// MAGIC ## Bonus
// MAGIC #### [TODO]  Test your implementation on large dataset (20 Points)

// COMMAND ----------

// Generate coordinate matrices of dimension 16384x16384
// Convert the coordinate matrices to block matrices using the function that you have written earlier. You should change the block size to 8*8
// Call COOMatrixMultiply function and BlockMatrixMultiply

// Generate a large matrix (16384x16384)
val rows = 16384
val cols = 16384

// For simplicity, we'll create a sparse matrix with some random values
val M = Array.fill(rows, cols)(scala.util.Random.nextDouble())
val N = Array.fill(rows, cols)(scala.util.Random.nextDouble())

// Convert to RDD in coordinate format
val M_RDD = sc.parallelize(M.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })
val N_RDD = sc.parallelize(N.zipWithIndex.flatMap { case (row, i) => row.zipWithIndex.map { case (value, j) => ((i.toInt, j.toInt), value) } })


// Convert to block format (8x8 blocks)
val M_RDD_Block = convertToBlockMatrix(M_RDD, blockSize = 8)
val N_RDD_Block = convertToBlockMatrix(N_RDD, blockSize = 8)


// Perform COO matrix multiplication
val cooResult = COOMatrixMultiply(M_RDD, N_RDD)
cooResult.collect().foreach {
  case ((row, col), value) =>
    println(s"($row, $col): $value")
}


// Perform Block matrix multiplication
val blockResult = blockMatrixMultiply(M_RDD_Block, N_RDD_Block)
blockResult.collect().foreach {
  case ((blockRow, blockCol), blockArray) =>
    println(s"Block ($blockRow, $blockCol): ${blockArray.mkString(", ")}")
}



// COMMAND ----------


