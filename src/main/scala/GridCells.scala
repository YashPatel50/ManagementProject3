import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

object GridCells {
  def main(args: Array[String]) {
    val spark = SparkSession.builder()
      .appName("GridCells")
      .master("local[*]")
      .getOrCreate()

    val filePath = "hdfs://localhost:9000/user/Project3/points"

    println("Reading in points from:", filePath)

    //Read the points dataset
    val points = spark.read
      .option("header", false)
      .option("inferSchema", true)
      .csv(filePath)
      .withColumnRenamed("_c0", "a")
      .withColumnRenamed("_c1", "b")

    println("Finished reading points")

    println("Creating the Grid Cells")
    val gridCellsMapped = points.rdd.map(row =>{
      //Get the x and y of the point
      var a = row.getAs[Integer]("a")
      var b = row.getAs[Integer]("b")

      //Since we are doing 0 - 499, we have to make the last number technically be in the previous grid
      if (a == 10000){
        a = 9999
      }
      if (b == 10000){
        b = 9999
      }
      //Grid Calculation
      val key = (a / 20) + (b / 20 * 500)
      (key, 1)
    })

    // Reduce the pairs by key to get the total count for each key
    val gridCellsReduced = gridCellsMapped.reduceByKey(_ + _)

    println("Finished Creating the Grid Cells")

    println("Finding the Neighbors of each Grid Cell")
    // Create a map of grids to their neighbors
    val neighborsMap = gridCellsReduced.flatMap(row => {
      val grid = row._1
      val count = row._2
      val grid_count = grid.toString + "," + count.toString
      val (r, c) = getCellCoords(grid)
      val topRow = r - 1
      val bottomRow = r + 1
      val leftCol = c - 1
      val rightCol = c + 1

      //Each element in the list looks like:
      // (neighborGridID, gridID_count)
      List(
        (getCellId(topRow, leftCol), grid_count), //Top Left
        (getCellId(topRow, c), grid_count), //Top
        (getCellId(topRow, rightCol), grid_count), //Top Right
        (getCellId(r, leftCol), grid_count), //Left
        (getCellId(r, rightCol), grid_count), //Right
        (getCellId(bottomRow, leftCol), grid_count), //Bottom left
        (getCellId(bottomRow, c), grid_count), //Bottom
        (getCellId(bottomRow, rightCol), grid_count), //Bottom Right
        (grid, grid_count) //The actual grid: Used for its own count in relative index
      ).filter{case (neighbor, _) => neighbor >= 0 && neighbor < 500 * 500
      }
    })

    println("Finished finding the Neighbors")
    println("Calculating relative-density index")
    val density_index = neighborsMap.groupByKey().map(
      //For each of the grids we calculate the relative density index
      value => {
        //Get the grid
        val grid = value._1
        //Get the list of grid + count
        //In this case the grid is the neighbor
        val grid_counts = value._2

        //Variables for density calculation
        var main_grid_count = 0
        var num_neighbors = (grid_counts.toSeq.length - 1).toDouble
        var sum_counts = 0

        //Go through each of the neighbors of the grid
        grid_counts.foreach(grid_count => {
          val arr = grid_count.split(",")
          val g = arr(0).toInt
          val count = arr(1).toInt
          //If the "neighbor" is actually the grid, we store that count separately
          if (g == grid){
            main_grid_count = count
          } else {
            sum_counts += count
          }
        })

        //Compute the relative density index : grid.count / average(neighbors.count)
        val average = sum_counts / num_neighbors
        val density_index = main_grid_count / average
        (grid, density_index)
      }
    )
    println("Finished Calculating relative-density index")

    println("Sorting the relative indexes")
    //Sorts the entire RDD
    val sorted_density_index = density_index.sortBy({case (_, density) => density}, false)

    //Take only the first 50 rows
    val top50 = sorted_density_index.take(50)
    //Create a dataframe
    val topIndex = spark.createDataFrame(top50)
        .withColumnRenamed("_1", "grid")
        .withColumnRenamed("_2", "relative_density_index")

    println("Finished Sorting the relative indexes")

    println("Top 50 grid cells on Relative Density Index")
    //Ensure to print all of them
    topIndex.show(50)
    println()

    println("Calculating Neighbors of top 50")
    val neighborTop50Mapped = topIndex.rdd.flatMap(row => {
      //Get the neighbors for each of the grids
      val grid = row.getAs[Int]("grid")
      val (r, c) = getCellCoords(grid)
      val topRow = r - 1
      val bottomRow = r + 1
      val leftCol = c - 1
      val rightCol = c + 1

      //Store it as (neighborID, 0)
      //The 0 is just needed to run reduceByKey()
      List(
        (getCellId(topRow, leftCol), 0), //Top Left
        (getCellId(topRow, c), 0), //Top
        (getCellId(topRow, rightCol), 0), //Top Right
        (getCellId(r, leftCol), 0), //Left
        (getCellId(r, rightCol), 0),  //Right
        (getCellId(bottomRow, leftCol), 0),  //Bottom left
        (getCellId(bottomRow, c), 0), //Bottom
        (getCellId(bottomRow, rightCol), 0), //Bottom Right
      ).filter{case (neighbor, _) => neighbor >= 0 && neighbor < 500 * 500
      }
    })

    //Removes the duplicate neighbors (0 +0) is 0 so nothing really will come of it
    val neighborTop50Reduced = neighborTop50Mapped.reduceByKey(_+_)

    //Inner join of the two RDD
    val joined = neighborTop50Reduced.join(sorted_density_index)

    //Cleans it up to remove the 0
    val cleanUpJoined = joined.map(row => (row._1, row._2._2))

    //Create the Dataframe for printing
    val neighborIndex = spark.createDataFrame(cleanUpJoined)
      .withColumnRenamed("_1", "neighbor_grid")
      .withColumnRenamed("_2", "relative_density_index")

    println("Reporting Neighbors of top 50")
    neighborIndex.show()
    spark.stop()
  }

  //Given an id get the row and column
  def getCellCoords(id: Int): (Int, Int) = {
    val row = id / 500
    val col = id % 500
    (row, col)
  }

  //Given a row and column return the cell id, Ensures to only return valid ids
  def getCellId(row: Int, col: Int): Int = {
    //Only get valid Cells
    if (row < 0 || col < 0 || row >= 500 || col >= 500){
      return -1
    }
    row * 500 + col
  }

}