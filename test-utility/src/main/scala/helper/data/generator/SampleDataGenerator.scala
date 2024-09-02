package helper.data.generator

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}

object SampleDataGenerator {


  def getNameMap(): Map[Int, String] = Map(1 -> "Olivia",
    2 -> "Liam",
    3 -> "Emma",
    4 -> "Noah",
    5 -> "Ava",
    6 -> "Oliver",
    7 -> "Sophia",
    8 -> "Elijah",
    9 -> "Isabella",
    10 -> "James",
    11 -> "Amelia",
    12 -> "Benjamin",
    13 -> "Mia",
    14 -> "Lucas",
    15 -> "Charlotte",
    16 -> "Mason",
    17 -> "Harper",
    18 -> "Ethan",
    19 -> "Evelyn",
    20 -> "Alexander",
    21 -> "Abigail",
    22 -> "Henry",
    23 -> "Emily",
    24 -> "Jacob",
    25 -> "Ella",
    26 -> "Michael",
    27 -> "Scarlett",
    28 -> "Daniel",
    29 -> "Grace",
    30 -> "Logan",
    31 -> "Chloe",
    32 -> "Jackson",
    33 -> "Victoria",
    34 -> "Sebastian",
    35 -> "Aria",
    36 -> "Aiden",
    37 -> "Penelope",
    38 -> "Matthew",
    39 -> "Riley",
    40 -> "Samuel",
    41 -> "Layla",
    42 -> "David",
    43 -> "Lillian",
    44 -> "Joseph",
    45 -> "Nora",
    46 -> "Carter",
    47 -> "Zoey",
    48 -> "Owen",
    49 -> "Lily",
    50 -> "Wyatt",
    51 -> "Hannah",
    52 -> "John",
    53 -> "Addison",
    54 -> "Jack",
    55 -> "Eleanor",
    56 -> "Luke",
    57 -> "Natalie",
    58 -> "Jayden",
    59 -> "Audrey",
    60 -> "Dylan",
    61 -> "Leah",
    62 -> "Levi",
    63 -> "Savannah",
    64 -> "Isaac",
    65 -> "Bella",
    66 -> "Gabriel",
    67 -> "Stella",
    68 -> "Julian",
    69 -> "Ellie",
    70 -> "Nathan",
    71 -> "Paisley",
    72 -> "Anthony",
    73 -> "Hazel",
    74 -> "Caleb",
    75 -> "Aurora",
    76 -> "Andrew",
    77 -> "Violet",
    78 -> "Joshua",
    79 -> "Aurora",
    80 -> "Christian",
    81 -> "Camila",
    82 -> "Landon",
    83 -> "Mila",
    84 -> "Jonathan",
    85 -> "Lucy",
    86 -> "Connor",
    87 -> "Grace",
    88 -> "Hudson",
    89 -> "Everly",
    90 -> "Adrian",
    91 -> "Emilia",
    92 -> "Thomas",
    93 -> "Madelyn",
    94 -> "Nolan",
    95 -> "Addison",
    96 -> "Easton",
    97 -> "Autumn",
    98 -> "Elias",
    99 -> "Kennedy",
    100 -> "Robert"
  )

  val getNameUDF = udf((nameIndex: Int) => getNameMap().getOrElse(nameIndex, "Undefined"))

  def getSampleEmployeeData(numRows: Int = 100000000)(implicit spark: SparkSession): DataFrame = {
    val nameMap: Broadcast[Map[Int, String]] = spark.sparkContext.broadcast(getNameMap())

    // Generate a DataFrame with columns id, name, age, salary
    val df: DataFrame = spark.range(1, numRows + 1).toDF("id")
      .withColumn("name_index",(rand() * 100).cast("int"))
      .withColumn("name", getNameUDF(col("name_index")))
      .withColumn("age", (rand() * 80 + 1).cast("int")) // Random age between 1 and 81
      .withColumn("salary", (rand() * 50000 + 30000).cast("int"))
      .withColumn("salary_date", date_sub(current_date(), col("name_index")))
      .withColumn("created_at", current_timestamp())
      .withColumn("updated_at", current_timestamp())
      .drop("name_index")
    df
  }

}
