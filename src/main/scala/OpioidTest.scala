import org.apache.spark.sql.SparkSession
import java.util.Scanner
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet}
import java.util.Scanner

object OpioidTest {
  var state = ""
  var option: Int = 0
  val scanner = new Scanner(System.in)
  var spark: SparkSession = null
  val driver = "com.mysql.cj.jdbc.Driver"
  val url = "jdbc:mysql://localhost:3306/logindb"
  val username = "root"
  val password = "root"
  var user = ""
  var pass = ""
  var utype = ""
  var rowcount = 5
  var userdb = ""
  var passdb = ""
  var utypedb = ""

  def startMenu(): Unit = {
    println("US MORPHINE MILLIGRAM EQUIVALENT (MME) ANALYSIS TOOL (2010-2015)")
    println("----------------------------------")
    println(s"1) Show the top $rowcount US states with the highest/lowest average MME per capita")
    println(s"2) For a given US state, show the top $rowcount counties with highest/lowest MME per capita in 2015")
    println(s"3) Show the top $rowcount US counties with highest/lowest MME per capita")
    println(s"4) For a given US state, show the top $rowcount counties with highest percent increase/decrease from 2010 to 2015")
    println(s"5) Show the $rowcount US states with the lowest change from 2010 to 2015")
    println(s"6) For a given US state, show the $rowcount counties with the highest/lowest predicted 2020 MME")
    println("----------------------------------")
    println("Enter your option:")
    option = scanner.nextInt()
    if (option == 1) {
      println(s"Would you like to view the $rowcount states with the highest or lowest average MME?")
      println("----------------------------------")
      println("1) Highest")
      println("2) Lowest")
      println("----------------------------------")
      println("Enter your option:")
      option = scanner.nextInt()
      if (option == 1) {
        spark.sql(s"SELECT state,  AVG(mme_percap_2015) as avg_mme_percap from opioid_amounts group by state order by AVG(mme_percap_2015) desc limit $rowcount").show()
        returnToMenu()
      } else if (option == 2) {
        spark.sql(s"SELECT state, AVG(mme_percap_2015) as avg_mme_percap from opioid_amounts group by state order by AVG(mme_percap_2015) asc limit $rowcount").show()
        returnToMenu()
      }
    }
    else if (option == 2) {
      println("Which state would you like to query?")
      println("----------------------------------")
      println("Please type state abbreviation : Ex: 'CA'")
      state = scanner.next()
      println(s"Would you like to view $rowcount counties with the highest or lowest MME per capita?")
      println("----------------------------------")
      println("1) Highest")
      println("2) Lowest")
      println("----------------------------------")
      println("Enter your option:")
      option = scanner.nextInt()
      if (option == 1) {
        spark.sql(s"SELECT state, county, mme_percap_2015 FROM opioid_amounts WHERE state = '$state' order by mme_percap_2015 desc limit $rowcount").show()
        returnToMenu()
      } else if (option == 2) {
        spark.sql(s"SELECT state, county, mme_percap_2015 FROM opioid_amounts WHERE state = '$state' order by mme_percap_2015 asc limit $rowcount").show()
        returnToMenu()
      }
    }
    else if (option == 3) {
      println(s"Would you like to view $rowcount counties in the US with the highest or lowest MME per capita?")
      println("----------------------------------")
      println("1) Highest")
      println("2) Lowest")
      println("----------------------------------")
      println("Enter your option:")
      option = scanner.nextInt()
      if (option == 1) {
        spark.sql(s"SELECT state, county, mme_percap_2015 from opioid_amounts order by mme_percap_2015 desc limit $rowcount").show()
        returnToMenu()
      } else if (option == 2) {
        spark.sql(s"Select state, county, mme_percap_2015 from opioid_amounts where mme_percap_2015 is not null order by mme_percap_2015 asc limit $rowcount").show()
        returnToMenu()
      }
    }
    else if (option == 4) {
      println("Which state would you like to query?")
      println("----------------------------------")
      println("Please type state abbreviation : Ex: 'CA'")
      state = scanner.next()
      println(s"Would you like to view $rowcount counties in $state with the greatest percent increase or percent decrease")
      println("----------------------------------")
      println("1) Percent increase")
      println("2) Percent decrease")
      println("----------------------------------")
      println("Enter your option:")
      option = scanner.nextInt()
      if (option == 1) {
        spark.sql(s"SELECT state, county, mme_percap_2010, mme_percap_2015, (mme_percap_2015-mme_percap_2010)/mme_percap_2010*100 as percent_inc FROM opioid_amounts WHERE state = '$state' order by (mme_percap_2015-mme_percap_2010)/mme_percap_2010*100 desc limit $rowcount").show()
        returnToMenu()
      } else if (option == 2) {
        spark.sql(s"SELECT state, county, mme_percap_2010, mme_percap_2015, (mme_percap_2010-mme_percap_2015)/mme_percap_2010*100 as percent_dec FROM opioid_amounts WHERE state = '$state' order by (mme_percap_2010-mme_percap_2015)/mme_percap_2010*100 desc limit $rowcount").show()
        returnToMenu()
      }
    }

    else if (option == 5) {
      println("Which state would you like to query?")
      println("----------------------------------")
      println("Please type state abbreviation : Ex: 'CA'")
      state = scanner.next()
      spark.sql(s"SELECT state, county, mme_percap_2010, mme_percap_2015, ABS((mme_percap_2015-mme_percap_2010)/mme_percap_2010*100) as percent_change FROM opioid_amounts WHERE state = '$state' AND mme_percap_2010 is not null AND mme_percap_2015 is not null order by ABS((mme_percap_2015-mme_percap_2010)/mme_percap_2010*100) asc limit $rowcount").show()
      returnToMenu()
    }
    else if (option == 6) {
      println("Which state would you like to query?")
      println("----------------------------------")
      println("Please type state abbreviation : Ex: 'CA'")
      state = scanner.next()
      println(s"Would you like to view the $rowcount counties with the highest or lowest predicted MME?")
      println("----------------------------------")
      println("1) Highest")
      println("2) Lowest")
      println("----------------------------------")
      println("Enter your option:")
      option = scanner.nextInt()
      if(option == 1) {
        spark.sql(s"SELECT state, county, mme_percap_2010, mme_percap_2015, mme_percap_2015/mme_percap_2010 as rate, mme_percap_2015*mme_percap_2015/mme_percap_2010 as 2020_MME_prediction FROM opioid_amounts WHERE state = '$state' AND mme_percap_2010 is not null AND mme_percap_2015 is not null order by 2020_MME_prediction desc limit $rowcount").show()
        returnToMenu()
      }
      else if(option == 2) {
        spark.sql(s"SELECT state, county, mme_percap_2010, mme_percap_2015, mme_percap_2015/mme_percap_2010 as rate, mme_percap_2015*mme_percap_2015/mme_percap_2010 as 2020_MME_prediction FROM opioid_amounts WHERE state = '$state' AND mme_percap_2010 is not null AND mme_percap_2015 is not null order by 2020_MME_prediction asc limit $rowcount").show()
        returnToMenu()
      }
    }
  }

  def returnToMenu(): Unit = {
    println("Would you like to?")
    println("----------------------------------")
    println("1) Return to menu")
    println("2) Return to login")
    option = scanner.nextInt()
    if (option == 1) {
      startMenu()
    } else if (option == 2) {
      loginMenu()
    }
  }

  def loginMenu(): Unit = {
    println("Please enter USERNAME")
    user = scanner.next()
    println("Please enter PASSWORD")
    pass = scanner.next()
    println("Please enter USERTYPE")
    utype = scanner.next()
    Class.forName("com.mysql.jdbc.Driver")
    val connection: Connection = DriverManager.getConnection(url, username, password)
    val query = "SELECT user, pass, utype FROM login WHERE user = ? AND pass = ? AND utype = ?"
    val statement : PreparedStatement = connection.prepareStatement(query)
    statement.setString(1, user)
    statement.setString(2, pass)
    statement.setString(3, utype)
    var resultSet : ResultSet = statement.executeQuery()
    while(resultSet.next()) {
      userdb = resultSet.getString("user")
      passdb = resultSet.getString("pass")
      utypedb = resultSet.getString("utype")
    }
    if(utypedb == "admin") {
      rowcount = 20
    } else {
      rowcount = 5
    }
    if(passdb == pass && userdb == user && utypedb == utype) {
      startMenu()
    } else {
      println("INCORRECT USERNAME OR PASSWORD")
      loginMenu()
    }
  }

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows
    System.setProperty("hadoop.home.dir", "C:\\hadoop")
    spark = SparkSession
      .builder
      .appName("hello hive")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")

    spark.sql("drop table if exists opioid_amounts")
    println("table dropped")
    //creates table
    spark.sql("create table if not exists opioid_amounts(FIPS int,State String,County String,MME_PerCap_2010 double,MME_PerCap_2015 double,Quartile_2015 int,2010_2015_Change String) row format delimited fields terminated by ','");
    //loads data from input folder
    println("table created")
    spark.sql("Select * from opioid_amounts").show()
    spark.sql("load data local inpath 'input/opioid_perscription_amounts.csv' into table opioid_amounts")
    loginMenu()
  }
}
