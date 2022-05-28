package object part2dataframes {
  private val url = "jdbc:postgresql://localhost:5432/rtjvm"
  private val driver = "org.postgresql.Driver"
  private val user = "docker"
  private val password = "docker"
  val myDbOptions = Map("driver" -> driver, "url" -> url, "user" -> user, "password" -> password)
}
