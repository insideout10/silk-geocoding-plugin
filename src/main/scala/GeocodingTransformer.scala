import java.net.{HttpURLConnection, URL, URLEncoder}
import java.util.logging.{Level, Logger}

import de.fuberlin.wiwiss.silk.linkagerule.input.Transformer
import de.fuberlin.wiwiss.silk.runtime.plugin.Plugin

import scala.io.Source

/**
 * @since 1.0.0
 */
@Plugin(
  id = "geocoding",
  categories = Array("API"),
  label = "Geocoding",
  description = "Returns latitude/longitude of an address."
)
case class GeocodingTransformer(appKey: String, limit: Int = 1) extends Transformer {

  private val logger = Logger.getLogger(getClass.getName)

  /**
   *
   * @since 1.0.0
   *
   * @param values
   * @return
   */
  override def apply(values: Seq[Set[String]]): Set[String] = {

    logger.log(Level.FINE, "Processing (1).")

    // Get a vector from the Seq[Set[...]] in order to access the elements by their index to build the query string.
    val parameters = values.reduce(_ union _).toVector

    parameters.size match {

      // Return an empty set if the parameters size is less than 3.
      case size if size < 4 =>
        logger.log(Level.WARNING, s"Expected 4 parameters [ parameters size :: $size ]")
        Set()

      // Process the parameters.
      case _ =>
        val queryString =
          "?key=" + appKey + // The client Id.
            "&street=" + URLEncoder.encode(parameters(0), "UTF-8") +
            "&city=" + URLEncoder.encode(parameters(2), "UTF-8") +
            "&postalCode=" + URLEncoder.encode(parameters(1), "UTF-8") +
            "&country=" + URLEncoder.encode(parameters(3), "UTF-8")

        logger.log(Level.FINE, "[ query-string :: " + queryString + " ]")

        // Return the set of Ids otherwise return an empty set.
        MapQuestClient.request("", queryString).getOrElse(Set())
          .slice(0, limit) // return only the number of requested results.

    }

  }

}


/**
 * Provides methods to query the remote MapQuest service.
 *
 * @since 1.0.0
 */
object MapQuestClient {

  import play.api.libs.json._

  private val logger = Logger.getLogger(MapQuestClient.getClass.getName)

  private val API_URL = "http://open.mapquestapi.com/geocoding/v1/"

  /**
   * Perform a request to MapQuest.
   *
   * @since 1.0.0
   *
   * @param path
   * @param queryString
   * @return
   */
  def request(path: String, queryString: String): Option[Set[String]] = {

    logger.log(Level.FINE, s"Going to perform a request to MapQuest [ path :: $path ][ query-string :: $queryString ]")

    // Combine the request in one URL.
    val url = new URL(API_URL + path + queryString)
    val connection = url.openConnection().asInstanceOf[HttpURLConnection]
    connection.setRequestMethod("GET")
    connection.setRequestProperty("Accept", "application/json")
    connection.connect()

    // Process only successful responses.
    connection.getResponseCode match {

      // Error response code.
      case code: Int if code < 200 || code >= 300 =>
        val responseMessage = connection.getResponseMessage
        logger.log(Level.WARNING, s"An error occurred [ code :: $code ][ url :: $url ][ response :: $responseMessage ].") // error
        None // Return no results.

      // Success (2xx)
      case _ =>

        // Get the response string using the specified encoding (or use UTF-8 by default).
        val encoding = if (null != connection.getContentEncoding) connection.getContentEncoding else "UTF-8"
        val responseString = Source.fromInputStream(connection.getInputStream, encoding).mkString

        // Decode the JSON.
        val json: JsValue = Json.parse(responseString)
        logger.log(Level.FINER, s"[ response-string :: $responseString ]")


        // Check Foursquare response.
        (json \ "results").as[JsArray] match {

          case results if 0 == results.value.size || 0 == (results(0) \ "locations").as[JsArray].value.length =>
            // No results / no locations.
            logger.log(Level.FINE, "No results.")
            None

          case results =>
            val latLng = (results(0) \ "locations")(0) \ "latLng"
            val latitude = latLng \ "lat"
            val longitude = latLng \ "lng"

            Some(Set(s"$latitude,$longitude"))

        }


    }
  }

}
