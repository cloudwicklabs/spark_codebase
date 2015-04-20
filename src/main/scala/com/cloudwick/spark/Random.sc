import java.io.File
import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder

val dbFile = new File("/Users/ashrith/Development/spark/src/main/resources/GeoLite2-City.mmdb")

val reader = new Builder(dbFile).build()

val ipAddress = InetAddress.getByName("0.0.0.0")

val response = reader.city(ipAddress)

val country = response.getCountry

val city = response.getCity