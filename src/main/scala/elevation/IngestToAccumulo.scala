package elevation

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.render.png._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.io._
import geotrellis.spark.io.accumulo.AccumuloInstance
import geotrellis.spark.io.s3._
import geotrellis.spark.pyramid.Pyramid


import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import geotrellis.vector.io._

import com.amazonaws.services.s3.model._
import org.apache.accumulo.core.client.security.tokens.PasswordToken
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.awt.Color.{RGBtoHSB, HSBtoRGB}
import java.io._

// object IngestToAccumulo {
//   def readResource(name: String): String = {
//     val stream: InputStream = getClass.getResourceAsStream(s"/$name")
//     try { scala.io.Source.fromInputStream( stream ).getLines.mkString(" ") } finally { stream.close() }
//   }

//   def continentalUS: MultiPolygon = {
//     val mp = readResource("united-states.json").parseGeoJson[MultiPolygon]
//     val crop = readResource("continental-crop.json").parseGeoJson[Polygon]
//     mp.intersection(crop).as[MultiPolygon].get
//   }

//   def main(args: Array[String]): Unit =
//     implicit val sc = SparkUtils.createSparkContext("Elevation Accumulo Ingest", new SparkConf(true))
//     try {

//       println(args.toSeq)
//       val etl = Etl(args)

//       val maskPoly = continentalUS

//       val partitioner = new HashPartitioner(25000)

//       // These are big images, so split them up and repartition.
//       val sourceTiles =
//         etl.load[ProjectedExtent, Tile]
//           .filter { case (key, value) => key.extent.intersects(maskPoly.envelope) }
//           .split(tileCols = 1024, tileRows = 1024)
//           .repartition(partitioner.numPartitions)

//       // Reproject and tile the layer, and then mask by our polygon.
//       val (zoom, unmaskedElevation) =
//         etl.tile(sourceTiles)

//       val wmMaskPoly = maskPoly.reproject(LatLng, WebMercator)
//       val elevation =
//         unmaskedElevation
//           .mask(wmMaskPoly)

//       println(s"MASKED META: ${unmaskedElevation.metadata}")

//       // Save to accumulo
//       val outputProps = conf.outputProps
//       val instance = AccumuloInstance(outputProps("instance"), outputProps("zookeeper"), outputProps("user"), new PasswordToken(outputProps("password")))
//       val layerWriter = AccumuloLayerWriter(instance)

//       val conf = etl.conf

//       Pyramid.levelStream(elevation, conf.layoutScheme()(conf.crs(), conf.tileSize()), zoom, Bilinear)
//         .foreach { case (z, layer) =>
//           // Write the raw elevation data
//           val layerId = LayerId(conf.layerName(), z)
//           layerWriter.write[SpatialKey, Tile, TileLayerMetadata](layerId, layer)
//           // Write the hillshade
//           val hillshadeLayerId = LayerId(s"{layerId.name}-hillshade", z)
//           layerWriter.write[SpatialKey, Tile, TileLayerMetadata](layerId, layer.hillshade(altitude = 60))

//           // If this is at the midpoint zoom, calculate the histogram and
//           // save off as an attribute in the zoom 0 layer
//           if(z == zoom / 2) {
//             // Save the histogram off for laster
//             val hist = layer.histogram(numBuckets = 100)
//             layerWriter.attributeStore.write(LayerId(layerId.name, 0), "histogram", hist)
//           }
//         }
//     } finally {
//       sc.stop()
//     }
//   }
// }
