package elevation

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.render._
import geotrellis.raster.render.png._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.io._
import geotrellis.spark.io.s3._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.render._
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._
import geotrellis.vector.io._

import com.amazonaws.services.s3.model._
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job
import org.apache.spark._
import org.apache.spark.rdd.RDD

import java.awt.Color.{RGBtoHSB, HSBtoRGB}
import java.io._

object Ingest {
  val targetLayoutScheme =
    ZoomedLayoutScheme(WebMercator, 512)

  def readResource(name: String): String = {
    val stream: InputStream = getClass.getResourceAsStream(s"/$name")
    try { scala.io.Source.fromInputStream( stream ).getLines.mkString(" ") } finally { stream.close() }
  }

  def continentalUS: MultiPolygon = {
    val mp = readResource("united-states.json").parseGeoJson[MultiPolygon]
    val crop = readResource("continental-crop.json").parseGeoJson[Polygon]
    mp.intersection(crop).as[MultiPolygon].get
  }

  def color(elevation: Tile, hillshade: Tile, cm: ColorMap): Tile =
    elevation.color(cm).combine(hillshade) { (rgba, z) =>
      // Convert to HSB, replace the brightness with the hillshade value, convert back to RGBA
      val (r, g, b, a) = rgba.unzipRGBA
      val hsbArr = java.awt.Color.RGBtoHSB(r, g, b, null)
      val (newR, newG, newB) = (java.awt.Color.HSBtoRGB(hsbArr(0), hsbArr(1), math.min(z, 160).toFloat / 160.0f) << 8).unzipRGB
      RGBA(newR, newG, newB, a)
    }

  // http://fwarmerdam.blogspot.com/2010/01/hsvmergepy.html
  // http://linfiniti.com/2010/12/a-workflow-for-creating-beautiful-relief-shaded-dems-using-gdal/
  def color(colors: Tile, hillshade: Tile): Tile =
    colors.combine(hillshade) { (rgba, z) =>
      // Convert to HSB, replace the brightness with the hillshade value, convert back to RGBA
      val (r, g, b, a) = rgba.unzipRGBA
      val hsbArr = java.awt.Color.RGBtoHSB(r, g, b, null)
      val (newR, newG, newB) = (java.awt.Color.HSBtoRGB(hsbArr(0), hsbArr(1), math.min(z, 160).toFloat / 160.0f) << 8).unzipRGB
      RGBA(newR, newG, newB, a)
    }

  def main(args: Array[String]): Unit =
    mainColorRampHillshade(args)
//    mainNLCD()

  // http://geotrellis-test.s3.amazonaws.com/elevation-ingest/{z}/{x}/{y}.png

  def mainColorRampHillshade(args: Array[String]): Unit = {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis ETL", new SparkConf(true))
    try {

      println(args.toSeq)
      val etl = Etl(args)

      val maskPoly = continentalUS

      val partitioner = new HashPartitioner(25000)

      // These are big images, so split them up and repartition.
      val sourceTiles =
        etl.load[ProjectedExtent, Tile]
          .filter { case (key, value) => key.extent.intersects(maskPoly.envelope) }
//          .sample(false, 0.20)
          .split(tileCols = 1024, tileRows = 1024)
          .repartition(partitioner.numPartitions)

      // Reproject and tile the layer, and then mask by our polygon.
      val (zoom, unmaskedElevation) =
        etl.tile(sourceTiles)

//      layerWriter.write(LayerId("elevation", zoom), unmaskedElevation, ZCurveIndexMethod)

      println(s"UNMASKED META: ${unmaskedElevation.metadata}")

      val wmMaskPoly = maskPoly.reproject(LatLng, WebMercator)
      val elevation =
        unmaskedElevation
          .filter()
          .where(Intersects(wmMaskPoly))
          .result
          .mask(wmMaskPoly)

      println(s"MASKED META: ${unmaskedElevation.metadata}")

      // Use the histogram to create a color map
      val hist = elevation.histogramDouble
      val colorMap =
        ColorRamps.BlueToOrange
          .stops(75)
          .toColorMap(hist)

      println(s"THE HISTO BREAKS: ${hist.quantileBreaks(75).toSeq}")

      val conf = etl.conf
      val path = "s3://geotrellis-test/elevation-ingest/{z}/{x}/{y}.png"

//      Pyramid.upLevels(painted, conf.layoutScheme()(conf.crs(), conf.tileSize()), lowestZoom, BiCubic) { (layer, z) =>
      Pyramid.levelStream(elevation, conf.layoutScheme()(conf.crs(), conf.tileSize()), lowestZoom, Bilinear)
        .foreach { case (z, layer) =>
          val layerId = LayerId(conf.layerName(), z)
          val keyToPath = SaveToS3.spatialKeyToPath(layerId, path)

          // Color the tiles by the color map,
          // shading it according to the hillshade.
          layer
            .join(layer.hillshade(altitude = 60))
            .mapValues { case (tile1, tile2) => color(tile1, tile2, colorMap).renderPng().bytes }
            .saveToS3(keyToPath, { putObject =>
              putObject.withCannedAcl(CannedAccessControlList.PublicRead)
            })

        }
    } finally {
      sc.stop()
    }
  }

  def color(colors: Tile, elevation: Tile, hillshade: Tile, elevationSaturationMap: ColorMap): Tile =
    colors.map { (col, row, rgba) =>
      val e = elevation.getDouble(col, row)
      val shade = hillshade.get(col, row)
      // Convert to HSB, replace the brightness with the hillshade value,
      // modify the saturation based on the elevation, and convert back to RGBA.
      val (r, g, b, a) = rgba.unzipRGBA
      val hsbArr = RGBtoHSB(r, g, b, null)
      val hue = hsbArr(0)
      val saturation: Float =
        hsbArr(1) * ((50 + elevationSaturationMap.mapDouble(e)) / 100.0f)
      val brightness =
        math.min(shade, 160).toFloat / 160.0f

      HSBtoRGB(hue, saturation, brightness) << 8 | a
    }

  def loadFromS3(bucket: String, prefix: String, partitionCount: Int)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] = {
    val conf = {
      val job = Job.getInstance(sc.hadoopConfiguration)
      S3InputFormat.setBucket(job, "azavea-datahub")
      S3InputFormat.setPrefix(job, "raw/nlcd/nlcd_2011_landcover_full_raster_30m")
      S3InputFormat.setPartitionCount(job, 1000)
      job.getConfiguration
    }

    sc.newAPIHadoopRDD(conf, classOf[GeoTiffS3InputFormat], classOf[ProjectedExtent], classOf[Tile])
  }

  def tile(rdd: RDD[(ProjectedExtent, Tile)], tileSize: Int, method: ResampleMethod, partitioner: Partitioner): (Int, TileLayerRDD[SpatialKey]) = {
    val (_, md) = TileLayerMetadata.fromRdd(rdd, FloatingLayoutScheme(tileSize))
    val tilerOptions = Tiler.Options(resampleMethod = method, partitioner = partitioner)
    val tiled = ContextRDD(rdd.tileToLayout[SpatialKey](md, tilerOptions), md)
    tiled.reproject(WebMercator, ZoomedLayoutScheme(WebMercator, tileSize), method)
  }

  def mainNLCD(): Unit = {
    implicit val sc = SparkUtils.createSparkContext("GeoTrellis Shaded NLCD Creation", new SparkConf(true))

    val partitioner = new HashPartitioner(25000)

    try {
      val filterPoly = continentalUS
      val maskPoly = filterPoly.reproject(LatLng, WebMercator)

      // Load & Tile NLCD
      val rawNlcd =
        loadFromS3("azavea-datahub", "raw/nlcd/nlcd_2011_landcover_full_raster_30m", 1000)

      val (nZoom, unmaskedNlcd) =
        tile(rawNlcd, 512, NearestNeighbor, partitioner)

      val nlcd =
        unmaskedNlcd.mask(maskPoly)

      println(s"NLCD: ${nlcd.metadata}")

      // Load & Tile Elevation
      val rawElevation =
        loadFromS3("azavea-datahub", "raw/ned-13arcsec-geotiff", 1115)
          .filter { case (key, value) => key.extent.intersects(filterPoly.envelope) }
          .split(tileCols = 1024, tileRows = 1024)
          .repartition(partitioner.numPartitions)

      val (eZoom, unmaskedElevation) =
        tile(rawElevation, 512, Bilinear, partitioner)

      val elevation =
        unmaskedElevation.mask(maskPoly)

      println(s"ELEV: ${elevation.metadata}")

      require(eZoom ==nZoom, "Elevation zoom and NLCD zoom aren't the same apparently.")

      // Calculate hillshade
      val hillshade =
        elevation.hillshade(altitude = 60)

      // Create "color ramp" that will render elevations
      val elevationSaturationMap =
        ColorMap(
          elevation
            .histogramDouble
            .quantileBreaks(50)
            .zipWithIndex
            .map { case (q, i) => (q, 50 - i) }
            .toMap
        ).withFallbackColor(1)


      // NLCD pyramid stream
      val nlcdLevels =
        Pyramid.levelStream(nlcd, targetLayoutScheme, nZoom, NearestNeighbor)

      // Elevation pyramid stream
      val elevationLevels =
        Pyramid.levelStream(elevation, targetLayoutScheme, eZoom, Bilinear)

      nlcdLevels
        .zip(elevationLevels)
        .foreach { case ((z, nlcdLayer), (_, elevationLayer)) =>
          val layerId = LayerId("nlcd-shade", z)
          val keyToPath = SaveToS3.spatialKeyToPath(layerId, "s3://geotrellis-test/{name}/{z}/{x}/{y}.png")

          nlcdLayer
            .color(NLCD.colorMap)
            .join(elevationLayer)
            .join(elevationLayer.hillshade(altitude = 60))
            .mapValues { case ((coloredNlcdTile, elevationTile), hillshadeTile) =>
              color(coloredNlcdTile, elevationTile, hillshadeTile, elevationSaturationMap).renderPng().bytes
            }
            .saveToS3(keyToPath, { putObject =>
              putObject.withCannedAcl(CannedAccessControlList.PublicRead)
            })
        }
    } finally {
      sc.stop()
    }
  }
}
