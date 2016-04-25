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
      if(rgba == 0) { 0 }
      else {
        // Convert to HSB, replace the brightness with the hillshade value, convert back to RGBA
        val (r, g, b, a) = rgba.unzipRGBA
        val hsbArr = java.awt.Color.RGBtoHSB(r, g, b, null)
        val (newR, newG, newB) = (java.awt.Color.HSBtoRGB(hsbArr(0), hsbArr(1), math.min(z, 160).toFloat / 160.0f) << 8).unzipRGB
        RGBA(newR, newG, newB, a)
      }
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
//    mainColorRampHillshade(args)
    mainNLCD()

  // http://geotrellis-test.s3.amazonaws.com/elevation-ingest/{z}/{x}/{y}.png
  // http://geotrellis-test.s3.amazonaws.com/nlcd-shade/{z}/{x}/{y}.png
  // http://geotrellis-test.s3.amazonaws.com/nlcd-shade-2/{z}/{x}/{y}.png

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
          .split(tileCols = 1024, tileRows = 1024)
          .repartition(partitioner.numPartitions)

      // Reproject and tile the layer, and then mask by our polygon.
      val (zoom, unmaskedElevation) =
        etl.tile(sourceTiles)

      val wmMaskPoly = maskPoly.reproject(LatLng, WebMercator)
      val elevation =
        unmaskedElevation
          .mask(wmMaskPoly)

      println(s"MASKED META: ${unmaskedElevation.metadata}")

      // Use the histogram to create a color map
      val hist = elevation.histogram(numBuckets = 100)
      val colorMap =
        ColorRamps.BlueToOrange
          .stops(100)
          .toColorMap(hist)

      println(s"THE HISTO BREAKS: ${hist.quantileBreaks(75).toSeq}")

      val conf = etl.conf
      val path = "s3://geotrellis-test/elevation-ingest/{z}/{x}/{y}.png"

      Pyramid.levelStream(elevation, conf.layoutScheme()(conf.crs(), conf.tileSize()), zoom, Bilinear)
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

  def colorNlcd(colors: Tile, hillshade: Tile): Tile =
    colors.map { (col, row, rgba) =>
      val shade = hillshade.get(col, row)
      // Convert to HSB, replace the brightness with the hillshade value,
      // modify the saturation based on the elevation, and convert back to RGBA.
      val (r, g, b, a) = rgba.unzipRGBA
      val hsbArr = RGBtoHSB(r, g, b, null)
      val hue = hsbArr(0)
      val saturation = hsbArr(1)
      val brightness =
        math.min(shade, 180).toFloat / 180.0f

      HSBtoRGB(hue, saturation, brightness) << 8 | a
    }

  def loadFromS3(bucket: String, prefix: String, partitionCount: Int)(implicit sc: SparkContext): RDD[(ProjectedExtent, Tile)] = {
    val conf = {
      val job = Job.getInstance(sc.hadoopConfiguration)
      S3InputFormat.setBucket(job, bucket)
      S3InputFormat.setPrefix(job, prefix)
      S3InputFormat.setPartitionCount(job, partitionCount)
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
      // Load & Tile Elevation
      val rawElevation =
        loadFromS3("azavea-datahub", "raw/ned-13arcsec-geotiff", 1115)
          .filter { case (key, value) => key.extent.intersects(continentalUS.envelope) }
          .split(tileCols = 1024, tileRows = 1024)
          .repartition(25000)

      val (zoom, elevation) =
        tile(rawElevation, 512, Bilinear, partitioner)

      println(s"ELEV: ${elevation.metadata}")

      // Load & Tile NLCD
      val rawNlcd =
        loadFromS3("azavea-datahub", "raw/nlcd/nlcd_2011_landcover_full_raster_30m", 4029)

      val (nZoom, tiledNlcd) =
        tile(rawNlcd, 512, NearestNeighbor, partitioner)

      // NLCD will tile to zoom 12, so resample to zoom 13 to match elevation
      val nlcd =
        tiledNlcd
          .resampleToZoom(nZoom, zoom, NearestNeighbor)

      println(s"NLCD: ${nlcd.metadata}")

      // NLCD pyramid stream
      val nlcdLevels =
        Pyramid.levelStream(nlcd, targetLayoutScheme, zoom, NearestNeighbor)

      // Elevation pyramid stream
      val elevationLevels =
        Pyramid.levelStream(elevation, targetLayoutScheme, zoom, Bilinear)

      // Create the color function based on the histgram of elevation values.
      val colorFunc =
        NLCD.colorFunc(elevation.histogram(numBuckets = 200))

      nlcdLevels
        .zip(elevationLevels)
        .foreach { case ((z, nlcdLayer), (_, elevationLayer)) =>
          val layerId = LayerId("nlcd-shade-2", z)
          val keyToPath = SaveToS3.spatialKeyToPath(layerId, "s3://geotrellis-test/{name}/{z}/{x}/{y}.png")

          nlcdLayer
            .convert(IntConstantNoDataCellType)
            .join(elevationLayer)
            .mapValues { case (nlcdTile, elevationTile) => colorFunc(nlcdTile, elevationTile) }
            .join(elevationLayer.hillshade(altitude = 60))
            .mapValues { case (coloredNlcdTile, hillshadeTile) =>
              colorNlcd(coloredNlcdTile, hillshadeTile).renderPng().bytes
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
