package elevation

import geotrellis.proj4._
import geotrellis.raster._
import geotrellis.raster.io.geotiff._
import geotrellis.raster.render._
import geotrellis.raster.render.png._
import geotrellis.raster.resample._
import geotrellis.spark._
import geotrellis.spark.etl.Etl
import geotrellis.spark.io.index.ZCurveKeyIndexMethod
import geotrellis.spark.io.s3._
import geotrellis.spark.pyramid.Pyramid
import geotrellis.spark.render._
import geotrellis.spark.testkit._
import geotrellis.spark.tiling._
import geotrellis.spark.util.SparkUtils
import geotrellis.vector._

import com.amazonaws.services.s3.model._
import org.apache.spark.SparkConf

object Test {
  def main(args: Array[String]): Unit = {
    hillshade()
  }

  def geojson(): Unit = {
    val mp = Ingest.continentalUS
  }

  def hillshadeSpark(): Unit = {
//    val tile = SinglebandGeoTiff("/Users/rob/tmp/elevation-test/elevation-wm.tif")
    val gt = SinglebandGeoTiff("/Users/rob/data/imgn38w112_13.tif")
    //-111.514712, 37.103610

//    val slope = tile.tile.slope(tile.rasterExtent.cellSize)
    implicit val sc = SparkUtils.createLocalSparkContext("local[*]", "GeoTrellis ETL")
    try {
      val (raster, rdd) = TileLayerRDDBuilders.createTileLayerRDD(gt.raster, 50, 50, gt.crs)
      val (z, reprojected) = rdd.reproject(WebMercator, ZoomedLayoutScheme(WebMercator, 512), Bilinear)

      val Extent(xmin, ymin, xmax, ymax) = gt.extent
      val dx = (xmax - xmin) / 3
      val dy = (ymax - ymin) / 2
      val mask = Extent(xmin + dx, ymin, xmax, ymin + dy).reproject(gt.crs, WebMercator)

      //    println(rdd.mask(mask.toPolygon).histogramDouble.quantileBreaks(70).toSeq)

      val masked = reprojected.mask(mask.toPolygon)

      val hs = masked.hillshade(altitude = 60)
      val hist = masked.histogram
      val cr = ColorRamps.BlueToOrange.stops(70)
      val cm = ColorMap.fromQuantileBreaks(hist, cr)

      masked
        .join(hs)
        .mapValues { case (tile1, tile2) => Ingest.color(tile1, tile2, cm) }
        .stitch
        .renderPng()
        .write("/Users/rob/tmp/elevation-test/hs-s1.png")
    } finally {
      sc.stop
    }
  }

  def hillshade(): Unit = {
    val gt = SinglebandGeoTiff("/Users/rob/data/elevation2.tif")

//   val hs = gt.tile.hillshade(gt.rasterExtent.cellSize, altitude = 70, zFactor = 2.0)
   val hs = gt.tile.hillshade(gt.rasterExtent.cellSize, altitude = 60)

    val cr = ColorRamps.BlueToOrange.stops(100)
    val hInt = gt.tile.histogramDouble(100)
//    val hDouble = gt.tile.histogramDouble
//    println("INT")
//    hInt.quantileBreaks(cr.numStops).zip(cr.colors).foreach { case (d, z) => println(f"$d -> $z%02X") }
    // println("Double")
    // hDouble.quantileBreaks(cr.numStops).zip(cr.colors).foreach { case (d, z) => println(f"$d -> $z%02X") }

//    val cm1 = ColorMap.fromQuantileBreaks(hInt, ColorRamps.BlueToOrange)
    val cm = ColorMap.fromQuantileBreaks(hInt, cr)
//    val cm1 = ColorMap.fromQuantileBreaks(hDouble, cr)

    //val cm = ColorMap.fromQuantileBreaks(hist, ColorRamps.BlueToOrange)
    // println(cm)

    // gt.tile.renderPng(cm1).write("/Users/rob/tmp/elevation-test/hs3-int.png")
    // gt.tile.renderPng(cm2).write("/Users/rob/tmp/elevation-test/hs3-double.png")

    // import geotrellis.raster.render.png._
    // val colorEncoding = PngColorEncoding(cm1.colors, cm1.options.noDataColor, cm1.options.fallbackColor)
    // println(colorEncoding)
    // val convertedColorMap = colorEncoding.convertColorMap(cm1)

    val colored = gt.tile.color(cm)

//    colored.renderPng(colorEncoding).write("/Users/rob/tmp/elevation-test/hs4.png")
    // val Tuple2(m, x) = colored.findMinMax
    // println(s"MIN MAX ON RASTER ${gt.tile.findMinMax}")
    // println(s"MIN MAX ON RASTER Double ${gt.tile.findMinMaxDouble}")
    // println(f"MIN MAX COLOR $m%02X , $x%02X")

    // colored.renderPng().write("/Users/rob/tmp/elevation-test/hs3.png")


    colored.combine(hs) { (rgba, z) =>
      val (r, g, b, a) = rgba.unzipRGBA
      val hsbArr = java.awt.Color.RGBtoHSB(r, g, b, null)
      val (newR, newG, newB) = (java.awt.Color.HSBtoRGB(hsbArr(0), hsbArr(1), math.min(z, 160).toFloat / 160.0f) << 8).unzipRGB
//      val (newR, newG, newB) = (java.awt.Color.HSBtoRGB(hsbArr(0), hsbArr(1), z.toFloat / 255.0f) << 8).unzipRGB
      RGBA(newR, newG, newB, a)
    }.renderPng().write("/Users/rob/tmp/elevation-test/hs8.png")

//    hs.renderPng(GreyPngEncoding).write("/Users/rob/tmp/elevation-test/hs2.png")

//    hs.map { z => z << 8 | 0xFF }.renderPng(GreyaPngEncoding).write("/Users/rob/tmp/elevation-test/hs2.png")
//       .renderPng(GreyaPngEncoding).write("/Users/rob/tmp/elevation-test/hs.png")
  }
}
