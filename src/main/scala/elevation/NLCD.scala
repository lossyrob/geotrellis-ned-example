package elevation

import geotrellis.raster._
import geotrellis.raster.histogram._
import geotrellis.raster.render._

import ColorRamps._

object NLCD {
  // Water has to be the darkest blue
  // Use actual color ramps for each category
  // Green is too bright
  // Orange is starkest

  // Use a dark blue to light green color for water. Created with colorbrewer.com
  val waterColorRamp =
    ColorRamp(0xf7fcf0ff, 0xe0f3dbff, 0xccebc5ff, 0xa8ddb5ff, 0x7bccc4ff, 0x4eb3d3ff, 0x2b8cbeff, 0x08589eff)
      .stops(200)

  val barrenLandColorRamp =
    HeatmapDarkRedToYellowWhite
      .stops(200)

  val forestColorRamp =
    GreenToRedOrange
      .stops(200)

  val shrubColorRamp =
    ColorRamp(LightToDarkGreen.colors.reverse)
      .stops(200)

  val farmColorRamp =
    ColorRamp(HeatmapYellowToRed.colors.reverse)
      .stops(200)

  val wetlandsColorRamp =
    ColorRamp(BlueToOrange.colors.reverse)
      .stops(200)

  val developedLandColorRamp =
    ClassificationBoldLandUse
      .stops(200)

  val defaultColorRamp =
    ColorRamp(BlueToOrange.colors.reverse)
      .stops(200)

  def colorFunc(histogram: Histogram[Double]): (Tile, Tile) => Tile = {
    val waterColorMap = waterColorRamp.toColorMap(histogram)
    val barrenLandColorMap = barrenLandColorRamp.toColorMap(histogram)
    val forestColorMap = forestColorRamp.toColorMap(histogram)
    val shrubColorMap = shrubColorRamp.toColorMap(histogram)
    val farmColorMap = farmColorRamp.toColorMap(histogram)
    val wetlandsColorMap = wetlandsColorRamp.toColorMap(histogram)
    val developedLandColorMap = developedLandColorRamp.toColorMap(histogram)
    val defaultColorMap = defaultColorRamp.toColorMap(histogram)

    { (nlcd, elevation) =>
      nlcd.map { (col, row, z) =>
        if(z == 0) { 0 }
        else {
          val e = elevation.getDouble(col, row)
          z match {
            case x if x == 11 =>
              waterColorMap.mapDouble(e)
            case x if x == 12 =>
              RGB(r = 209, g = 221, b = 249)
            case x if 20 <= x && x < 30 =>
              developedLandColorMap.mapDouble(e)
            case x if 30 <= x && x < 40 =>
              barrenLandColorMap.mapDouble(e)
            case x if 40 <= x && x < 50 =>
              forestColorMap.mapDouble(e)
            case x if 50 <= x && x < 80 =>
              shrubColorMap.mapDouble(e)
            case x if 80 <= x && x < 90 =>
              shrubColorMap.mapDouble(e)
            case x if 90 <= x && x < 100 =>
              wetlandsColorMap.mapDouble(e)
            case _ =>
              defaultColorMap.mapDouble(e)
          }
        }
      }
    }
  }

  // From http://www.mrlc.gov/nlcd06_leg.php
  // val colorMap =
  //   ColorMap(
  //     Map(
  //       0  -> 0x00000000, // Transparent
  //       11 -> 0x5475A8FF, // Open water
  //       12 -> 0xFFFFFFFF, // Perennial Ice/Snow
  //       21 -> 0xE8D1D1FF, // Developed, Open space
  //       22 -> 0xE29E8CFF, // Developed, Low intensity
  //       23 -> 0xFF0000FF, // Developed, Medium intensity
  //       24 -> 0xB50000FF, // Developed, High intensity
  //       31 -> 0xD2CDC0FF, // Barren Land
  //       41 -> 0x85C77EFF, // Deciduous Forest
  //       42 -> 0x38814EFF, // Evergreen Forest
  //       43 -> 0xD4E7B0FF, // Mixed Forest
  //       51 -> 0xAF963CFF, // Dwarf Shrub
  //       52 -> 0xDCCA8FFF, // Shrub/Scrub
  //       71 -> 0xFDE9AAFF, // Grassland/Herbaceous
  //       72 -> 0xD1D182FF, // Sedge/Herbaceous
  //       73 -> 0xA3CC51FF, // Lichens
  //       74 -> 0x82BA9EFF, // Moss
  //       81 -> 0xFBF65DFF, // Pasture/Hay
  //       82 -> 0xCA9146FF, // Cultivated Crops
  //       90 -> 0xC8E6F8FF, // Woody Wetlands
  //       95 -> 0x64B3D5FF // Emergent Herbaceous Wetlands
  //     )
  //   ).withBoundaryType(Exact).withFallbackColor(0xFFFFB3AA)

  // Color map from source geotiff color map
  val colorMap =
    ColorMap(
      Map(
        0  -> RGB(r = 0, g = 0, b = 0),
        1  -> RGB(r = 0, g = 249, b = 0),
        11 -> RGB(r = 71, g = 107, b = 160),
        12 -> RGB(r = 209, g = 221, b = 249),
        21 -> RGB(r = 221, g = 201, b = 201),
        22 -> RGB(r = 216, g = 147, b = 130),
        23 -> RGB(r = 237, g = 0, b = 0),
        24 -> RGB(r = 170, g = 0, b = 0),
        31 -> RGB(r = 178, g = 173, b = 163),
        32 -> RGB(r = 249, g = 249, b = 249),
        41 -> RGB(r = 104, g = 170, b = 99),
        42 -> RGB(r = 28, g = 99, b = 48),
        43 -> RGB(r = 181, g = 201, b = 142),
        51 -> RGB(r = 165, g = 140, b = 48),
        52 -> RGB(r = 204, g = 186, b = 124),
        71 -> RGB(r = 226, g = 226, b = 193),
        72 -> RGB(r = 201, g = 201, b = 119),
        73 -> RGB(r = 153, g = 193, b = 71),
        74 -> RGB(r = 119, g = 173, b = 147),
        81 -> RGB(r = 219, g = 216, b = 61),
        82 -> RGB(r = 170, g = 112, b = 40),
        90 -> RGB(r = 186, g = 216, b = 234),
        91 -> RGB(r = 181, g = 211, b = 229),
        92 -> RGB(r = 181, g = 211, b = 229),
        93 -> RGB(r = 181, g = 211, b = 229),
        94 -> RGB(r = 181, g = 211, b = 229),
        95 -> RGB(r = 112, g = 163, b = 186)
      )
    ).withBoundaryType(Exact).withFallbackColor(0xFFFFB3AA)
}
