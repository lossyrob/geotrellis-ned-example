package elevation

import geotrellis.raster.render._

object NLCD {
  // From http://www.mrlc.gov/nlcd06_leg.php
  val colorMap =
    ColorMap(
      Map(
        11 -> 0x5475A8FF, // Open water
        12 -> 0xFFFFFFFF, // Perennial Ice/Snow
        21 -> 0xE8D1D1FF, // Developed, Open space
        22 -> 0xE29E8CFF, // Developed, Low intensity
        23 -> 0xFF0000FF, // Developed, Medium intensity
        24 -> 0xB50000FF, // Developed, High intensity
        31 -> 0xD2CDC0FF, // Barren Land
        41 -> 0x85C77EFF, // Deciduous Forest
        42 -> 0x38814EFF, // Evergreen Forest
        43 -> 0xD4E7B0FF, // Mixed Forest
        51 -> 0xAF963CFF, // Dwarf Shrub
        52 -> 0xDCCA8FFF, // Shrub/Scrub
        71 -> 0xFDE9AAFF, // Grassland/Herbaceous
        72 -> 0xD1D182FF, // Sedge/Herbaceous
        73 -> 0xA3CC51FF, // Lichens
        74 -> 0x82BA9EFF, // Moss
        81 -> 0xFBF65DFF, // Pasture/Hay
        82 -> 0xCA9146FF, // Cultivated Crops
        90 -> 0xC8E6F8FF, // Woody Wetlands
        95 -> 0x64B3D5FF // Emergent Herbaceous Wetlands
      )
    ).withBoundaryType(Exact).withFallbackColor(0xFFFFB3AA)
}
