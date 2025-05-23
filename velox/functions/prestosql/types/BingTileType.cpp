/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/functions/prestosql/types/BingTileType.h"
#include <folly/Expected.h>
#include <algorithm>
#include <optional>
#include <string>

namespace facebook::velox {

namespace {
folly::Expected<int64_t, std::string> mapSize(uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(zoomLevel > BingTileType::kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Zoom level {} is greater than max zoom {}",
        zoomLevel,
        BingTileType::kBingTileMaxZoomLevel));
  }
  return 256L << zoomLevel;
}

int32_t axisToCoordinates(double axis, long mapSize) {
  int32_t tileAxis = std::clamp<int32_t>(
      static_cast<int32_t>(axis * mapSize),
      0,
      static_cast<int32_t>(mapSize - 1));
  return tileAxis / BingTileType::kTilePixels;
}

/**
 * Given longitude in degrees, and the level of detail, the tile X coordinate
 * can be calculated as follows: pixelX = ((longitude + 180) / 360) * 2**level
 * The latitude and longitude are assumed to be on the WGS 84 datum. Even though
 * Bing Maps uses a spherical projection, it’s important to convert all
 * geographic coordinates into a common datum, and WGS 84 was chosen to be that
 * datum. The longitude is assumed to range from -180 to +180 degrees. <p>
 * reference: https://msdn.microsoft.com/en-us/library/bb259689.aspx
 */
folly::Expected<uint32_t, std::string> longitudeToTileX(
    double longitude,
    uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(
          longitude > BingTileType::kMaxLongitude ||
          longitude < BingTileType::kMinLongitude)) {
    return folly::makeUnexpected(fmt::format(
        "Longitude {} is outside of valid range [{}, {}]",
        longitude,
        BingTileType::kMinLongitude,
        BingTileType::kMaxLongitude));
  }
  double x = (longitude + 180) / 360;

  folly::Expected<int64_t, std::string> mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }

  return axisToCoordinates(x, mpSize.value());
}

/**
 * Given latitude in degrees, and the level of detail, the tile Y coordinate can
 * be calculated as follows: sinLatitude = sin(latitude * pi/180) pixelY = (0.5
 * – log((1 + sinLatitude) / (1 – sinLatitude)) / (4 * pi)) * 2**level The
 * latitude and longitude are assumed to be on the WGS 84 datum. Even though
 * Bing Maps uses a spherical projection, it’s important to convert all
 * geographic coordinates into a common datum, and WGS 84 was chosen to be that
 * datum. The latitude must be clipped to range from -85.05112878
 * to 85.05112878. This avoids a singularity at the poles, and it causes the
 * projected map to be square. <p> reference:
 * https://msdn.microsoft.com/en-us/library/bb259689.aspx
 */
folly::Expected<uint32_t, std::string> latitudeToTileY(
    double latitude,
    uint8_t zoomLevel) {
  if (FOLLY_UNLIKELY(
          latitude > BingTileType::kMaxLatitude ||
          latitude < BingTileType::kMinLatitude)) {
    return folly::makeUnexpected(fmt::format(
        "Latitude {} is outside of valid range [{}, {}]",
        latitude,
        BingTileType::kMinLatitude,
        BingTileType::kMaxLatitude));
  }
  double sinLatitude = sin(latitude * M_PI / 180);
  double y = 0.5 - log((1 + sinLatitude) / (1 - sinLatitude)) / (4 * M_PI);
  folly::Expected<int64_t, std::string> mpSize = mapSize(zoomLevel);
  if (FOLLY_UNLIKELY(mpSize.hasError())) {
    return folly::makeUnexpected(mpSize.error());
  }

  return axisToCoordinates(y, mpSize.value());
}
} // namespace

std::optional<std::string> BingTileType::bingTileInvalidReason(uint64_t tile) {
  // TODO?: We are duplicating some logic in isBingTileIntValid; maybe we
  // should extract?

  uint8_t version = BingTileType::bingTileVersion(tile);
  if (version != BingTileType::kBingTileVersion) {
    return fmt::format("Version {} not supported", version);
  }

  uint8_t zoom = BingTileType::bingTileZoom(tile);
  if (zoom > BingTileType::kBingTileMaxZoomLevel) {
    return fmt::format(
        "Bing tile zoom {} is greater than max zoom {}",
        zoom,
        BingTileType::kBingTileMaxZoomLevel);
  }

  uint64_t coordinateBound = 1ul << zoom;

  if (BingTileType::bingTileX(tile) >= coordinateBound) {
    return fmt::format(
        "Bing tile X coordinate {} is greater than max coordinate {} at zoom {}",
        BingTileType::bingTileX(tile),
        coordinateBound - 1,
        zoom);
  }
  if (BingTileType::bingTileY(tile) >= coordinateBound) {
    return fmt::format(
        "Bing tile Y coordinate {} is greater than max coordinate {} at zoom {}",
        BingTileType::bingTileY(tile),
        coordinateBound - 1,
        zoom);
  }

  return std::nullopt;
}

folly::Expected<uint64_t, std::string> BingTileType::bingTileParent(
    uint64_t tile,
    uint8_t parentZoom) {
  uint8_t tileZoom = bingTileZoom(tile);
  if (FOLLY_UNLIKELY(tileZoom == parentZoom)) {
    return tile;
  }
  uint32_t x = bingTileX(tile);
  uint32_t y = bingTileY(tile);

  if (FOLLY_UNLIKELY(tileZoom < parentZoom)) {
    return folly::makeUnexpected(fmt::format(
        "Parent zoom {} must be <= tile zoom {}", parentZoom, tileZoom));
  }
  uint8_t shift = tileZoom - parentZoom;
  return bingTileCoordsToInt((x >> shift), (y >> shift), parentZoom);
}

folly::Expected<std::vector<uint64_t>, std::string>
BingTileType::bingTileChildren(uint64_t tile, uint8_t childZoom) {
  uint8_t tileZoom = bingTileZoom(tile);
  if (FOLLY_UNLIKELY(tileZoom == childZoom)) {
    return std::vector<uint64_t>{tile};
  }
  uint32_t x = bingTileX(tile);
  uint32_t y = bingTileY(tile);

  if (FOLLY_UNLIKELY(childZoom < tileZoom)) {
    return folly::makeUnexpected(fmt::format(
        "Child zoom {} must be >= tile zoom {}", childZoom, tileZoom));
  }
  if (FOLLY_UNLIKELY(childZoom > kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Child zoom {} must be <= max zoom {}",
        childZoom,
        kBingTileMaxZoomLevel));
  }

  uint8_t shift = childZoom - tileZoom;
  uint32_t xBase = (x << shift);
  uint32_t yBase = (y << shift);
  uint32_t numChildrenPerOrdinate = 1 << shift;
  std::vector<uint64_t> children;
  children.reserve(numChildrenPerOrdinate * numChildrenPerOrdinate);
  for (uint32_t deltaX = 0; deltaX < numChildrenPerOrdinate; ++deltaX) {
    for (uint32_t deltaY = 0; deltaY < numChildrenPerOrdinate; ++deltaY) {
      children.push_back(
          bingTileCoordsToInt(xBase + deltaX, yBase + deltaY, childZoom));
    }
  }
  return children;
}

folly::Expected<uint64_t, std::string> BingTileType::bingTileFromQuadKey(
    const std::string_view& quadKey) {
  size_t zoomLevelInt32 = quadKey.size();
  if (FOLLY_UNLIKELY(zoomLevelInt32 > kBingTileMaxZoomLevel)) {
    return folly::makeUnexpected(fmt::format(
        "Zoom level {} is greater than max zoom {}",
        zoomLevelInt32,
        kBingTileMaxZoomLevel));
  }
  uint8_t zoomLevel = static_cast<uint8_t>(zoomLevelInt32);
  uint32_t tileX = 0;
  uint32_t tileY = 0;
  for (uint8_t i = zoomLevel; i > 0; i--) {
    int mask = 1 << (i - 1);
    switch (quadKey.at(zoomLevel - i)) {
      case '0':
        break;
      case '1':
        tileX |= mask;
        break;
      case '2':
        tileY |= mask;
        break;
      case '3':
        tileX |= mask;
        tileY |= mask;
        break;
      default:
        return folly::makeUnexpected(
            fmt::format("Invalid QuadKey digit sequence: {}", quadKey));
    }
  }
  return BingTileType::bingTileCoordsToInt(tileX, tileY, zoomLevel);
}

std::string BingTileType::bingTileToQuadKey(uint64_t tile) {
  uint8_t zoomLevel = bingTileZoom(tile);
  uint32_t tileX = bingTileX(tile);
  uint32_t tileY = bingTileY(tile);

  std::string quadKey;
  quadKey.resize(zoomLevel);

  for (uint8_t i = zoomLevel; i > 0; i--) {
    char digit = '0';
    int mask = 1 << (i - 1);
    if ((tileX & mask) != 0) {
      digit++;
    }
    if ((tileY & mask) != 0) {
      digit += 2;
    }
    quadKey[zoomLevel - i] = digit;
  }
  return quadKey;
}

/**
 * Returns a Bing tile at a given zoom level containing a point at a given
 * latitude and longitude. Latitude must be within [-85.05112878, 85.05112878]
 * range. Longitude must be within [-180, 180] range. Zoom levels from 1 to 23
 * are supported. For latitude/longitude values on the border of multiple tiles,
 * the southeastern tile is returned. For example, bing_tile_at(0,0,3) will
 * return a Bing tile with coordinates (4,4)
 */
folly::Expected<uint64_t, std::string> BingTileType::latitudeLongitudeToTile(
    double latitude,
    double longitude,
    uint8_t zoomLevel) {
  auto tileX = longitudeToTileX(longitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileX.hasError())) {
    return folly::makeUnexpected(tileX.error());
  }

  auto tileY = latitudeToTileY(latitude, zoomLevel);
  if (FOLLY_UNLIKELY(tileY.hasError())) {
    return folly::makeUnexpected(tileY.error());
  }

  return bingTileCoordsToInt(tileX.value(), tileY.value(), zoomLevel);
}

} // namespace facebook::velox
