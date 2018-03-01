////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Simon Grätzer
////////////////////////////////////////////////////////////////////////////////

#ifndef ARANGOD_ROCKSDB_GEO_S2_INDEX_H
#define ARANGOD_ROCKSDB_GEO_S2_INDEX_H 1

#include "Basics/Result.h"
#include "GeoIndex/Index.h"
#include "RocksDBEngine/RocksDBIndex.h"
#include "VocBase/voc-types.h"

#include <s2/s2cell_id.h>
#include <velocypack/Builder.h>

class S2Region;

namespace arangodb {
class RocksDBGeoS2Index final : public RocksDBIndex, public geo_index::Index {
  friend class RocksDBSphericalIndexIterator;

 public:
  RocksDBGeoS2Index() = delete;

  RocksDBGeoS2Index(TRI_idx_iid_t, arangodb::LogicalCollection*,
                    velocypack::Slice const&, std::string const& typeName);

  ~RocksDBGeoS2Index() override {}

 public:
  IndexType type() const override {
    if ("geo1" == _typeName) {
      return TRI_IDX_TYPE_GEO1_INDEX;
    } else if ("geo2" == _typeName) {
      return TRI_IDX_TYPE_GEO2_INDEX;
    }
    return TRI_IDX_TYPE_S2_INDEX;
  }

  bool pointsOnly() const {
    return (_typeName != "s2index");
  }

  char const* typeName() const override { return _typeName.c_str(); }

  IndexIterator* iteratorForCondition(transaction::Methods*,
                                      ManagedDocumentResult*,
                                      arangodb::aql::AstNode const*,
                                      arangodb::aql::Variable const*,
                                      IndexIteratorOptions const&) override;

  bool allowExpansion() const override { return false; }

  bool canBeDropped() const override { return true; }

  bool isSorted() const override { return false; }

  bool hasSelectivityEstimate() const override { return false; }

  void toVelocyPack(velocypack::Builder&, bool, bool) const override;
  // Uses default toVelocyPackFigures

  bool matchesDefinition(velocypack::Slice const& info) const override;

  /// insert index elements into the specified write batch.
  Result insertInternal(transaction::Methods* trx, RocksDBMethods*,
                        LocalDocumentId const& documentId,
                        arangodb::velocypack::Slice const&,
                        OperationMode mode) override;

  /// remove index elements and put it in the specified write batch.
  Result removeInternal(transaction::Methods*, RocksDBMethods*,
                        LocalDocumentId const& documentId,
                        arangodb::velocypack::Slice const&,
                        OperationMode mode) override;

  /// @brief looks up all points within a given radius
  void withinQuery(transaction::Methods*, double, double,
                              double, std::string const&, VPackBuilder&) const;

  /// @brief looks up the nearest points
  void nearQuery(transaction::Methods*, double, double,
                            size_t, std::string const&, VPackBuilder&) const;

 private:
  std::string const _typeName;
};
}  // namespace arangodb

#endif
