////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2018 ArangoDB GmbH, Cologne, Germany
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
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include <mutex>

#include "Basics/conversions.h"
#include "Basics/VelocyPackHelper.h"
#include "Cluster/ClusterInfo.h"
#include "Cluster/ServerState.h"
#include "RestServer/ServerIdFeature.h"
#include "velocypack/StringRef.h"
#include "VocBase/LogicalCollection.h"
#include "VocBase/ticks.h"

#include "LogicalDataSource.h"

namespace {

std::string ensureGuid(
    std::string&& guid,
    TRI_voc_cid_t id,
    TRI_voc_cid_t planId,
    std::string const& name,
    bool isSystem
) {
  if (!guid.empty()) {
    return std::move(guid);
  }

  guid.reserve(64);

  if (arangodb::ServerState::instance()->isCoordinator()) {
    TRI_ASSERT(planId);
    guid.append("c");
    guid.append(std::to_string(planId));
  } else if (arangodb::ServerState::instance()->isDBServer()) {
    TRI_ASSERT(planId);
    guid.append("c");
    // we add the shard name to the collection. If we ever
    // replicate shards, we can identify them cluster-wide
    guid.append(std::to_string(planId));
    guid.push_back('/');
    guid.append(name);
  } else if (isSystem) {
    guid.append(name);
  } else {
    char buf[sizeof(TRI_server_id_t) * 2 + 1];
    auto len =
      TRI_StringUInt64HexInPlace(arangodb::ServerIdFeature::getId(), buf);

    TRI_ASSERT(id);
    guid.append("h");
    guid.append(buf, len);
    TRI_ASSERT(guid.size() > 3);
    guid.push_back('/');
    guid.append(std::to_string(id));
  }

  return std::move(guid);
}

TRI_voc_cid_t ensureId(TRI_voc_cid_t id) {
  if (id) {
    return id;
  }

  if (arangodb::ServerState::instance()->isCoordinator()
      || arangodb::ServerState::instance()->isDBServer()) {
    auto* ci = arangodb::ClusterInfo::instance();

    return ci ? ci->uniqid(1) : 0;
  }

  return TRI_NewTickServer();
}

} // namespace

namespace arangodb {

/*static*/ LogicalDataSource::Type const& LogicalDataSource::Type::emplace(
    arangodb::velocypack::StringRef const& name
) {
  struct Less {
    bool operator()(
        arangodb::velocypack::StringRef const& lhs,
        arangodb::velocypack::StringRef const& rhs
    ) const noexcept {
      return lhs.compare(rhs) < 0;
    }
  };
  static std::mutex mutex;
  static std::map<arangodb::velocypack::StringRef, LogicalDataSource::Type, Less> types;
  std::lock_guard<std::mutex> lock(mutex);
  auto itr = types.emplace(name, Type());

  if (itr.second) {
    const_cast<std::string&>(itr.first->second._name) = name.toString(); // update '_name'
    const_cast<arangodb::velocypack::StringRef&>(itr.first->first) =
      itr.first->second.name(); // point key at value stored in '_name'
  }

  return itr.first->second;
}

LogicalDataSource::LogicalDataSource(
    Category const& category,
    Type const& type,
    TRI_vocbase_t& vocbase,
    TRI_voc_cid_t id,
    std::string&& guid,
    TRI_voc_cid_t planId,
    std::string&& name,
    uint64_t planVersion,
    bool system,
    bool deleted
): _name(std::move(name)),
   _category(category),
   _type(type),
   _vocbase(vocbase),
   _id(ensureId(id)),
   _planId(planId ? planId : _id),
   _planVersion(planVersion),
   _guid(ensureGuid(std::move(guid), _id, _planId, _name, system)),
   _deleted(deleted),
   _system(system) {
  TRI_ASSERT(_id);
  TRI_ASSERT(!_guid.empty());
}

} // arangodb

// -----------------------------------------------------------------------------
// --SECTION--                                                       END-OF-FILE
// -----------------------------------------------------------------------------