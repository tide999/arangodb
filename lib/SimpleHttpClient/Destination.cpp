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
/// @author Andreas Streichardt
////////////////////////////////////////////////////////////////////////////////

#include "SimpleHttpClient/Destination.h"

using namespace arangodb::communicator;

// create a http url from the arango tcp+http stuff :S
std::string Destination::endpointToScheme(std::string const& endpoint) {
  if (endpoint.substr(0, 6) == "tcp://") {
    return "http://" + endpoint.substr(6);
  } else if (endpoint.substr(0, 11) == "http+tcp://") {
    return "http://" + endpoint.substr(11);
  } else if (endpoint.substr(0, 11) == "http+ssl://") {
    return "https://" + endpoint.substr(11);
  } else if (endpoint.substr(0, 6) == "ssl://") {
    return "https://" + endpoint.substr(6);
  } else {
    return endpoint;
  }
}