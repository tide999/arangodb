////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 ArangoDB GmbH, Cologne, Germany
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

#include "HttpCommunicationFeature.h"

#include "SimpleHttpClient/Communicator.h"
#include "SimpleHttpClient/HttpCommunicationThread.h"

using namespace arangodb;
using namespace arangodb::communicator;

HttpCommunicationFeature::HttpCommunicationFeature(
    application_features::ApplicationServer* server)
    : ApplicationFeature(server, "HttpCommunication") {
  setOptional(false);
  requiresElevatedPrivileges(false);
  startsAfter("Logger");
}

void HttpCommunicationFeature::prepare() {
  _communicator = std::make_shared<communicator::Communicator>();
  _workThread.reset(new HttpCommunicationThread(_communicator));
}

void HttpCommunicationFeature::start() {
  if (!_workThread->start()) {
    LOG_TOPIC(FATAL, Logger::CLUSTER)
      << "ClusterComm background thread does not work";
    FATAL_ERROR_EXIT();
  }
}

void HttpCommunicationFeature::stop() {
  _workThread->beginShutdown();
}
