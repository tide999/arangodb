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

#include "HttpCommunicationThread.h"

#include "Cluster/ClusterInfo.h"
#include "Logger/Logger.h"
#include "SimpleHttpClient/Communicator.h"

using namespace arangodb;
using namespace arangodb::communicator;

void HttpCommunicationThread::run() {
  LOG_TOPIC(DEBUG, Logger::COMMUNICATION) << "starting HttpCommunicationThread";

  while (!isStopping()) {
    try {
      _communicator->work_once();
      _communicator->wait();
      LOG_TOPIC(TRACE, Logger::CLUSTER) << "done waiting in HttpCommunicationThread";
    } catch (std::exception const& ex) {
      LOG_TOPIC(ERR, arangodb::Logger::FIXME) << "caught exception in HttpCommunicationThread: " << ex.what();
    } catch (...) {
      LOG_TOPIC(ERR, arangodb::Logger::FIXME) << "caught unknown exception in HttpCommunicationThread";
    }
  }
  _communicator->abortRequests();
  LOG_TOPIC(DEBUG, Logger::CLUSTER) << "waiting for curl to stop remaining handles";
  while (_communicator->work_once() > 0) {
    usleep(10);
  }

  LOG_TOPIC(DEBUG, Logger::CLUSTER) << "stopped HttpCommunicationThread";
}