/*
 * This file is part of the TrinityCore Project. See AUTHORS file for Copyright information
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 */

#ifndef TRINITYCORE_REWIRE_FIRESTORE_TRANSPORT_H
#define TRINITYCORE_REWIRE_FIRESTORE_TRANSPORT_H

#include "RewireConfig.h"
#include "RewireQueue.h"

#include <condition_variable>
#include <cstdint>
#include <mutex>
#include <string>
#include <thread>

namespace Trinity::Rewire
{
class FirestoreTransport final
{
public:
    FirestoreTransport(RewireConfig const& config, PersistentQueue& queue);
    ~FirestoreTransport();

    FirestoreTransport(FirestoreTransport const&) = delete;
    FirestoreTransport(FirestoreTransport&&) = delete;
    FirestoreTransport& operator=(FirestoreTransport const&) = delete;
    FirestoreTransport& operator=(FirestoreTransport&&) = delete;

    bool Start(std::string& error);
    void Stop();
    void Notify();

private:
    void Run();
    bool DeliverBatch(std::string& error);
    bool SendCommit(std::string const& body, std::string& error) const;
    std::string ReadAccessToken(std::string& error) const;

    RewireConfig _config;
    PersistentQueue& _queue;
    std::thread _worker;
    std::mutex _mutex;
    std::condition_variable _condition;
    bool _running = false;
    bool _wakeRequested = false;
    std::uint32_t _retryDelayMs = 0;
};
}

#endif // TRINITYCORE_REWIRE_FIRESTORE_TRANSPORT_H
