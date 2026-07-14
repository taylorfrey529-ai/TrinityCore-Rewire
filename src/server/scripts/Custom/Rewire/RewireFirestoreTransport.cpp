/*
 * This file is part of the TrinityCore Project. See AUTHORS file for Copyright information
 *
 * This program is free software; you can redistribute it and/or modify it
 * under the terms of the GNU General Public License as published by the
 * Free Software Foundation; either version 2 of the License, or (at your
 * option) any later version.
 */

#include "RewireFirestoreTransport.h"

#include "Log.h"

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include <boost/asio/connect.hpp>
#include <boost/asio/ip/tcp.hpp>
#include <boost/asio/ssl/context.hpp>
#include <boost/asio/ssl/error.hpp>
#include <boost/asio/ssl/stream.hpp>
#include <boost/beast/core.hpp>
#include <boost/beast/http.hpp>
#include <boost/beast/ssl.hpp>
#include <openssl/ssl.h>

#include <algorithm>
#include <chrono>
#include <cstdlib>
#include <sstream>
#include <utility>
#include <vector>

namespace Trinity::Rewire
{
namespace
{
std::string BuildCommitBody(std::vector<std::string> const& writes)
{
    std::ostringstream body;
    body << "{\"writes\":[";
    for (std::size_t index = 0; index < writes.size(); ++index)
    {
        if (index != 0)
            body << ',';
        body << writes[index];
    }
    body << "]}";
    return body.str();
}
}

FirestoreTransport::FirestoreTransport(RewireConfig const& config, PersistentQueue& queue)
    : _config(config), _queue(queue), _retryDelayMs(config.Transport.InitialRetryMs)
{
}

FirestoreTransport::~FirestoreTransport()
{
    Stop();
}

bool FirestoreTransport::Start(std::string& error)
{
    if (!_config.Transport.Enabled)
    {
        error.clear();
        return true;
    }

    std::string tokenError;
    if (ReadAccessToken(tokenError).empty())
    {
        error = std::move(tokenError);
        return false;
    }

    {
        std::lock_guard lock(_mutex);
        if (_running)
        {
            error.clear();
            return true;
        }

        _running = true;
        _wakeRequested = true;
    }

    try
    {
        _worker = std::thread(&FirestoreTransport::Run, this);
    }
    catch (std::exception const& exception)
    {
        std::lock_guard lock(_mutex);
        _running = false;
        error = std::string("unable to start Firestore transport thread: ") + exception.what();
        return false;
    }

    error.clear();
    return true;
}

void FirestoreTransport::Stop()
{
    {
        std::lock_guard lock(_mutex);
        if (!_running)
            return;

        _running = false;
        _wakeRequested = true;
    }

    _condition.notify_all();
    if (_worker.joinable())
        _worker.join();
}

void FirestoreTransport::Notify()
{
    {
        std::lock_guard lock(_mutex);
        if (!_running)
            return;
        _wakeRequested = true;
    }

    _condition.notify_one();
}

void FirestoreTransport::Run()
{
    while (true)
    {
        {
            std::unique_lock lock(_mutex);
            _condition.wait(lock, [this]
            {
                return !_running || _wakeRequested;
            });

            if (!_running)
                break;

            _wakeRequested = false;
        }

        while (true)
        {
            std::string error;
            if (DeliverBatch(error))
            {
                _retryDelayMs = _config.Transport.InitialRetryMs;
                if (_queue.Pending() == 0)
                    break;
                continue;
            }

            TC_LOG_ERROR("server.rewire", "Firestore delivery failed; retrying in {} ms: {}", _retryDelayMs, error);

            std::unique_lock lock(_mutex);
            if (_condition.wait_for(lock, std::chrono::milliseconds(_retryDelayMs), [this]
                {
                    return !_running || _wakeRequested;
                }))
            {
                if (!_running)
                    return;
                _wakeRequested = false;
            }

            _retryDelayMs = std::min<std::uint32_t>(_retryDelayMs * 2, _config.Transport.MaxRetryMs);
        }
    }
}

bool FirestoreTransport::DeliverBatch(std::string& error)
{
    std::vector<std::string> batch = _queue.PeekBatch(_config.Transport.BatchSize);
    if (batch.empty())
    {
        error.clear();
        return true;
    }

    std::string const body = BuildCommitBody(batch);
    if (!SendCommit(body, error))
        return false;

    if (!_queue.Acknowledge(batch.size(), error))
        return false;

    TC_LOG_DEBUG("server.rewire", "Delivered and acknowledged {} Firestore write(s)", batch.size());
    return true;
}

bool FirestoreTransport::SendCommit(std::string const& body, std::string& error)
{
    std::string const token = ReadAccessToken(error);
    if (token.empty())
        return false;

    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = beast::http;
    using tcp = asio::ip::tcp;

    try
    {
        asio::io_context ioContext;
        asio::ssl::context sslContext(asio::ssl::context::tls_client);
        sslContext.set_default_verify_paths();
        sslContext.set_verify_mode(asio::ssl::verify_peer);

        tcp::resolver resolver(ioContext);
        beast::ssl_stream<beast::tcp_stream> stream(ioContext, sslContext);

        if (!SSL_set_tlsext_host_name(stream.native_handle(), _config.Transport.FirestoreHost.c_str()))
        {
            error = "unable to configure TLS SNI for Firestore";
            return false;
        }

        auto const endpoints = resolver.resolve(_config.Transport.FirestoreHost, _config.Transport.FirestorePort);
        beast::get_lowest_layer(stream).expires_after(std::chrono::milliseconds(_config.Transport.RequestTimeoutMs));
        beast::get_lowest_layer(stream).connect(endpoints);
        stream.handshake(asio::ssl::stream_base::client);

        std::string const target = "/v1/projects/" + _config.Firebase.ProjectId + "/databases/" +
            _config.Firebase.DatabaseId + "/documents:commit";

        http::request<http::string_body> request(http::verb::post, target, 11);
        request.set(http::field::host, _config.Transport.FirestoreHost);
        request.set(http::field::user_agent, "TrinityCore-Rewire/2");
        request.set(http::field::authorization, "Bearer " + token);
        request.set(http::field::content_type, "application/json");
        request.body() = body;
        request.prepare_payload();

        beast::get_lowest_layer(stream).expires_after(std::chrono::milliseconds(_config.Transport.RequestTimeoutMs));
        http::write(stream, request);

        beast::flat_buffer responseBuffer;
        http::response<http::string_body> response;
        http::read(stream, responseBuffer, response);

        boost::system::error_code shutdownError;
        stream.shutdown(shutdownError);
        if (shutdownError == asio::error::eof || shutdownError == asio::ssl::error::stream_truncated)
            shutdownError.clear();

        unsigned const status = response.result_int();
        if (status == 401)
        {
            std::lock_guard tokenLock(_tokenMutex);
            _cachedAccessToken.clear();
            _accessTokenExpiry = {};
        }

        if (status < 200 || status >= 300)
        {
            std::ostringstream message;
            message << "Firestore commit returned HTTP " << status;
            if (!response.body().empty())
                message << ": " << response.body();
            error = message.str();
            return false;
        }

        error.clear();
        return true;
    }
    catch (std::exception const& exception)
    {
        error = std::string("Firestore HTTPS request failed: ") + exception.what();
        return false;
    }
}

std::string FirestoreTransport::ReadAccessToken(std::string& error)
{
    if (char const* token = std::getenv(_config.Transport.AccessTokenEnvironment.c_str()); token && *token)
    {
        error.clear();
        return token;
    }

    std::lock_guard lock(_tokenMutex);
    auto const now = std::chrono::steady_clock::now();
    if (!_cachedAccessToken.empty() && now < _accessTokenExpiry)
    {
        error.clear();
        return _cachedAccessToken;
    }

    std::uint32_t expiresInSeconds = 0;
    std::string token = ReadMetadataAccessToken(expiresInSeconds, error);
    if (token.empty())
    {
        error = "missing access token environment variable '" + _config.Transport.AccessTokenEnvironment +
            "' and metadata-server acquisition failed: " + error;
        return {};
    }

    std::uint32_t const refreshWindow = expiresInSeconds > 60 ? expiresInSeconds - 60 : expiresInSeconds;
    _cachedAccessToken = std::move(token);
    _accessTokenExpiry = now + std::chrono::seconds(refreshWindow);
    error.clear();
    return _cachedAccessToken;
}

std::string FirestoreTransport::ReadMetadataAccessToken(std::uint32_t& expiresInSeconds, std::string& error) const
{
    namespace asio = boost::asio;
    namespace beast = boost::beast;
    namespace http = beast::http;
    using tcp = asio::ip::tcp;

    try
    {
        asio::io_context ioContext;
        tcp::resolver resolver(ioContext);
        beast::tcp_stream stream(ioContext);

        auto const endpoints = resolver.resolve(_config.Transport.MetadataHost, _config.Transport.MetadataPort);
        stream.expires_after(std::chrono::milliseconds(_config.Transport.RequestTimeoutMs));
        stream.connect(endpoints);

        http::request<http::empty_body> request(http::verb::get,
            "/computeMetadata/v1/instance/service-accounts/default/token", 11);
        request.set(http::field::host, _config.Transport.MetadataHost);
        request.set(http::field::user_agent, "TrinityCore-Rewire/2");
        request.set("Metadata-Flavor", "Google");

        http::write(stream, request);

        beast::flat_buffer responseBuffer;
        http::response<http::string_body> response;
        http::read(stream, responseBuffer, response);

        boost::system::error_code shutdownError;
        stream.socket().shutdown(tcp::socket::shutdown_both, shutdownError);

        unsigned const status = response.result_int();
        if (status < 200 || status >= 300)
        {
            std::ostringstream message;
            message << "metadata token request returned HTTP " << status;
            if (!response.body().empty())
                message << ": " << response.body();
            error = message.str();
            return {};
        }

        rapidjson::Document document;
        document.Parse(response.body().data(), response.body().size());
        if (document.HasParseError() || !document.IsObject())
        {
            std::ostringstream message;
            message << "invalid metadata token JSON";
            if (document.HasParseError())
                message << " at offset " << document.GetErrorOffset() << ": "
                        << rapidjson::GetParseError_En(document.GetParseError());
            error = message.str();
            return {};
        }

        auto token = document.FindMember("access_token");
        auto expires = document.FindMember("expires_in");
        if (token == document.MemberEnd() || !token->value.IsString() ||
            expires == document.MemberEnd() || !expires->value.IsUint())
        {
            error = "metadata token response must contain string access_token and unsigned expires_in";
            return {};
        }

        expiresInSeconds = expires->value.GetUint();
        error.clear();
        return std::string(token->value.GetString(), token->value.GetStringLength());
    }
    catch (std::exception const& exception)
    {
        error = std::string("metadata token request failed: ") + exception.what();
        return {};
    }
}
}
