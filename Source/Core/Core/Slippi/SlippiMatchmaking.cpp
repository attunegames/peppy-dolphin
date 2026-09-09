#include "SlippiMatchmaking.h"
#include "Common/Common.h"
#include "Common/ENetUtil.h"
#include "Common/FileUtil.h"
#include "Common/Logging/Log.h"
#include "Common/StringUtil.h"
#include "Core/NetPlayProto.h"
#include "VideoCommon/OnScreenDisplay.h"
#include <array>
#include <atomic>
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <curl/curl.h>
#include <fstream>
#include <map>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

#include "SlippiRustExtensions.h"

#if defined __linux__ && HAVE_ALSA
#elif defined __APPLE__
#include <arpa/inet.h>
#include <netdb.h>
#elif defined _WIN32
#endif

class MmMessageType
{
  public:
	static std::string CREATE_TICKET;
	static std::string CREATE_TICKET_RESP;
	static std::string GET_TICKET_RESP;
};

// Peppy: set while this client is in a match, so its presence keeps being
// refreshed after the matchmake thread has exited. Declared up here because the
// destructor clears it.
namespace
{
std::atomic<bool> s_heartbeat(false);
void PeppyHeartbeat(); // defined below, next to the rest of the room polling
}

std::string MmMessageType::CREATE_TICKET = "create-ticket";
std::string MmMessageType::CREATE_TICKET_RESP = "create-ticket-resp";
std::string MmMessageType::GET_TICKET_RESP = "get-ticket-resp";

SlippiMatchmaking::SlippiMatchmaking(uintptr_t rs_exi_device_ptr, SlippiUser *user)
{
	m_user = user;
	m_state = ProcessState::IDLE;
	m_errorMsg = "";

	slprs_exi_device_ptr = rs_exi_device_ptr;

	m_client = nullptr;
	m_server = nullptr;

	MM_HOST = scm_slippi_semver_str.find("dev") == std::string::npos ? MM_HOST_PROD : MM_HOST_DEV;

	generator = std::default_random_engine(Common::Timer::GetTimeMs());
}

SlippiMatchmaking::~SlippiMatchmaking()
{
	isMmTerminated = true;
	// The heartbeat deliberately survives this object. It is destroyed when a
	// match ends, and a player sitting at "press start to search" is still in the
	// room - stopping here let them be swept out, which cost them their place in
	// the queue and handed their next opponent to whoever pressed start first.
	m_state = ProcessState::ERROR_ENCOUNTERED;
	m_errorMsg = "Matchmaking shut down";

	if (m_matchmakeThread.joinable())
		m_matchmakeThread.join();

	terminateMmConnection();
}

void SlippiMatchmaking::FindMatch(MatchSearchSettings settings)
{
	isMmConnected = false;

	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Starting matchmaking...");

	m_searchSettings = settings;

	m_errorMsg = "";
	m_state = ProcessState::INITIALIZING;
	m_matchmakeThread = std::thread(&SlippiMatchmaking::MatchmakeThread, this);
}

SlippiMatchmaking::ProcessState SlippiMatchmaking::GetMatchmakeState()
{
	return m_state;
}

std::string SlippiMatchmaking::GetErrorMessage()
{
	return m_errorMsg;
}

bool SlippiMatchmaking::IsSearching()
{
	return searchingStates.count(m_state) != 0;
}

std::unique_ptr<SlippiNetplayClient> SlippiMatchmaking::GetNetplayClient()
{
	return std::move(m_netplayClient);
}

bool SlippiMatchmaking::IsFixedRulesMode(SlippiMatchmaking::OnlinePlayMode mode)
{
	return mode == SlippiMatchmaking::OnlinePlayMode::UNRANKED || mode == SlippiMatchmaking::OnlinePlayMode::RANKED ||
	       mode == SlippiMatchmaking::OnlinePlayMode::PARTY;
}

void SlippiMatchmaking::sendMessage(json msg)
{
	enet_uint32 flags = ENET_PACKET_FLAG_RELIABLE;
	u8 channelId = 0;

	std::string msgContents = msg.dump();

	ENetPacket *epac = enet_packet_create(msgContents.c_str(), msgContents.length(), flags);
	enet_peer_send(m_server, channelId, epac);
}

int SlippiMatchmaking::receiveMessage(json &msg, int timeoutMs)
{
	int hostServiceTimeoutMs = 250;

	// Make sure loop runs at least once
	if (timeoutMs < hostServiceTimeoutMs)
		timeoutMs = hostServiceTimeoutMs;

	// This is not a perfect way to timeout but hopefully it's close enough?
	int maxAttempts = timeoutMs / hostServiceTimeoutMs;

	for (int i = 0; i < maxAttempts; i++)
	{
		ENetEvent netEvent;
		int net = enet_host_service(m_client, &netEvent, hostServiceTimeoutMs);
		if (net <= 0)
			continue;

		switch (netEvent.type)
		{
		case ENET_EVENT_TYPE_RECEIVE:
		{

			std::vector<u8> buf;
			buf.insert(buf.end(), netEvent.packet->data, netEvent.packet->data + netEvent.packet->dataLength);

			std::string str(buf.begin(), buf.end());
			msg = json::parse(str);

			enet_packet_destroy(netEvent.packet);
			return 0;
		}
		case ENET_EVENT_TYPE_DISCONNECT:
			// Return -2 code to indicate we have lost connection to the server
			return -2;
		}
	}

	return -1;
}

void SlippiMatchmaking::MatchmakeThread()
{
	while (IsSearching())
	{
		if (isMmTerminated)
		{
			break;
		}

		switch (m_state)
		{
		case ProcessState::INITIALIZING:
			startMatchmaking();
			break;
		case ProcessState::MATCHMAKING:
			handleMatchmaking();
			break;
		case ProcessState::OPPONENT_CONNECTING:
			handleConnecting();
			break;
		}
	}

	// Clean up ENET connections
	terminateMmConnection();
}

void SlippiMatchmaking::disconnectFromServer()
{
	isMmConnected = false;

	if (m_server)
		enet_peer_disconnect(m_server, 0);
	else
		return;

	ENetEvent netEvent;
	while (enet_host_service(m_client, &netEvent, 3000) > 0)
	{
		switch (netEvent.type)
		{
		case ENET_EVENT_TYPE_RECEIVE:
			enet_packet_destroy(netEvent.packet);
			break;
		case ENET_EVENT_TYPE_DISCONNECT:
			m_server = nullptr;
			return;
		default:
			break;
		}
	}

	// didn't disconnect gracefully force disconnect
	enet_peer_reset(m_server);
	m_server = nullptr;
}

void SlippiMatchmaking::terminateMmConnection()
{
	// Disconnect from server
	disconnectFromServer();

	// Destroy client
	if (m_client)
	{
		enet_host_destroy(m_client);
		m_client = nullptr;
	}
}

// ============================================================================
//                                Peppy matchmaking
// ============================================================================
//
// Slippi's matchmaking server does two jobs: it decides who plays whom, and -
// because the client deliberately talks to it from the netplay port - the
// packet it sends opens the NAT mapping the opponent will later connect to. It
// is a directory AND an accomplice in the hole punch.
//
// Peppy needs the first job to be its own (rooms, a queue, winner stays) and
// gets the second for free from STUN: a binding request sent from that same
// socket opens the identical mapping and reports what the internet saw. Public
// STUN servers are free, so there is no server to run and nothing to pay for.
//
// Symmetric NATs still lose - the mapping differs per destination, so what the
// STUN server saw is not what the peer gets. Slippi has that same limitation
// for the same reason; those players port forward either way.
//
// Nothing below this block changes. The ENet connection, rollback and the
// character select handshake only ever consume the parsed result, so they
// cannot tell the difference.
namespace
{
struct PeppyConfig
{
	bool ok = false;
	std::string url, key, room, name, code;
};

PeppyConfig ReadPeppyConfig()
{
	PeppyConfig cfg;
	std::ifstream file(File::GetUserPath(D_CONFIG_IDX) + "peppy.json");
	if (!file.good())
		return cfg;
	try
	{
		json j;
		file >> j;
		cfg.url = j.value("supabaseUrl", "");
		cfg.key = j.value("supabaseKey", "");
		cfg.room = j.value("roomCode", "");
		cfg.name = j.value("displayName", "");
		cfg.code = j.value("connectCode", "");
		// No room means Peppy is not driving this launch - fall through to the
		// normal Slippi matchmaking rather than half-using ours.
		cfg.ok = !cfg.url.empty() && !cfg.key.empty() && !cfg.room.empty();
	}
	catch (...)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Peppy] peppy.json could not be read");
	}
	return cfg;
}

const PeppyConfig &PeppyCfg()
{
	static PeppyConfig cfg = ReadPeppyConfig();
	return cfg;
}

size_t PeppyCurlWrite(char *ptr, size_t size, size_t nmemb, void *out)
{
	static_cast<std::string *>(out)->append(ptr, size * nmemb);
	return size * nmemb;
}

std::string PeppyPost(const std::string &url, const std::string &body, const std::string &bearer)
{
	CURL *curl = curl_easy_init();
	if (!curl)
		return "";

	std::string out;
	struct curl_slist *headers = nullptr;
	headers = curl_slist_append(headers, ("apikey: " + PeppyCfg().key).c_str());
	headers = curl_slist_append(headers, "Content-Type: application/json");
	if (!bearer.empty())
		headers = curl_slist_append(headers, ("Authorization: Bearer " + bearer).c_str());

	curl_easy_setopt(curl, CURLOPT_URL, url.c_str());
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, body.c_str());
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, headers);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, PeppyCurlWrite);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &out);
	curl_easy_setopt(curl, CURLOPT_TIMEOUT, 10L);

	CURLcode res = curl_easy_perform(curl);
	curl_slist_free_all(headers);
	curl_easy_cleanup(curl);

	if (res != CURLE_OK)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Peppy] HTTP failed: %s", curl_easy_strerror(res));
		return "";
	}
	return out;
}

std::string &PeppyToken()
{
	static std::string token;
	return token;
}

// Dolphin signs in for itself rather than being handed a token by Peppy. Tokens
// expire in about an hour, and refreshing a file underneath a running emulator
// is a worse problem than one extra request.
bool PeppySignIn()
{
	if (!PeppyToken().empty())
		return true;

	std::string resp = PeppyPost(PeppyCfg().url + "/auth/v1/signup", "{}", "");
	try
	{
		json j = json::parse(resp);
		PeppyToken() = j.value("access_token", "");
	}
	catch (...)
	{
	}
	return !PeppyToken().empty();
}

struct PeppyStunServer
{
	const char *host;
	u16 port;
};
const PeppyStunServer PEPPY_STUN[] = {
    {"stun.l.google.com", 19302},
    {"stun1.l.google.com", 19302},
    {"stun.cloudflare.com", 3478},
};

// A STUN binding request and the one attribute we care about. This MUST go out
// of the socket the game will play on - a hole punched for any other socket
// reaches nobody.
bool PeppyStun(ENetSocket sock, std::string &out)
{
	for (const auto &server : PEPPY_STUN)
	{
		ENetAddress addr;
		if (enet_address_set_host(&addr, server.host) < 0)
			continue;
		addr.port = server.port;

		u8 req[20] = {0};
		req[1] = 0x01;                                     // binding request
		req[4] = 0x21; req[5] = 0x12; req[6] = 0xA4; req[7] = 0x42;  // magic cookie
		for (int i = 8; i < 20; i++)
			req[i] = (u8)(rand() & 0xFF);                  // transaction id

		ENetBuffer sendBuf;
		sendBuf.data = req;
		sendBuf.dataLength = sizeof(req);
		if (enet_socket_send(sock, &addr, &sendBuf, 1) <= 0)
			continue;

		u8 resp[512];
		ENetBuffer recvBuf;
		recvBuf.data = resp;
		recvBuf.dataLength = sizeof(resp);

		enet_uint32 cond = ENET_SOCKET_WAIT_RECEIVE;
		if (enet_socket_wait(sock, &cond, 2000) != 0 || !(cond & ENET_SOCKET_WAIT_RECEIVE))
			continue;

		ENetAddress from;
		int len = enet_socket_receive(sock, &from, &recvBuf, 1);
		if (len < 20 || resp[0] != 0x01 || resp[1] != 0x01)
			continue;
		if (memcmp(resp + 8, req + 8, 12) != 0)
			continue;                                      // not our request

		int end = 20 + ((resp[2] << 8) | resp[3]);
		if (end > len)
			end = len;

		for (int off = 20; off + 4 <= end;)
		{
			int type = (resp[off] << 8) | resp[off + 1];
			int alen = (resp[off + 2] << 8) | resp[off + 3];
			const u8 *v = resp + off + 4;

			// XOR-MAPPED-ADDRESS is obfuscated with the cookie so middleboxes
			// cannot helpfully rewrite the IP inside it. MAPPED-ADDRESS is the
			// older plain form.
			if ((type == 0x0020 || type == 0x0001) && alen >= 8 && v[1] == 0x01)
			{
				bool xored = type == 0x0020;
				u16 port = (u16)(((v[2] << 8) | v[3]) ^ (xored ? 0x2112 : 0));
				u8 ip[4];
				for (int i = 0; i < 4; i++)
					ip[i] = v[4 + i] ^ (xored ? req[4 + i] : 0);

				char buf[64];
				sprintf(buf, "%u.%u.%u.%u:%u", ip[0], ip[1], ip[2], ip[3], port);
				out = buf;
				WARN_LOG(SLIPPI_ONLINE, "[Peppy] STUN says we are %s", out.c_str());
				return true;
			}
			off += 4 + alen + ((4 - (alen % 4)) % 4);       // attributes pad to 4
		}
	}

	ERROR_LOG(SLIPPI_ONLINE, "[Peppy] No STUN server answered");
	return false;
}

std::string PeppyIpOf(const std::string &endpoint)
{
	auto colon = endpoint.find(':');
	return colon == std::string::npos ? endpoint : endpoint.substr(0, colon);
}

// The room panel. Drawn by Dolphin over the game rather than by Melee, because a
// native screen means artwork and assembly - this is readable today and costs
// nothing that a native version would have to undo later.
//
// Typed, so each update replaces the last instead of stacking up.
void PeppyShowRoom(const json &resp)
{
	std::stringstream out;
	out << "ROOM " << resp.value("room", "?");

	auto active = resp.find("active");
	if (active != resp.end() && active->is_array() && active->size() >= 2)
	{
		// A pairing exists well before the two players have connected - the room
		// arranges it the moment they are both free. Only "ready" means a game is
		// actually being played.
		out << (resp.value("live", false) ? " - playing: " : " - next up: ") << (*active)[0].value("name", "?")
		    << " vs " << (*active)[1].value("name", "?");
	}
	else
		out << " - no match yet";

	auto queue = resp.find("queue");
	size_t waiting = (queue != resp.end() && queue->is_array()) ? queue->size() : 0;

	if (resp.value("role", "") == "queued")
	{
		int pos = resp.value("position", 0);
		out << "\nqueue: ";
		if (pos <= 1)
			out << "you are next";
		else
			out << "you are #" << pos;
		out << " (" << waiting << (waiting == 1 ? " waiting)" : " waiting)");
	}
	else if (waiting > 0)
	{
		out << "\nqueue: " << waiting << (waiting == 1 ? " waiting" : " waiting");
	}

	OSD::AddTypedMessage(OSD::MessageType::PeppyRoom, out.str(), 4000, OSD::Color::CYAN);
}
} // namespace

// Fallback: arbitrarily choose the last available local IP address listed. They seem to be listed in decreasing order
// IE. 192.168.0.100 > 192.168.0.10 > 10.0.0.2
static char *getLocalAddressFallback()
{
	char host[256];
	int hostname = gethostname(host, sizeof(host)); // find the host name
	if (hostname == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Error finding LAN address");
		return nullptr;
	}

	struct hostent *host_entry = gethostbyname(host); // find host information
	if (host_entry == NULL || host_entry->h_addrtype != AF_INET || host_entry->h_addr_list[0] == 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Error finding LAN host");
		return nullptr;
	}

	// Fetch the last IP (because that was correct for me, not sure if it will be for all)
	for (int i = 0; host_entry->h_addr_list[i] != 0; i++)
		if (host_entry->h_addr_list[i + 1] == 0)
			return host_entry->h_addr_list[i];
	return nullptr;
}

// Set up and connect a socket (UDP, so "connect" doesn't actually send any
// packets) so that the OS will determine what device/local IP address we will
// actually use.
static enet_uint32 getLocalAddress(ENetAddress *mm_address)
{
	ENetSocket socket = enet_socket_create(ENET_SOCKET_TYPE_DATAGRAM);
	if (socket == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket create");
		enet_socket_destroy(socket);
		return 0;
	}

	if (enet_socket_connect(socket, mm_address) == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket connect");
		enet_socket_destroy(socket);
		return 0;
	}

	ENetAddress enetAddress;
	if (enet_socket_get_address(socket, &enetAddress) == -1)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to get local address: socket get address");
		enet_socket_destroy(socket);
		return 0;
	}

	enet_socket_destroy(socket);
	return enetAddress.host;
}

void SlippiMatchmaking::startMatchmaking()
{
	// I don't understand why I have to do this... if I don't do this, rand always returns the
	// same value
	m_client = nullptr;

	int retryCount = 0;

	// If IsFixedRules mode, don't allow ISO's that are known to desync
	if (IsFixedRulesMode(m_searchSettings.mode))
	{
		auto check = slprs_get_iso_md5_check(slprs_exi_device_ptr);
		while (check.result == 0)
		{
			Common::SleepCurrentThread(500);
			retryCount++;
			if (retryCount > 10)
			{
				m_state = ProcessState::ERROR_ENCOUNTERED;
				m_errorMsg = "Could not validate ISO";
				return;
			}
			check = slprs_get_iso_md5_check(slprs_exi_device_ptr);
		}

		if (check.result == 2)
		{
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Cannot queue for this mode with a modded ISO known to desync";
			return;
		}
	}

	retryCount = 0;
	auto userInfo = m_user->GetUserInfo();
	while (m_client == nullptr && retryCount < 15)
	{
		bool customPort = SConfig::GetInstance().m_slippiForceNetplayPort;

		if (customPort)
			m_hostPort = SConfig::GetInstance().m_slippiNetplayPort;
		else if (PeppyCfg().ok)
		{
			// The port must not move between attempts. A fresh one on every retry
			// means both players end up dialling an address the other has already
			// abandoned - and the router drops those packets, because you never
			// sent anything to the port they are now on. Two moving targets that
			// almost never line up: connecting failed over and over on real
			// networks whose NATs were both perfectly friendly.
			//
			// Chosen once per process rather than hardcoded, so two clients on one
			// machine still get different ports.
			static const int peppyPort = 41000 + (int)(Common::Timer::GetTimeMs() % 10000);
			m_hostPort = peppyPort;
		}
		else
			m_hostPort = 41000 + (generator() % 10000);
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Port to use: %d...", m_hostPort);

		// We are explicitly setting the client address because we are trying to utilize our connection
		// to the matchmaking service in order to hole punch. This port will end up being the port
		// we listen on when we start our server
		ENetAddress clientAddr;
		clientAddr.host = ENET_HOST_ANY;
		clientAddr.port = m_hostPort;

		m_client = enet_host_create(&clientAddr, 1, 3, 0, 0);
		retryCount++;
	}

	if (m_client == nullptr)
	{
		// Failed to create client
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to create mm client";
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to create client...");
		return;
	}

	// Peppy runs its own matchmaking. The socket above is bound and that is all
	// we needed from this step - no connection to Slippi's server is made, and
	// the hole gets punched by a STUN request from that same socket once there
	// is actually somebody to play.
	if (PeppyCfg().ok && m_searchSettings.mode == OnlinePlayMode::ROOMS)
	{
		if (!PeppySignIn())
		{
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Could not reach the Peppy server";
			return;
		}

		// Every retry binds a NEW random port, which makes any endpoint we
		// published for the last one a lie - and while it is still inside the
		// freshness window the room would happily pair on it, sending the
		// opponent at a socket that no longer exists. Clear it on the way in so
		// nothing can pair until we have measured the port we are actually on.
		json reset;
		reset["p_room"] = PeppyCfg().room;
		reset["p_name"] = PeppyCfg().name;
		reset["p_code"] = PeppyCfg().code;
		reset["p_reset"] = true;
		PeppyPost(PeppyCfg().url + "/rest/v1/rpc/pd_tick", reset.dump(), PeppyToken());

		if (!s_heartbeat.exchange(true))
			std::thread(PeppyHeartbeat).detach();

		WARN_LOG(SLIPPI_ONLINE, "[Peppy] Joined room '%s' as %s on port %d", PeppyCfg().room.c_str(),
		         PeppyCfg().code.c_str(), m_hostPort);
		m_state = ProcessState::MATCHMAKING;
		return;
	}

	ENetAddress addr;
	enet_address_set_host(&addr, MM_HOST.c_str());
	addr.port = MM_PORT;

	m_server = enet_host_connect(m_client, &addr, 3, 0);

	if (m_server == nullptr)
	{
		// Failed to connect to server
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to start connection to mm server";
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to start connection to mm server...");
		return;
	}

	// Before we can request a ticket, we must wait for connection to be successful
	int connectAttemptCount = 0;
	while (!isMmConnected)
	{
		ENetEvent netEvent;
		int net = enet_host_service(m_client, &netEvent, 500);
		if (net <= 0 || netEvent.type != ENET_EVENT_TYPE_CONNECT)
		{
			// Not yet connected, will retry
			connectAttemptCount++;
			if (connectAttemptCount >= 20)
			{
				ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to connect to mm server...");
				m_state = ProcessState::ERROR_ENCOUNTERED;
				m_errorMsg = "Failed to connect to mm server";
				return;
			}

			continue;
		}

		netEvent.peer->data = &userInfo.displayName;
		m_client->intercept = ENetUtil::InterceptCallback;
		isMmConnected = true;
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connected to mm server...");
	}

	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Trying to find match...");

	/*if (!m_user->IsLoggedIn())
	{
	    ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Must be logged in to queue");
	    m_state = ProcessState::ERROR_ENCOUNTERED;
	    m_errorMsg = "Must be logged in to queue. Go back to menu";
	    return;
	}*/

	// Determine local IP address. We can attempt to connect to our opponent via
	// local IP address if we have the same external IP address. The following
	// scenarios can cause us to have the same external IP address:
	// - we are connected to the same LAN
	// - we are connected to the same VPN node
	// - we are behind the same CGNAT
	char lanAddr[30] = "";
	if (SConfig::GetInstance().m_slippiForceLanIp)
	{
		WARN_LOG(SLIPPI_ONLINE, "[Matchmaking] Overwriting LAN IP sent with configured address");
		sprintf(lanAddr, "%s:%d", SConfig::GetInstance().m_slippiLanIp.c_str(), m_hostPort);
	}
	else
	{
		enet_uint32 localAddress = getLocalAddress(&addr);
		if (localAddress != 0)
		{
			sprintf(lanAddr, "%s:%d", inet_ntoa(*(struct in_addr *)&localAddress), m_hostPort);
		}
		else
		{
			char *fallbackAddress = getLocalAddressFallback();
			if (fallbackAddress != nullptr)
				sprintf(lanAddr, "%s:%d", inet_ntoa(*(struct in_addr *)fallbackAddress), m_hostPort);
		}
	}
	WARN_LOG(SLIPPI_ONLINE, "[Matchmaking] Sending LAN address: %s", lanAddr);

	std::vector<u8> connectCodeBuf;
	connectCodeBuf.insert(connectCodeBuf.end(), m_searchSettings.connectCode.begin(),
	                      m_searchSettings.connectCode.end());

	// Send message to server to create ticket
	json request;
	request["type"] = MmMessageType::CREATE_TICKET;
	request["user"] = {{"uid", userInfo.uid},
	                   {"playKey", userInfo.playKey},
	                   {"connectCode", userInfo.connectCode},
	                   {"displayName", userInfo.displayName}};
	request["search"] = {{"mode", m_searchSettings.mode}, {"connectCode", connectCodeBuf}};
	request["appVersion"] = scm_slippi_semver_str;
	request["ipAddressLan"] = lanAddr;
	sendMessage(request);

	// Get response from server
	json response;
	int rcvRes = receiveMessage(response, 5000);
	if (rcvRes != 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Did not receive response from server for create ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Failed to join mm queue";
		return;
	}

	std::string respType = response["type"];
	if (respType != MmMessageType::CREATE_TICKET_RESP)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received incorrect response for create ticket");
		ERROR_LOG(SLIPPI_ONLINE, "%s", response.dump().c_str());
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Invalid response when joining mm queue";
		return;
	}

	std::string err = response.value("error", "");
	if (err.length() > 0)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received error from server for create ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = err;
		return;
	}

	m_state = ProcessState::MATCHMAKING;
	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Request ticket success");
}

// --------------------------------------------------------- input timeline ---
//
// Turns the packet stream into what a simulation actually needs: for every
// frame, what each player pressed.
//
// Each pad packet leads with a frame number and then carries that frame's inputs
// followed by the unacknowledged backlog behind it - so a packet for frame 4000
// also contains 3999, 3998 and so on. That redundancy is why the stream survives
// packet loss, and it is also what lets a watcher joining late rebuild the whole
// match from the burst of history it is sent.
namespace
{
constexpr int PEPPY_PAD_OFFSET = 14; // mid(1) + frame(4) + playerIdx(1) + checksum(8)
constexpr int PEPPY_PAD_STRIDE = 8;  // SLIPPI_PAD_DATA_SIZE

struct PeppyTimeline
{
	std::mutex m;
	std::map<s32, std::array<std::array<u8, PEPPY_PAD_STRIDE>, 4>> frames;
	std::array<bool, 4> seen{};
	s32 low = 0, high = 0;
	bool any = false;

	void Add(const u8 *d, size_t len)
	{
		if (len < PEPPY_PAD_OFFSET + PEPPY_PAD_STRIDE)
			return;

		s32 frame = (s32)((d[1] << 24) | (d[2] << 16) | (d[3] << 8) | d[4]);
		u8 idx = d[5];
		if (idx >= 4)
			return;

		int pads = (int)((len - PEPPY_PAD_OFFSET) / PEPPY_PAD_STRIDE);

		std::lock_guard<std::mutex> lk(m);
		seen[idx] = true;
		for (int i = 0; i < pads; i++)
		{
			s32 f = frame - i; // the queue runs newest first
			auto &slot = frames[f][idx];
			memcpy(slot.data(), d + PEPPY_PAD_OFFSET + i * PEPPY_PAD_STRIDE, PEPPY_PAD_STRIDE);
		}
		if (!any)
		{
			low = high = frame;
			any = true;
		}
		if (frame < low)
			low = frame;
		if (frame > high)
			high = frame;
	}

	void Reset()
	{
		std::lock_guard<std::mutex> lk(m);
		frames.clear();
		seen.fill(false);
		any = false;
	}

	// How complete is it? A simulation cannot skip a frame, so the only number
	// that matters is whether any frame in range is missing a player.
	std::string Describe()
	{
		std::lock_guard<std::mutex> lk(m);
		if (!any)
			return "no frames yet";

		int players = 0;
		for (bool b : seen)
			if (b)
				players++;

		int missing = 0;
		for (s32 f = low; f <= high; f++)
		{
			auto it = frames.find(f);
			if (it == frames.end())
			{
				missing++;
				continue;
			}
			for (int i = 0; i < 4; i++)
				if (seen[i] && it->second[i] == std::array<u8, PEPPY_PAD_STRIDE>{})
					missing++;
		}

		std::stringstream out;
		out << "frames " << low << ".." << high << " (" << (high - low + 1) << "), " << players << " players, "
		    << missing << " missing";
		return out.str();
	}
};

PeppyTimeline s_timeline;

// What started the match. Two of these arrive, one per player.
struct PeppySelections
{
	std::mutex m;
	bool have[4] = {};
	u8 character[4] = {};
	u8 colour[4] = {};
	u16 stage = 0;
	u32 rngOffset = 0;

	// Laid out by WriteSelectionsToPacket: characterId, characterColor,
	// isCharacterSelected, playerIdx, stageId, isStageSelected, rngOffset...
	void Add(const u8 *d, size_t len)
	{
		if (len < 12)
			return;
		u8 idx = d[4];
		if (idx >= 4)
			return;

		std::lock_guard<std::mutex> lk(m);
		character[idx] = d[1];
		colour[idx] = d[2];
		have[idx] = d[3] != 0;
		u16 st = (u16)((d[5] << 8) | d[6]);
		if (st != 0)
			stage = st;
		if (len >= 12)
			rngOffset = (u32)((d[8] << 24) | (d[9] << 16) | (d[10] << 8) | d[11]);
	}

	void Reset()
	{
		std::lock_guard<std::mutex> lk(m);
		for (int i = 0; i < 4; i++)
			have[i] = false;
		stage = 0;
	}

	int Count()
	{
		std::lock_guard<std::mutex> lk(m);
		int n = 0;
		for (bool b : have)
			if (b)
				n++;
		return n;
	}

	std::string Describe()
	{
		std::lock_guard<std::mutex> lk(m);
		std::stringstream out;
		for (int i = 0; i < 4; i++)
			if (have[i])
				out << "p" << (int)i << "=char" << (int)character[i] << "/c" << (int)colour[i] << " ";
		out << "stage " << stage;
		return out.str();
	}
};

PeppySelections s_selections;
} // namespace

// ------------------------------------------------------------- the watcher ---
//
// Attaches to a match in progress as a read-only peer. It connects, receives the
// same input packets the players send each other, and sends nothing back.
//
// The players never wait on it: on their side spectators live outside m_server,
// which is what every part of the match iterates. This end is deliberately dumb
// - it reports what arrives and whether the stream has holes in it, because that
// is the question worth answering before anything tries to draw a picture.
namespace
{
std::atomic<bool> s_watching(false);

void PeppyWatch(std::string endpoint)
{
	auto colon = endpoint.find(':');
	if (colon == std::string::npos)
	{
		s_watching = false;
		return;
	}
	std::string host = endpoint.substr(0, colon);
	u16 port = (u16)atoi(endpoint.substr(colon + 1).c_str());

	ENetHost *client = enet_host_create(nullptr, 1, 3, 0, 0);
	if (!client)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Peppy] Watcher could not create host");
		s_watching = false;
		return;
	}

	ENetAddress addr;
	enet_address_set_host(&addr, host.c_str());
	addr.port = port;
	ENetPeer *peer = enet_host_connect(client, &addr, 3, 0);
	if (!peer)
	{
		enet_host_destroy(client);
		s_watching = false;
		return;
	}

	s_timeline.Reset();
	s_selections.Reset();
	WARN_LOG(SLIPPI_ONLINE, "[Peppy] Watching %s", endpoint.c_str());

	s32 lastFrame = -1;
	int gaps = 0, packets = 0;
	bool catchingUp = true, gotSelections = false;
	u64 lastReport = Common::Timer::GetTimeMs();

	while (s_watching)
	{
		ENetEvent ev;
		if (enet_host_service(client, &ev, 250) <= 0)
			continue;

		if (ev.type == ENET_EVENT_TYPE_CONNECT)
		{
			// Announce ourselves. The player cannot tell a watcher from a peer by
			// address - several clients on one machine share a host, and players'
			// ports legitimately change - so we say so, and keep saying so until
			// input starts arriving, in case the first one is lost.
			u8 hello = NP_MSG_PEPPY_WATCH;
			ENetPacket *pk = enet_packet_create(&hello, 1, ENET_PACKET_FLAG_RELIABLE);
			enet_peer_send(ev.peer, 0, pk);
			WARN_LOG(SLIPPI_ONLINE, "[Peppy] Watcher connected to %s, announcing", endpoint.c_str());
			continue;
		}
		if (ev.type == ENET_EVENT_TYPE_DISCONNECT)
		{
			WARN_LOG(SLIPPI_ONLINE, "[Peppy] Watcher disconnected");
			break;
		}
		if (ev.type != ENET_EVENT_TYPE_RECEIVE)
		{
			if (packets == 0 && peer->state == ENET_PEER_STATE_CONNECTED)
			{
				u8 hello = NP_MSG_PEPPY_WATCH;
				ENetPacket *pk = enet_packet_create(&hello, 1, ENET_PACKET_FLAG_RELIABLE);
				enet_peer_send(peer, 0, pk);
			}
			continue;
		}

		const u8 *d = ev.packet->data;

		// The selections that started the match - characters, stage, RNG offset.
		// A watcher needs these before it could ever start a game of its own.
		// The player marking the end of the backlog. Everything after this is live.
		if (ev.packet->dataLength >= 1 && d[0] == NP_MSG_PEPPY_WATCH && catchingUp)
		{
			catchingUp = false;
			WARN_LOG(SLIPPI_ONLINE, "[Peppy] Caught up after %d packets at frame %d - now live", packets, lastFrame);
		}

		if (ev.packet->dataLength >= 1 && d[0] == NP_MSG_SLIPPI_MATCH_SELECTIONS)
		{
			s_selections.Add(d, ev.packet->dataLength);
			gotSelections = true;
			WARN_LOG(SLIPPI_ONLINE, "[Peppy] Watcher received match selections (%d bytes)",
			         (int)ev.packet->dataLength);
			WARN_LOG(SLIPPI_ONLINE, "[Peppy] Match so far: %s", s_selections.Describe().c_str());
		}

		// Pad packets lead with the message id, then the frame as a big-endian
		// s32. The frame number answers both questions that matter: is the stream
		// whole, and how far behind live are we.
		if (ev.packet->dataLength >= 5 && d[0] == NP_MSG_SLIPPI_PAD)
		{
			s32 frame = (s32)((d[1] << 24) | (d[2] << 16) | (d[3] << 8) | d[4]);
			packets++;

			// Assemble it into a per-frame timeline. This is the thing a
			// simulation would consume; counting packets only ever told us the
			// pipe was healthy.
			s_timeline.Add(d, ev.packet->dataLength);

			// The catch-up burst arrives far faster than a game is played. Once
			// packets stop outrunning the clock, we are live.
			u64 now = Common::Timer::GetTimeMs();

			if (lastFrame >= 0 && frame > lastFrame + 1)
				gaps++;
			if (frame > lastFrame)
				lastFrame = frame;

			if (now - lastReport > 1000)
			{
				lastReport = now;
				std::stringstream out;
				out << (catchingUp ? "CATCHING UP - frame " : "WATCHING - frame ") << lastFrame << "\n"
				    << packets << " packets, " << gaps << " gaps"
				    << (gotSelections ? ", have match info" : ", NO match info");
				OSD::AddTypedMessage(OSD::MessageType::PeppyWatch, out.str(), 4000, OSD::Color::GREEN);
				WARN_LOG(SLIPPI_ONLINE, "[Peppy] %s frame %d, %d packets, %d gaps, selections %s | timeline: %s",
				         catchingUp ? "Catching up:" : "Watching:", lastFrame, packets, gaps,
				         gotSelections ? "yes" : "no", s_timeline.Describe().c_str());
			}
		}
		enet_packet_destroy(ev.packet);
	}

	enet_peer_disconnect(peer, 0);
	enet_host_destroy(client);
	s_watching = false;
	WARN_LOG(SLIPPI_ONLINE, "[Peppy] Watcher stopped after %d packets, %d gaps", packets, gaps);
}
} // namespace

// Players stop polling the moment they connect - the matchmake thread exits and
// nothing refreshes their presence. The room then sweeps them out mid-match and
// reports no match in progress, which is both wrong on screen and dangerous:
// the pairing their result will settle against has already been closed.
//
// So a heartbeat outlives that thread. It only refreshes presence; the room does
// the rest.
namespace
{
void PeppyHeartbeat()
{
	while (s_heartbeat)
	{
		json body;
		body["p_room"] = PeppyCfg().room;
		body["p_name"] = PeppyCfg().name;
		body["p_code"] = PeppyCfg().code;
		// Presence only. pd_tick also pairs people, and a heartbeat arranging
		// matches is how the loser kept re-pairing with the winner the instant a
		// game ended, skipping whoever was actually next in the queue.
		body["p_presence_only"] = true;
		PeppyPost(PeppyCfg().url + "/rest/v1/rpc/pd_tick", body.dump(), PeppyToken());
		for (int i = 0; i < 20 && s_heartbeat; i++)
			std::this_thread::sleep_for(std::chrono::milliseconds(500));
	}
}
} // namespace

// The matchmake thread has no pacing of its own - the Slippi path is paced by a
// blocking receive. Ours polls HTTP, so it has to wait deliberately.
void SlippiMatchmaking::peppySleep()
{
	std::this_thread::sleep_for(std::chrono::milliseconds(1500));
}

// One turn of the room loop. Answers are "waiting", "stun" or "ready":
//
//   waiting  nobody else is here. Send nothing over UDP - a hole punched now
//            would just be closed again by the time anyone arrived.
//   stun     someone to play. Measure the mapping and publish it. Both clients
//            are told this at the same moment, so both mappings are seconds old
//            when the dial happens.
//   ready    both endpoints are fresh. Hand Slippi the addresses and get out of
//            the way.
void SlippiMatchmaking::handlePeppyMatchmaking()
{
	const PeppyConfig &cfg = PeppyCfg();

	json body;
	body["p_room"] = cfg.room;
	body["p_name"] = cfg.name;
	body["p_code"] = cfg.code;

	std::string raw = PeppyPost(cfg.url + "/rest/v1/rpc/pd_tick", body.dump(), PeppyToken());
	json resp;
	try
	{
		resp = json::parse(raw);
	}
	catch (...)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Peppy] Bad response from room: %s", raw.c_str());
		peppySleep();
		return;
	}

	PeppyShowRoom(resp);

	std::string state = resp.value("state", "");

	if (state == "waiting")
	{
		// Queued behind a match in progress: attach to it as a read-only peer.
		// Prefer the LAN address when we share a public IP with them, exactly as
		// the players do with each other.
		auto watch = resp.find("watch");
		if (!s_watching && watch != resp.end() && watch->is_array() && watch->size() > 0)
		{
			std::string target = (*watch)[0].value("lan", "");
			if (target.empty())
				target = (*watch)[0].value("external", "");
			if (!target.empty())
			{
				s_watching = true;
				std::thread(PeppyWatch, target).detach();
			}
		}
		peppySleep();
		return;
	}

	if (state == "stun")
	{
		std::string external;
		if (!PeppyStun(m_client->socket, external))
		{
			peppySleep();
			return;
		}

		// The LAN address lets two players behind one router skip the internet
		// entirely, exactly as Slippi does it.
		char lanAddr[64] = "";
		ENetAddress probe;
		if (enet_address_set_host(&probe, PEPPY_STUN[0].host) == 0)
		{
			probe.port = PEPPY_STUN[0].port;
			enet_uint32 local = getLocalAddress(&probe);
			if (local != 0)
				sprintf(lanAddr, "%s:%d", inet_ntoa(*(struct in_addr *)&local), m_hostPort);
		}

		body["p_external"] = external;
		body["p_lan"] = std::string(lanAddr);
		PeppyPost(cfg.url + "/rest/v1/rpc/pd_tick", body.dump(), PeppyToken());
		peppySleep();
		return;
	}

	if (state != "ready")
	{
		if (state == "error")
			ERROR_LOG(SLIPPI_ONLINE, "[Peppy] Room error: %s", resp.value("error", "").c_str());
		peppySleep();
		return;
	}

	// ---- paired ------------------------------------------------------------

	m_isSwapAttempt = false;
	m_netplayClient = nullptr;
	m_remoteIps.clear();
	m_playerInfo.clear();

	m_isHost = resp.value("isHost", false);
	json me = resp["me"], opp = resp["opponent"];

	// The room assigns ports, so who is P1 is decided by who held the setup
	// rather than by anything in Melee.
	SlippiUser::UserInfo mine, theirs;
	mine.displayName = me.value("name", "");
	mine.connectCode = me.value("code", "");
	mine.uid = mine.connectCode;
	mine.port = m_isHost ? 1 : 2;
	mine.chatMessages = m_user->GetDefaultChatMessages();

	theirs.displayName = opp.value("name", "");
	theirs.connectCode = opp.value("code", "");
	theirs.uid = theirs.connectCode;
	theirs.port = m_isHost ? 2 : 1;
	theirs.chatMessages = m_user->GetDefaultChatMessages();

	if (m_isHost)
	{
		m_playerInfo.push_back(mine);
		m_playerInfo.push_back(theirs);
	}
	else
	{
		m_playerInfo.push_back(theirs);
		m_playerInfo.push_back(mine);
	}
	m_localPlayerIndex = mine.port - 1;

	std::string myExternal = me.value("external", "");
	std::string oppExternal = opp.value("external", "");
	std::string oppLan = opp.value("lan", "");

	if (!oppLan.empty() && PeppyIpOf(myExternal) == PeppyIpOf(oppExternal))
		m_remoteIps.push_back(oppLan);
	else
		m_remoteIps.push_back(oppExternal);

	m_allowedStages.clear();
	m_allowedStages.push_back(0x3);  // Pokemon Stadium
	m_allowedStages.push_back(0x8);  // Yoshi's Story
	m_allowedStages.push_back(0x1C); // Dream Land
	m_allowedStages.push_back(0x1F); // Battlefield
	m_allowedStages.push_back(0x20); // Final Destination
	m_allowedStages.push_back(0x2);  // Fountain of Dreams

	m_mmResult.id = resp.value("matchId", "");
	m_mmResult.players = m_playerInfo;
	m_mmResult.stages = m_allowedStages;
	m_mmResult.items = 0;

	WARN_LOG(SLIPPI_ONLINE, "[Peppy] Matched with %s at %s (isHost: %s)", theirs.displayName.c_str(),
	         m_remoteIps[0].c_str(), m_isHost ? "true" : "false");

	// Frees the port so the netplay client can bind it. The NAT mapping we just
	// opened survives, because routers key them on the port, not the socket.
	terminateMmConnection();

	if (!s_heartbeat.exchange(true))
		std::thread(PeppyHeartbeat).detach();

	m_state = ProcessState::OPPONENT_CONNECTING;
}

// ------------------------------------------------------- result + rotation ---

// Is anyone actually waiting to play? Asked while still paired, so the two
// people in the match do not count themselves - if the queue is empty, they are
// left alone to keep playing each other, which is the whole point of only
// interrupting a set when somebody is there to take the loser's place.
bool SlippiMatchmaking::PeppyShouldRotate()
{
	if (!PeppyCfg().ok)
		return false;

	json body;
	body["p_room"] = PeppyCfg().room;
	body["p_name"] = PeppyCfg().name;
	body["p_code"] = PeppyCfg().code;

	std::string raw = PeppyPost(PeppyCfg().url + "/rest/v1/rpc/pd_tick", body.dump(), PeppyToken());
	try
	{
		json resp = json::parse(raw);
		auto queue = resp.find("queue");
		bool waiting = queue != resp.end() && queue->is_array() && queue->size() > 0;
		WARN_LOG(SLIPPI_ONLINE, "[Peppy] %d waiting to play", waiting ? (int)queue->size() : 0);
		return waiting;
	}
	catch (...)
	{
		return false;
	}
}

// Melee tells us who won at the end of every game, so this is a report, not a
// guess. Both clients call it with the same match id and opposite answers; the
// room takes the first and ignores the second.
void SlippiMatchmaking::PeppyReportResult(const std::string &matchId, bool iWon)
{
	if (!PeppyCfg().ok || matchId.empty())
		return;

	std::thread([matchId, iWon]() {
		json body;
		body["p_room"] = PeppyCfg().room;
		body["p_match_id"] = matchId;
		body["p_i_won"] = iWon;
		PeppyPost(PeppyCfg().url + "/rest/v1/rpc/pd_result", body.dump(), PeppyToken());
		WARN_LOG(SLIPPI_ONLINE, "[Peppy] Reported %s for %s", iWon ? "a win" : "a loss", matchId.c_str());
	}).detach();
}

void SlippiMatchmaking::handleMatchmaking()
{
	// Deal with class shut down
	if (m_state != ProcessState::MATCHMAKING)
		return;

	if (PeppyCfg().ok && m_searchSettings.mode == OnlinePlayMode::ROOMS)
	{
		handlePeppyMatchmaking();
		return;
	}

	// Get response from server
	json getResp;
	int rcvRes = receiveMessage(getResp, 2000);
	if (rcvRes == -1)
	{
		INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] Have not yet received assignment");
		return;
	}
	else if (rcvRes != 0)
	{
		// Right now the only other code is -2 meaning the server died probably?
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Lost connection to the mm server");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Lost connection to the mm server";
		return;
	}

	std::string respType = getResp["type"];
	if (respType != MmMessageType::GET_TICKET_RESP)
	{
		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received incorrect response for get ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = "Invalid response when getting mm status";
		return;
	}

	std::string err = getResp.value("error", "");
	std::string latestVersion = getResp.value("latestVersion", "");
	if (err.length() > 0)
	{
		if (latestVersion != "")
		{
			// Update version number when the mm server tells us our version is outdated
			m_user->OverwriteLatestVersion(
			    latestVersion); // Force latest version for people whose file updates dont work
		}

		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Received error from server for get ticket");
		m_state = ProcessState::ERROR_ENCOUNTERED;
		m_errorMsg = err;
		return;
	}

	m_isSwapAttempt = false;
	m_netplayClient = nullptr;

	// Clear old users
	m_remoteIps.clear();
	m_playerInfo.clear();

	std::string matchId = getResp.value("matchId", "");
	WARN_LOG(SLIPPI_ONLINE, "Match ID: %s", matchId.c_str());

	auto queue = getResp["players"];
	if (queue.is_array())
	{
		std::string localExternalIp = "";

		for (json::iterator it = queue.begin(); it != queue.end(); ++it)
		{
			json el = *it;
			SlippiUser::UserInfo playerInfo;

			bool isLocal = el.value("isLocalPlayer", false);
			playerInfo.uid = el.value("uid", "");
			playerInfo.displayName = el.value("displayName", "");
			playerInfo.connectCode = el.value("connectCode", "");
			playerInfo.port = el.value("port", 0);
			playerInfo.isBot = el.value("isBot", false);

			if (el["chatMessages"].is_array())
			{
				playerInfo.chatMessages = el.value("chatMessages", m_user->GetDefaultChatMessages());

				if (playerInfo.chatMessages.size() != 16)
				{
					playerInfo.chatMessages = m_user->GetDefaultChatMessages();
				}
			}
			else
			{
				playerInfo.chatMessages = m_user->GetDefaultChatMessages();
			}

			json rankEl = el["rank"];
			if (rankEl.is_object())
			{
				playerInfo.rankedRating = rankEl.value("rating", 0.0f);
				playerInfo.rankedUpdateCount = rankEl.value("updateCount", 0);
				playerInfo.rankedGlobalPlacement = rankEl.value("globalPlacement", 0);
				playerInfo.rankedRegionalPlacement = rankEl.value("regionalPlacement", 0);
			}

			m_playerInfo.push_back(playerInfo);

			if (isLocal)
			{
				std::vector<std::string> localIpParts;
				SplitString(el.value("ipAddress", "1.1.1.1:123"), ':', localIpParts);
				localExternalIp = localIpParts[0];
				m_localPlayerIndex = playerInfo.port - 1;
			}
		};

		// Loop a second time to get the correct remote IPs
		for (json::iterator it = queue.begin(); it != queue.end(); ++it)
		{
			json el = *it;

			if (el.value("port", 0) - 1 == m_localPlayerIndex)
				continue;

			auto extIp = el.value("ipAddress", "1.1.1.1:123");
			std::vector<std::string> exIpParts;
			SplitString(extIp, ':', exIpParts);

			auto lanIp = el.value("ipAddressLan", "1.1.1.1:123");

			WARN_LOG(SLIPPI_ONLINE, "LAN IP: %s", lanIp.c_str());

			if (exIpParts[0] != localExternalIp || lanIp.empty())
			{
				// If external IPs are different, just use that address
				m_remoteIps.push_back(extIp);
				continue;
			}

			// TODO: Instead of using one or the other, it might be better to try both

			// If external IPs are the same, try using LAN IPs
			m_remoteIps.push_back(lanIp);
		}
	}
	m_isHost = getResp.value("isHost", false);

	// Get allowed stages. For stage select modes like direct and teams, this will only impact the first map selected
	m_allowedStages.clear();
	auto stages = getResp["stages"];
	if (stages.is_array())
	{
		for (json::iterator it = stages.begin(); it != stages.end(); ++it)
		{
			json el = *it;
			auto stageId = el.get<int>();
			m_allowedStages.push_back(stageId);
		}
	}

	if (m_allowedStages.empty())
	{
		// Default case, shouldn't ever really be hit but it's here just in case
		m_allowedStages.push_back(0x3);  // Pokemon
		m_allowedStages.push_back(0x8);  // Yoshi's Story
		m_allowedStages.push_back(0x1C); // Dream Land
		m_allowedStages.push_back(0x1F); // Battlefield
		m_allowedStages.push_back(0x20); // Final Destination

		// Add FoD if singles
		if (m_playerInfo.size() == 2)
		{
			m_allowedStages.push_back(0x2); // FoD
		}
	}

	m_mmResult.id = matchId;
	m_mmResult.players = m_playerInfo;
	m_mmResult.stages = m_allowedStages;
	m_mmResult.items = getResp.value<u32>("items", 0);

	// Disconnect and destroy enet client to mm server
	terminateMmConnection();

	// If ranked, report to backend that we are attempting to connect to this match
	if (matchId.find("mode.ranked") != std::string::npos)
	{
		slprs_exi_device_report_match_status(slprs_exi_device_ptr, matchId.c_str(), "connecting", true);
	}

	m_state = ProcessState::OPPONENT_CONNECTING;
	ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Opponent found. isDecider: %s", m_isHost ? "true" : "false");
}

int SlippiMatchmaking::LocalPlayerIndex()
{
	return m_localPlayerIndex;
}

std::vector<SlippiUser::UserInfo> SlippiMatchmaking::GetPlayerInfo()
{
	return m_playerInfo;
}

std::vector<u16> SlippiMatchmaking::GetStages()
{
	return m_allowedStages;
}

// This is kind of duplicate code from what exists in rust. Maybe eventually it should be remove
// and exist only on the rust side
SlippiMatchmaking::SlippiRank SlippiMatchmaking::GetPlayerRank(u8 port)
{
	if (port >= m_playerInfo.size())
	{
		return SlippiRank::Unranked;
	}

	auto info = m_playerInfo[port];

	float rating = info.rankedRating;
	int updateCount = info.rankedUpdateCount;
	int global = info.rankedGlobalPlacement;
	int regional = info.rankedRegionalPlacement;

	if (updateCount < 5)
	{
		return SlippiRank::Unranked;
	}

	if (rating <= 765.42f)
	{
		return SlippiRank::Bronze1;
	}

	if (rating > 765.43f && rating <= 913.71f)
	{
		return SlippiRank::Bronze2;
	}

	if (rating > 913.72f && rating <= 1054.86f)
	{
		return SlippiRank::Bronze3;
	}

	if (rating > 1054.87f && rating <= 1188.87f)
	{
		return SlippiRank::Silver1;
	}

	if (rating > 1188.88f && rating <= 1315.74f)
	{
		return SlippiRank::Silver2;
	}

	if (rating > 1315.75f && rating <= 1435.47f)
	{
		return SlippiRank::Silver3;
	}

	if (rating > 1435.48f && rating <= 1548.06f)
	{
		return SlippiRank::Gold1;
	}

	if (rating > 1548.07f && rating <= 1653.51f)
	{
		return SlippiRank::Gold2;
	}

	if (rating > 1653.52f && rating <= 1751.82f)
	{
		return SlippiRank::Gold3;
	}

	if (rating > 1751.83f && rating <= 1842.99f)
	{
		return SlippiRank::Platinum1;
	}

	if (rating > 1843.0f && rating <= 1927.02f)
	{
		return SlippiRank::Platinum2;
	}

	if (rating > 1927.03f && rating <= 2003.91f)
	{
		return SlippiRank::Platinum3;
	}

	if (rating > 2003.92f && rating <= 2073.66f)
	{
		return SlippiRank::Diamond1;
	}

	if (rating > 2073.67f && rating <= 2136.27f)
	{
		return SlippiRank::Diamond2;
	}

	if (rating > 2136.28f && rating <= 2191.74f)
	{
		return SlippiRank::Diamond3;
	}

	if (rating >= 2191.75f && (global > 0 || regional > 0))
	{
		return SlippiRank::Grandmaster;
	}

	if (rating > 2191.75f && rating <= 2274.99f)
	{
		return SlippiRank::Master1;
	}

	if (rating > 2275.0f && rating <= 2350.0f)
	{
		return SlippiRank::Master2;
	}

	if (rating > 2350.0f)
	{
		return SlippiRank::Master3;
	}

	return SlippiRank::Unranked;
}

SlippiMatchmaking::MatchmakeResult SlippiMatchmaking::GetMatchmakeResult()
{
	return m_mmResult;
}

std::string SlippiMatchmaking::GetPlayerName(u8 port)
{
	if (port >= m_playerInfo.size())
	{
		return "";
	}
	return m_playerInfo[port].displayName;
}

u8 SlippiMatchmaking::RemotePlayerCount()
{
	if (m_playerInfo.size() == 0)
		return 0;

	return (u8)m_playerInfo.size() - 1;
}

void SlippiMatchmaking::handleConnecting()
{
	auto userInfo = m_user->GetUserInfo();

	m_isSwapAttempt = false;
	m_netplayClient = nullptr;

	u8 remotePlayerCount = (u8)m_remoteIps.size();
	std::vector<std::string> remoteParts;
	std::vector<std::string> addrs;
	std::vector<u16> ports;
	for (int i = 0; i < m_remoteIps.size(); i++)
	{
		remoteParts.clear();
		SplitString(m_remoteIps[i], ':', remoteParts);
		addrs.push_back(remoteParts[0]);
		ports.push_back(std::stoi(remoteParts[1]));
	}

	std::stringstream ipLog;
	ipLog << "Remote player IPs: ";
	for (int i = 0; i < m_remoteIps.size(); i++)
	{
		ipLog << m_remoteIps[i] << ", ";
	}
	// INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] My port: %d || %s", m_hostPort, ipLog.str());

	// Is host is now used to specify who the decider is
	auto client = std::make_unique<SlippiNetplayClient>(addrs, ports, remotePlayerCount, m_hostPort, m_isHost,
	                                                    m_localPlayerIndex);

	while (!m_netplayClient)
	{
		auto status = client->GetSlippiConnectStatus();
		if (status == SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_INITIATED)
		{
			INFO_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection not yet successful");
			Common::SleepCurrentThread(500);

			// Deal with class shut down
			if (m_state != ProcessState::OPPONENT_CONNECTING)
				return;

			continue;
		}
		else if (status == SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_FAILED &&
		         m_searchSettings.mode == SlippiMatchmaking::OnlinePlayMode::TEAMS)
		{
			// If we failed setting up a connection in teams mode, show a detailed error about who we had issues
			// connecting to.
			ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Failed to connect to players");
			m_state = ProcessState::ERROR_ENCOUNTERED;
			m_errorMsg = "Timed out waiting for other players to connect";
			auto failedConns = client->GetFailedConnections();
			if (!failedConns.empty())
			{
				std::stringstream err;
				err << "Could not connect to players: ";
				for (int i = 0; i < failedConns.size(); i++)
				{
					int p = failedConns[i];
					if (p >= m_localPlayerIndex)
						p++;

					err << m_playerInfo[p].displayName;
					if (i < failedConns.size() - 1)
					{
						err << ", ";
					}
				}
				m_errorMsg = err.str();
			}

			return;
		}
		else if (status != SlippiNetplayClient::SlippiConnectStatus::NET_CONNECT_STATUS_CONNECTED)
		{
			ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection attempt failed, looking for someone else.");

			// Return to the start to get a new ticket to find someone else we can hopefully connect with
			m_netplayClient = nullptr;
			m_state = ProcessState::INITIALIZING;
			return;
		}

		ERROR_LOG(SLIPPI_ONLINE, "[Matchmaking] Connection success!");

		// Successful connection
		m_netplayClient = std::move(client);
	}

	// Connection success, our work is done
	m_state = ProcessState::CONNECTION_SUCCESS;
}
