#pragma once

#include "Common/CommonTypes.h"
#include "Common/Thread.h"
#include "Core/Slippi/SlippiNetplay.h"
#include "Core/Slippi/SlippiUser.h"

#ifndef _WIN32
#include <netdb.h>
#include <arpa/inet.h>
#endif

#include <unordered_map>
#include <vector>
#include <json.hpp>

using json = nlohmann::json;

class SlippiMatchmaking
{
  public:
	SlippiMatchmaking(uintptr_t rs_exi_device_ptr, SlippiUser *user);
	~SlippiMatchmaking();

	enum OnlinePlayMode
	{
		RANKED = 0,
		UNRANKED = 1,
		DIRECT = 2,
		TEAMS = 3,
		PARTY = 4,
		// Peppy: added, not replacing. Melee's online submenu has a ninth option
		// that sets this mode, and it is the only one we intercept.
		ROOMS = 5,
	};

	enum ProcessState
	{
		IDLE,
		INITIALIZING,
		MATCHMAKING,
		OPPONENT_CONNECTING,
		CONNECTION_SUCCESS,
		ERROR_ENCOUNTERED,
	};

	enum SlippiRank
	{
		Unranked,
		Bronze1,
		Bronze2,
		Bronze3,
		Silver1,
		Silver2,
		Silver3,
		Gold1,
		Gold2,
		Gold3,
		Platinum1,
		Platinum2,
		Platinum3,
		Diamond1,
		Diamond2,
		Diamond3,
		Master1,
		Master2,
		Master3,
		Grandmaster
	};

	struct MatchSearchSettings
	{
		OnlinePlayMode mode = OnlinePlayMode::RANKED;
		std::string connectCode = "";
	};

	struct MatchmakeResult
	{
		std::string id = "";
		std::vector<SlippiUser::UserInfo> players;
		std::vector<u16> stages;
		u32 items = 0; // uninitialised, this is whatever was on the stack - and items turn on
	};

	void FindMatch(MatchSearchSettings settings);

	// Peppy: report the winner of a finished game to the room, and answer
	// whether the room wants these two to break up so somebody waiting can play.
	// Runs on its own thread - never block the EXI path on the network.
	void PeppyReportResult(const std::string &matchId, bool iWon);
	bool PeppyShouldRotate();

	// Peppy watch mode. A spectator holds the whole match - what to start and
	// what both players pressed on every frame - and the EXI device reads it
	// through these rather than from a live opponent.
	static bool PeppyWatchActive();
	static bool PeppyWatchReady();            // both players' selections in hand
	static u8 PeppyWatchCharacter(u8 idx);
	static u8 PeppyWatchColour(u8 idx);
	static u16 PeppyWatchStage();
	static u32 PeppyWatchRngOffset(); // the host's, the one the match ran with
	static bool PeppyWatchRestartPending(); // the players have started a new game
	static void PeppyWatchClearRestart();
	static s32 PeppyWatchLatestFrame();
	// The frame the watcher's game is currently on. Melee drives this - it tells
	// us which frame it is asking about - so the cursor never has to guess at
	// pacing, which is what makes this tractable at all.
	// Melee polls for match state at the character select and for inputs during
	// a game. Either means we are still in online mode; neither means the player
	// has backed out, and the room should stop being told they are here.
	static void PeppyStillOnline();

	static s32 PeppyWatchFrame();
	static void PeppyWatchSetFrame(s32 frame);
	// Fills SLIPPI_PAD_FULL_SIZE bytes; false when that frame is not held.
	static bool PeppyWatchPad(s32 frame, u8 idx, u8 *out);
	void MatchmakeThread();
	ProcessState GetMatchmakeState();
	bool IsSearching();
	std::unique_ptr<SlippiNetplayClient> GetNetplayClient();
	std::string GetErrorMessage();
	int LocalPlayerIndex();
	std::vector<SlippiUser::UserInfo> GetPlayerInfo();
	std::string GetPlayerName(u8 port);
	SlippiRank GetPlayerRank(u8 port);
	std::vector<u16> GetStages();
	u8 RemotePlayerCount();
	MatchmakeResult GetMatchmakeResult();
	static bool IsFixedRulesMode(OnlinePlayMode mode);

  protected:
	const std::string MM_HOST_DEV = "mm2.slippi.gg";
	const std::string MM_HOST_PROD = "mm.slippi.gg";
	const u16 MM_PORT = 43113;

	std::string MM_HOST = "";

	// Peppy: our own matchmaking, in place of the connection to mm.slippi.gg.
	// See the Peppy block in SlippiMatchmaking.cpp for how it works.
	void handlePeppyMatchmaking();
	void peppySleep();

	ENetHost *m_client;
	ENetPeer *m_server;

	std::default_random_engine generator;

	bool isMmConnected = false;
	bool isMmTerminated = false;

	std::thread m_matchmakeThread;

	MatchSearchSettings m_searchSettings;

	ProcessState m_state;
	std::string m_errorMsg = "";

	SlippiUser *m_user;

	int m_isSwapAttempt = false;

	int m_hostPort;
	int m_localPlayerIndex;
	std::vector<std::string> m_remoteIps;
	MatchmakeResult m_mmResult;
	std::vector<SlippiUser::UserInfo> m_playerInfo;
	std::vector<u16> m_allowedStages;
	bool m_joinedLobby;
	bool m_isHost;

	std::unique_ptr<SlippiNetplayClient> m_netplayClient;

	// A pointer to a "shadow" EXI Device that lives on the Rust side of things.
	// Do *not* do any cleanup of this! The EXI device will handle it.
	uintptr_t slprs_exi_device_ptr;

	const std::unordered_map<ProcessState, bool> searchingStates = {
	    {ProcessState::INITIALIZING, true},
	    {ProcessState::MATCHMAKING, true},
	    {ProcessState::OPPONENT_CONNECTING, true},
	};

	void disconnectFromServer();
	void terminateMmConnection();
	void sendMessage(json msg);
	int receiveMessage(json &msg, int maxAttempts);

	void sendHolePunchMsg(std::string remoteIp, u16 remotePort, u16 localPort);

	void startMatchmaking();
	void handleMatchmaking();
	void handleConnecting();
};
