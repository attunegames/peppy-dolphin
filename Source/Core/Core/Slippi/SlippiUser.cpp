#include "Common/Common.h"
#include "Common/FileUtil.h"
#include "Common/Logging/Log.h"

#include "SlippiUser.h"

#include "SlippiRustExtensions.h"

#include <fstream>
#include <json.hpp>
#include <mutex>

using json = nlohmann::json;

// ---------------------------------------------------------- Peppy identity ---
//
// Peppy runs its own matchmaking, so a slippi.gg account buys us nothing: there
// is no server of theirs to authenticate against. Players pick a name in Peppy
// on first launch and that is who they are.
//
// If Peppy has written User/Config/peppy.json, it replaces the login entirely.
// user.json is never opened and the play key inside it is never read - a
// constraint Peppy has been working around since the beginning, resolved here
// by simply not needing the file.
//
// Falls through to the normal Slippi login when the file is absent, so an
// unconfigured build still behaves like upstream.
namespace
{
std::once_flag s_peppy_once;
bool s_peppy_present = false;
SlippiUser::UserInfo s_peppy_user;

void LoadPeppyIdentity()
{
	std::call_once(s_peppy_once, []() {
		std::string path = File::GetUserPath(D_CONFIG_IDX) + "peppy.json";
		std::ifstream file(path);
		if (!file.good())
			return;

		try
		{
			json j;
			file >> j;
			s_peppy_user.displayName = j.value("displayName", "");
			s_peppy_user.connectCode = j.value("connectCode", "");
			s_peppy_user.uid = j.value("uid", "");
			s_peppy_user.playKey = "";

			// prepareOnlineStatus() semver-compares this against the running build and
			// tells Melee an update is required if it is newer. There is no update
			// service here, so report a version that can never win that comparison.
			s_peppy_user.latestVersion = "0.0.0";

			s_peppy_present = !s_peppy_user.displayName.empty() && !s_peppy_user.connectCode.empty();
			if (s_peppy_present)
			{
				WARN_LOG(SLIPPI_ONLINE, "[Peppy] Identity: %s (%s)", s_peppy_user.displayName.c_str(),
				         s_peppy_user.connectCode.c_str());
			}
		}
		catch (...)
		{
			// A malformed file must not take online mode down with it - fall back
			// to the normal login path.
			ERROR_LOG(SLIPPI_ONLINE, "[Peppy] Could not read %s, ignoring it", path.c_str());
		}
	});
}
} // namespace

// Takes a RustChatMessages pointer and extracts messages from them, then
// frees the underlying memory safely.
std::vector<std::string> ConvertChatMessagesFromRust(RustChatMessages *rsMessages)
{
	std::vector<std::string> chatMessages;

	for (int i = 0; i < rsMessages->len; i++)
	{
		std::string message = std::string(rsMessages->data[i]);
		chatMessages.push_back(message);
	}

	slprs_user_free_messages(rsMessages);

	return chatMessages;
}

SlippiUser::SlippiUser(uintptr_t rs_exi_device_ptr)
{
	slprs_exi_device_ptr = rs_exi_device_ptr;
}

SlippiUser::~SlippiUser() {}

bool SlippiUser::AttemptLogin()
{
	// Already "logged in" as whoever Peppy says we are - never send the player
	// off to slippi.gg to make an account they do not need.
	LoadPeppyIdentity();
	if (s_peppy_present)
		return true;

	return slprs_user_attempt_login(slprs_exi_device_ptr);
}

void SlippiUser::OpenLogInPage()
{
	slprs_user_open_login_page(slprs_exi_device_ptr);
}

void SlippiUser::ListenForLogIn()
{
	slprs_user_listen_for_login(slprs_exi_device_ptr);
}

bool SlippiUser::UpdateApp()
{
	return slprs_user_update_app(slprs_exi_device_ptr);
}

void SlippiUser::LogOut()
{
	slprs_user_logout(slprs_exi_device_ptr);
}

void SlippiUser::OverwriteLatestVersion(std::string version)
{
	slprs_user_overwrite_latest_version(slprs_exi_device_ptr, version.c_str());
}

SlippiUser::UserInfo SlippiUser::GetUserInfo()
{
	LoadPeppyIdentity();
	if (s_peppy_present)
		return s_peppy_user;

	SlippiUser::UserInfo userInfo;

	RustUserInfo *info = slprs_user_get_info(slprs_exi_device_ptr);
	userInfo.uid = std::string(info->uid);
	userInfo.playKey = std::string(info->play_key);
	userInfo.displayName = std::string(info->display_name);
	userInfo.connectCode = std::string(info->connect_code);
	userInfo.latestVersion = std::string(info->latest_version);
	slprs_user_free_info(info);

	return userInfo;
}

std::vector<std::string> SlippiUser::GetDefaultChatMessages()
{
	RustChatMessages *chatMessages = slprs_user_get_default_messages(slprs_exi_device_ptr);
	return ConvertChatMessagesFromRust(chatMessages);
}

std::vector<std::string> SlippiUser::GetUserChatMessages()
{
	RustChatMessages *chatMessages = slprs_user_get_messages(slprs_exi_device_ptr);
	return ConvertChatMessagesFromRust(chatMessages);
}

bool SlippiUser::IsLoggedIn()
{
	LoadPeppyIdentity();
	if (s_peppy_present)
		return true;

	return slprs_user_get_is_logged_in(slprs_exi_device_ptr);
}
