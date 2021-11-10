#ifndef mr_protocol_h_
#define mr_protocol_h_

#include <string>
#include <vector>

#include "rpc.h"

using namespace std;

#define REDUCER_COUNT 4

enum mr_tasktype {
	NONE = 0, // this flag means no task needs to be performed at this point
	MAP,
	REDUCE
};

class mr_protocol {
public:
	typedef int status;
	enum xxstatus { OK, RPCERR, NOENT, IOERR };
	enum rpc_numbers {
		asktask = 0xa001,
		submittask,
	};

	struct AskTaskResponse {
		mr_tasktype taskType;
		int index;
		string filename;
	};

	struct AskTaskRequest {

	};

	struct SubmitTaskResponse {

	};

	struct SubmitTaskRequest {
		int taskType;
		int index;
	};

};

inline unmarshall& operator>>(unmarshall& u, mr_protocol::AskTaskResponse& res)
{
	int type = 0;
	u >> type;
	res.taskType = mr_tasktype(type);
	u >> res.index;
	u >> res.filename;
	return u;
}

inline marshall& operator<<(marshall& m, mr_protocol::AskTaskResponse res)
{
	m << int(res.taskType);
	m << res.index;
	m << res.filename;
	return m;
}

#endif

