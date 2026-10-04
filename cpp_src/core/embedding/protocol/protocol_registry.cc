#include "iembed_protocol.h"

#include "openai_protocol.h"
#include "rx_protocol.h"
#include "tools/assertrx.h"

namespace reindexer::embedding {

const IEmbedProtocol& GetEmbedProtocol(EmbedderConfig::Protocol protocol) {
	static const RxEmbedProtocol kRx;
	static const OpenAIEmbedProtocol kOpenAI;
	switch (protocol) {
		case EmbedderConfig::Protocol::Rx:
			return kRx;
		case EmbedderConfig::Protocol::OpenAI:
			return kOpenAI;
	}
	assertrx(false);
	return kRx;
}

}  // namespace reindexer::embedding
