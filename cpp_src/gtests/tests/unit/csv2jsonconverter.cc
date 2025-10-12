#include "csv2jsonconverter.h"
#include "core/cjson/jsonbuilder.h"
#include "vendor/gason/gason.h"

namespace reindexer {

enum class [[nodiscard]] CSVState { UnquotedField, QuotedField, QuotedQuote };

std::vector<std::string> parseCSVRow(std::string_view row) {
	CSVState state = CSVState::UnquotedField;
	std::vector<std::string> fields{""};
	size_t i = 0;
	for (char c : row) {
		switch (state) {
			case CSVState::UnquotedField:
				switch (c) {
					case ',':
						fields.push_back("");
						i++;
						break;
					case '"':
						state = CSVState::QuotedField;
						break;
					default:
						fields[i].push_back(c);
						break;
				}
				break;
			case CSVState::QuotedField:
				switch (c) {
					case '"':
						state = CSVState::QuotedQuote;
						break;
					default:
						fields[i].push_back(c);
						break;
				}
				break;
			case CSVState::QuotedQuote:
				switch (c) {
					case ',':
						fields.push_back("");
						i++;
						state = CSVState::UnquotedField;
						break;
					case '"':
						fields[i].push_back('"');
						state = CSVState::QuotedField;
						break;
					default:
						state = CSVState::UnquotedField;
						break;
				}
				break;
		}
	}
	return fields;
}

std::string csv2json(std::string_view row, const std::vector<std::string>& schema) {
	auto fields = parseCSVRow(row);

	if (schema.size() < fields.size()) {
		throw Error(errParams, "Not completed schema for csv to json conversion");
	}

	WrSerializer json;
	{
		JsonBuilder builder(json);
		for (size_t i = 0; i < fields.size(); ++i) {
			if (!fields[i].empty()) {
				try {
					gason::JsonParser parser;
					parser.Parse(std::string_view{fields[i]});
					builder.Raw(schema[i], fields[i]);
				} catch (const gason::Exception&) {
					builder.Raw(schema[i], '"' + fields[i] + '"');
				}
			}
		}
	}

	return std::string{json.Slice()};
}

}  // namespace reindexer
