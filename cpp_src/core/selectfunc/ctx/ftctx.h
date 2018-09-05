#pragma once
#include <core/type_consts.h>
#include <memory>
#include "basefunctionctx.h"
#include "core/ft/areaholder.h"
#include "estl/h_vector.h"

namespace reindexer {
using std::shared_ptr;

class FtCtx : public BaseFunctionCtx {
public:
	typedef shared_ptr<FtCtx> Ptr;
	struct Data {
		typedef shared_ptr<Data> Ptr;
		std::vector<int16_t> proc_;
		fast_hash_map<IdType, AreaHolder::Ptr> holders_;
		bool need_area_ = false;
		bool is_composite_ = false;
		bool isWordPositions_ = false;
		std::string extraWordSymbols_;
	};

	FtCtx();
	int16_t Proc(size_t pos);
	bool isComposite() { return data_->is_composite_; }
	AreaHolder::Ptr Area(IdType id);
	size_t GetSize();

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder::UniquePtr &&holder = nullptr);
	void Reserve(size_t size);
	size_t Size();
	bool NeedArea();
	bool PrepareAreas(fast_hash_map<string, int> &fields, const string &name);

	void SetData(Data::Ptr data);
	Data::Ptr GetData();

private:
	Data::Ptr data_;

};  // namespace reindexer
}  // namespace reindexer
