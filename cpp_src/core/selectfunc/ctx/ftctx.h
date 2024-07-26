#pragma once

#include <core/type_consts.h>
#include <memory>
#include "basefunctionctx.h"
#include "core/ft/areaholder.h"
#include "core/ft/usingcontainer.h"

namespace reindexer {

class FtCtx : public BaseFunctionCtx {
public:
	typedef intrusive_ptr<FtCtx> Ptr;
	struct Data : public BaseFunctionCtx {
		bool NeedArea() const noexcept { return holders_.has_value(); }
		void InitHolders() {
			assertrx_dbg(!holders_.has_value());
			holders_.emplace();
		}

		typedef intrusive_ptr<Data> Ptr;
		std::vector<int16_t> proc_;
		std::optional<fast_hash_map<IdType, size_t>> holders_;
		std::vector<AreaHolder> area_;
		bool isComposite_ = false;
		bool isWordPositions_ = false;
		std::string extraWordSymbols_;
	};

	FtCtx() : data_(make_intrusive<Data>()) { this->type = BaseFunctionCtx::kFtCtx; }
	int16_t Proc(size_t pos) const noexcept { return (pos < data_->proc_.size()) ? data_->proc_[pos] : 0; }

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, AreaHolder &&holder);
	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc);

	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask, AreaHolder &&holder);
	template <typename InputIterator>
	void Add(InputIterator begin, InputIterator end, int16_t proc, const std::vector<bool> &mask);

	void Reserve(size_t size) { data_->proc_.reserve(size); }
	size_t Size() const noexcept { return data_->proc_.size(); }
	bool NeedArea() const noexcept { return data_->NeedArea(); }
	bool PrepareAreas(const RHashMap<std::string, int> &fields, const std::string &name);

	void SetData(Data::Ptr data) noexcept { data_ = std::move(data); }
	const Data::Ptr &GetData() const noexcept { return data_; }

private:
	Data::Ptr data_;
};
}  // namespace reindexer
